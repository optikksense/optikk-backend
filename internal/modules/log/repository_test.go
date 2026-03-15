package logs

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

type stubRows struct {
	cols []string
	rows [][]any
	idx  int
}

func (r *stubRows) Columns() []string { return r.cols }
func (r *stubRows) Close() error      { return nil }
func (r *stubRows) Next() bool {
	if r.idx >= len(r.rows) {
		return false
	}
	r.idx++
	return true
}
func (r *stubRows) Scan(dest ...any) {
	row := r.rows[r.idx-1]
	for i := range dest {
		if ptr, ok := dest[i].(*any); ok {
			*ptr = row[i]
		}
	}
}

type stubRow struct {
	values []any
}

func (r *stubRow) Scan(dest ...any) error {
	for i := range dest {
		switch ptr := dest[i].(type) {
		case *int64:
			if i < len(r.values) {
				if v, ok := r.values[i].(int64); ok {
					*ptr = v
				}
			}
		}
	}
	return nil
}

type queryResponse struct {
	rows dbutil.Rows
	err  error
}

type stubQuerier struct {
	responses []queryResponse
	queries   []string
}

func (s *stubQuerier) Exec(query string, args ...any) (sql.Result, error) { return nil, nil }
func (s *stubQuerier) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return nil, nil
}
func (s *stubQuerier) Query(query string, args ...any) (dbutil.Rows, error) {
	s.queries = append(s.queries, query)
	if len(s.responses) == 0 {
		return &stubRows{}, nil
	}
	resp := s.responses[0]
	s.responses = s.responses[1:]
	if resp.rows == nil {
		return &stubRows{}, resp.err
	}
	return resp.rows, resp.err
}
func (s *stubQuerier) QueryRow(query string, args ...any) dbutil.Row {
	return &stubRow{values: []any{int64(0)}}
}
func (s *stubQuerier) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) { return nil, nil }
func (s *stubQuerier) Close() error                                                      { return nil }

func TestGetLogAggregateUsesSafeBucketExpressionForErrorRate(t *testing.T) {
	repo := NewRepository(&stubQuerier{
		responses: []queryResponse{
			{
				rows: &stubRows{
					cols: []string{"grp", "err_cnt"},
					rows: [][]any{{"orders-service", int64(4)}},
				},
			},
			{
				rows: &stubRows{
					cols: []string{"time_bucket", "grp", "error_rate"},
					rows: [][]any{{"2026-03-15T10:00:00Z", "orders-service", 50.0}},
				},
			},
		},
	})

	rows, err := repo.GetLogAggregate(context.Background(), LogFilters{
		TeamID:  1,
		StartMs: 1_741_000_000_000,
		EndMs:   1_741_000_300_000,
	}, LogAggregateRequest{
		GroupBy: "service",
		Step:    "5m",
		Metric:  "error_rate",
		TopN:    5,
	})
	if err != nil {
		t.Fatalf("expected aggregate query to succeed, got %v", err)
	}
	if len(rows) != 1 || rows[0].ErrorRate != 50 {
		t.Fatalf("expected parsed error-rate row, got %+v", rows)
	}

	stub := repo.db.(*stubQuerier)
	if len(stub.queries) != 2 {
		t.Fatalf("expected 2 queries, got %d", len(stub.queries))
	}
	if strings.Contains(stub.queries[1], "INTERVAL 5m") {
		t.Fatalf("unexpected raw interval syntax in query: %s", stub.queries[1])
	}
	if !strings.Contains(stub.queries[1], "sum(if(severity_text IN ('ERROR', 'FATAL'), 1, 0))") {
		t.Fatalf("expected safe error-rate aggregation in query: %s", stub.queries[1])
	}
}

func TestGetLogStatsReturnsSnakeCaseFacetKeys(t *testing.T) {
	repo := NewRepository(&stubQuerier{
		responses: []queryResponse{
			{
				rows: &stubRows{
					cols: []string{"dim", "value", "count"},
					rows: [][]any{
						{"level", "ERROR", int64(3)},
						{"service_name", "orders-service", int64(2)},
					},
				},
			},
		},
	})

	stats, err := repo.GetLogStats(context.Background(), LogFilters{
		TeamID:  1,
		StartMs: 1_741_000_000_000,
		EndMs:   1_741_000_300_000,
	})
	if err != nil {
		t.Fatalf("expected stats query to succeed, got %v", err)
	}
	if _, ok := stats.Fields["level"]; !ok {
		t.Fatalf("expected level facets, got %+v", stats.Fields)
	}
	if _, ok := stats.Fields["service_name"]; !ok {
		t.Fatalf("expected service_name facets, got %+v", stats.Fields)
	}
	if stats.Total != 3 {
		t.Fatalf("expected total from level facets to be 3, got %d", stats.Total)
	}
}
