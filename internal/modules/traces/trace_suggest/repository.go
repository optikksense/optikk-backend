package trace_suggest	//nolint:revive,stylecheck

import (
	"context"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
)

const (
	tracesIndexTable	= "observability.traces_index"
	spansRawTable		= "observability.spans"
)

// scalarColumns maps the DSL field key to the traces_index column that backs
// suggestions for it. Attribute keys (prefixed `@`) take a different path.
var scalarColumns = map[string]string{
	"service":	"root_service",
	"operation":	"root_operation",
	"http_method":	"root_http_method",
	"http_status":	"toString(root_http_status)",
	"status":	"root_status",
	"environment":	"environment",
}

type Repository interface {
	SuggestScalar(ctx context.Context, teamID int64, startMs, endMs int64, field, prefix string, limit int) ([]suggestionRow, error)
	SuggestAttribute(ctx context.Context, teamID int64, startMs, endMs int64, attrKey, prefix string, limit int) ([]suggestionRow, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// SuggestScalar reads top-K values for a known scalar field from traces_index.
// Time-bounded via ts_bucket_start so the scan stays cheap.
func (r *ClickHouseRepository) SuggestScalar(ctx context.Context, teamID int64, startMs, endMs int64, field, prefix string, limit int) ([]suggestionRow, error) {
	col, ok := scalarColumns[field]
	if !ok {
		return nil, fmt.Errorf("trace_suggest: unknown scalar field %q", field)
	}
	var rows []suggestionRow
	query := fmt.Sprintf(`
		SELECT %s AS value, count() AS count
		FROM %s
		WHERE team_id = @teamID
		  AND start_ms BETWEEN @startMs AND @endMs
		  AND %s != ''
		  AND positionCaseInsensitive(%s, @prefix) > 0
		GROUP BY value
		ORDER BY count DESC
		LIMIT @limit
	`, col, tracesIndexTable, col, col)
	err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "trace_suggest.SuggestScalar", &rows, query,
		clickhouse.Named("teamID", uint32(teamID)),	//nolint:gosec // G115
		clickhouse.Named("startMs", uint64(startMs)),	//nolint:gosec // G115
		clickhouse.Named("endMs", uint64(endMs)),	//nolint:gosec // G115
		clickhouse.Named("prefix", prefix),
		clickhouse.Named("limit", uint64(limit)),	//nolint:gosec // G115
	)
	return rows, err
}

// SuggestAttribute reads top values for a custom span attribute key.
// Phase 7: reads from observability.trace_attribute_values_5m — a pre-bucketed
// (team_id, attribute_key, bucket_ts, value) MV — instead of scanning raw
// spans with positionCaseInsensitive(). Narrow range scan on the ordering
// key + prefix filter on the already-summarised values.
func (r *ClickHouseRepository) SuggestAttribute(ctx context.Context, teamID int64, startMs, endMs int64, attrKey, prefix string, limit int) ([]suggestionRow, error) {
	var rows []suggestionRow
	key := strings.TrimPrefix(attrKey, "@")
	query := `
		SELECT value, toUInt64(sumMerge(count_agg)) AS count
		FROM observability.trace_attribute_values_5m
		PREWHERE team_id = @teamID AND attribute_key = @attrKey
		WHERE bucket_ts BETWEEN fromUnixTimestamp64Milli(@startMs) AND fromUnixTimestamp64Milli(@endMs)
		  AND (length(@prefix) = 0 OR positionCaseInsensitive(value, @prefix) > 0)
		GROUP BY value
		ORDER BY count DESC
		LIMIT @limit
	`
	err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "trace_suggest.SuggestAttribute", &rows, query,
		clickhouse.Named("teamID", uint32(teamID)),	//nolint:gosec // G115
		clickhouse.Named("attrKey", key),
		clickhouse.Named("startMs", startMs),
		clickhouse.Named("endMs", endMs),
		clickhouse.Named("prefix", prefix),
		clickhouse.Named("limit", uint64(limit)),	//nolint:gosec // G115
	)
	return rows, err
}

// IsScalarField lets the service layer pick the scalar vs attribute path.
func IsScalarField(field string) bool {
	_, ok := scalarColumns[field]
	return ok
}
