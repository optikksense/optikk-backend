package span_query	//nolint:revive,stylecheck

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/querycompiler"
	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/shared/resource"
)

const spansRawTable = "observability.spans"

const spanRowColumns = `span_id, trace_id, parent_span_id, service, name, kind_string,
		duration_nano, toUnixTimestamp64Nano(timestamp) AS timestamp_ns, has_error,
		status_code_string, http_method, response_status_code, environment`

type Repository interface {
	ListSpans(ctx context.Context, f querycompiler.Filters, limit int, cur SpanCursor) ([]spanRowDTO, bool, []string, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// ListSpans reads individual spans from observability.spans with keyset pagination
// on (timestamp, span_id).
func (r *ClickHouseRepository) ListSpans(ctx context.Context, f querycompiler.Filters, limit int, cur SpanCursor) ([]spanRowDTO, bool, []string, error) {
	compiled := querycompiler.Compile(f, querycompiler.TargetSpansRaw)
	preWhere, args, empty, err := resource.WithFingerprints(ctx, r.db, f, compiled.PreWhere, compiled.Args)
	if err != nil {
		return nil, false, compiled.DroppedClauses, err
	}
	if empty {
		return nil, false, compiled.DroppedClauses, nil
	}
	where := compiled.Where
	if cur.SpanID != "" {
		where += ` AND (toUnixTimestamp64Nano(timestamp), span_id) < (@curTs, @curSpanID)`
		args = append(args,
			clickhouse.Named("curTs", cur.TimestampNs),
			clickhouse.Named("curSpanID", cur.SpanID),
		)
	}
	query := fmt.Sprintf(
		`SELECT %s FROM %s PREWHERE %s WHERE %s ORDER BY timestamp DESC, span_id DESC LIMIT @pgLimit`,
		spanRowColumns, spansRawTable, preWhere, where,
	)
	args = append(args, clickhouse.Named("pgLimit", uint64(limit+1)))	//nolint:gosec
	var rows []spanRowDTO
	if err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "span_query.ListSpans", &rows, query, args...); err != nil {
		return nil, false, compiled.DroppedClauses, err
	}
	hasMore := len(rows) > limit
	if hasMore {
		rows = rows[:limit]
	}
	return rows, hasMore, compiled.DroppedClauses, nil
}
