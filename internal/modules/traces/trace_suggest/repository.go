package trace_suggest	//nolint:revive,stylecheck

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
)

// scalarFields are the DSL field keys served by the scalar suggest path.
// Attribute keys (prefixed `@`) take a different path. The set must match
// the field_name values emitted by the trace_scalar_values_5m MV in
// db/clickhouse/30_trace_scalar_values_5m.sql.
var scalarFields = map[string]struct{}{
	"service":     {},
	"operation":   {},
	"http_method": {},
	"http_status": {},
	"status":      {},
	"environment": {},
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

// SuggestScalar reads top-K values for a known scalar field from the
// trace_scalar_values_5m MV. Mirrors SuggestAttribute: narrow keyed range
// scan + prefix filter on already-summarised values.
func (r *ClickHouseRepository) SuggestScalar(ctx context.Context, teamID int64, startMs, endMs int64, field, prefix string, limit int) ([]suggestionRow, error) {
	if _, ok := scalarFields[field]; !ok {
		return nil, fmt.Errorf("trace_suggest: unknown scalar field %q", field)
	}
	var rows []suggestionRow
	query := `
		SELECT value, toUInt64(sumMerge(count_agg)) AS count
		FROM observability.trace_scalar_values_5m
		PREWHERE team_id = @teamID AND field_name = @fieldName
		WHERE bucket_ts BETWEEN @startMs AND @endMs
		  AND (length(@prefix) = 0 OR positionCaseInsensitive(value, @prefix) > 0)
		GROUP BY value
		ORDER BY count DESC
		LIMIT @limit
	`
	err := dbutil.SelectCH(dbutil.DashboardCtx(ctx), r.db, "trace_suggest.SuggestScalar", &rows, query,
		clickhouse.Named("teamID", uint32(teamID)),	//nolint:gosec // G115
		clickhouse.Named("fieldName", field),
		clickhouse.Named("startMs", time.UnixMilli(startMs)),
		clickhouse.Named("endMs", time.UnixMilli(endMs)),
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
		WHERE bucket_ts BETWEEN @startMs AND @endMs
		  AND (length(@prefix) = 0 OR positionCaseInsensitive(value, @prefix) > 0)
		GROUP BY value
		ORDER BY count DESC
		LIMIT @limit
	`
	err := dbutil.SelectCH(dbutil.DashboardCtx(ctx), r.db, "trace_suggest.SuggestAttribute", &rows, query,
		clickhouse.Named("teamID", uint32(teamID)),	//nolint:gosec // G115
		clickhouse.Named("attrKey", key),
		clickhouse.Named("startMs", time.UnixMilli(startMs)),
		clickhouse.Named("endMs", time.UnixMilli(endMs)),
		clickhouse.Named("prefix", prefix),
		clickhouse.Named("limit", uint64(limit)),	//nolint:gosec // G115
	)
	return rows, err
}

// IsScalarField lets the service layer pick the scalar vs attribute path.
func IsScalarField(field string) bool {
	_, ok := scalarFields[field]
	return ok
}
