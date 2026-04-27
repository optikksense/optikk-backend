package trace_suggest //nolint:revive,stylecheck

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
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

// SuggestScalar reads top-K values for a known scalar field directly from the
// traces view. This keeps suggest functional after removing the old rollup
// helper tables.
func (r *ClickHouseRepository) SuggestScalar(ctx context.Context, teamID int64, startMs, endMs int64, field, prefix string, limit int) ([]suggestionRow, error) {
	if _, ok := scalarFields[field]; !ok {
		return nil, fmt.Errorf("trace_suggest: unknown scalar field %q", field)
	}
	var rows []suggestionRow
	column := scalarFieldExpr(field)
	query := `
		SELECT ` + column + ` AS value, count() AS count
		FROM observability.spans
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		WHERE timestamp BETWEEN @startMs AND @endMs
		  AND ` + column + ` != ''
		  AND (length(@prefix) = 0 OR positionCaseInsensitive(value, @prefix) > 0)
		GROUP BY value
		ORDER BY count DESC
		LIMIT @limit
	`
	err := dbutil.SelectCH(dbutil.DashboardCtx(ctx), r.db, "trace_suggest.SuggestScalar", &rows, query,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(endMs/1000)),
		clickhouse.Named("startMs", time.UnixMilli(startMs)),
		clickhouse.Named("endMs", time.UnixMilli(endMs)),
		clickhouse.Named("prefix", prefix),
		clickhouse.Named("limit", uint64(limit)), //nolint:gosec // G115
	)
	return rows, err
}

// SuggestAttribute reads top values for a custom span attribute key directly
// from the raw spans table.
func (r *ClickHouseRepository) SuggestAttribute(ctx context.Context, teamID int64, startMs, endMs int64, attrKey, prefix string, limit int) ([]suggestionRow, error) {
	var rows []suggestionRow
	key := strings.TrimPrefix(attrKey, "@")
	query := `
		SELECT JSONExtractString(toJSONString(attributes), @attrKey) AS value, count() AS count
		FROM observability.spans
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		WHERE timestamp BETWEEN @startMs AND @endMs
		  AND value != ''
		  AND (length(@prefix) = 0 OR positionCaseInsensitive(value, @prefix) > 0)
		GROUP BY value
		ORDER BY count DESC
		LIMIT @limit
	`
	err := dbutil.SelectCH(dbutil.DashboardCtx(ctx), r.db, "trace_suggest.SuggestAttribute", &rows, query,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("attrKey", key),
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(endMs/1000)),
		clickhouse.Named("startMs", time.UnixMilli(startMs)),
		clickhouse.Named("endMs", time.UnixMilli(endMs)),
		clickhouse.Named("prefix", prefix),
		clickhouse.Named("limit", uint64(limit)), //nolint:gosec // G115
	)
	return rows, err
}

// IsScalarField lets the service layer pick the scalar vs attribute path.
func IsScalarField(field string) bool {
	_, ok := scalarFields[field]
	return ok
}

func scalarFieldExpr(field string) string {
	switch field {
	case "service":
		return "service"
	case "operation":
		return "name"
	case "http_method":
		return "http_method"
	case "http_status":
		return "response_status_code"
	case "status":
		return "status_code_string"
	case "environment":
		return "environment"
	default:
		return "''"
	}
}
