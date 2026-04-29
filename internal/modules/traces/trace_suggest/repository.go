package trace_suggest //nolint:revive,stylecheck

import (
	"context"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
)

// Repository runs the two suggest queries. Queries only — service.go owns
// dispatch (`@`-prefix → SuggestAttribute, otherwise → SuggestScalar) plus
// scalar-field validation against the canonical name set.
type Repository interface {
	SuggestScalar(ctx context.Context, teamID, startMs, endMs int64, field, prefix string, limit int) ([]suggestionRow, error)
	SuggestAttribute(ctx context.Context, teamID, startMs, endMs int64, attrKey, prefix string, limit int) ([]suggestionRow, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// SuggestScalar reads top-K values for a known scalar field from raw spans
// over the [startMs, endMs] window. The field has been validated upstream
// in service.go (IsScalarField); scalarFieldExpr returns a closed-set
// column name so string concatenation is safe.
func (r *ClickHouseRepository) SuggestScalar(ctx context.Context, teamID, startMs, endMs int64, field, prefix string, limit int) ([]suggestionRow, error) {
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
		LIMIT @limit`
	var rows []suggestionRow
	return rows, dbutil.SelectCH(dbutil.DashboardCtx(ctx), r.db, "trace_suggest.SuggestScalar", &rows, query, suggestArgs(teamID, startMs, endMs, prefix, limit)...)
}

// SuggestAttribute reads top-K values for a custom span attribute key from
// the per-data-point attributes JSON on raw spans.
func (r *ClickHouseRepository) SuggestAttribute(ctx context.Context, teamID, startMs, endMs int64, attrKey, prefix string, limit int) ([]suggestionRow, error) {
	const query = `
		SELECT JSONExtractString(toJSONString(attributes), @attrKey) AS value, count() AS count
		FROM observability.spans
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		WHERE timestamp BETWEEN @startMs AND @endMs
		  AND value != ''
		  AND (length(@prefix) = 0 OR positionCaseInsensitive(value, @prefix) > 0)
		GROUP BY value
		ORDER BY count DESC
		LIMIT @limit`
	args := append(suggestArgs(teamID, startMs, endMs, prefix, limit),
		clickhouse.Named("attrKey", strings.TrimPrefix(attrKey, "@")),
	)
	var rows []suggestionRow
	return rows, dbutil.SelectCH(dbutil.DashboardCtx(ctx), r.db, "trace_suggest.SuggestAttribute", &rows, query, args...)
}

// suggestArgs binds the 6 parameters both suggest queries share. bucketEnd
// adds one bucket beyond the last covering bucket so endMs at a 5-minute
// boundary doesn't drop the final bucket — same shape as services/latency
// and topology.
func suggestArgs(teamID, startMs, endMs int64, prefix string, limit int) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", timebucket.BucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.BucketStart(endMs/1000)+uint32(timebucket.BucketSeconds)),
		clickhouse.Named("startMs", time.UnixMilli(startMs)),
		clickhouse.Named("endMs", time.UnixMilli(endMs)),
		clickhouse.Named("prefix", prefix),
		clickhouse.Named("limit", uint64(limit)), //nolint:gosec // G115
	}
}

// scalarFieldExpr maps a validated scalar field name to its raw spans
// column. Returns "”" for unknown fields as a defensive default — service
// layer validates via IsScalarField before reaching here.
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
