package livetail

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/observability/observability-backend-go/internal/database"
	rootspan "github.com/observability/observability-backend-go/internal/modules/spans/shared/rootspan"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

const maxSpansPerPoll = 100

type Repository struct {
	db *dbutil.NativeQuerier
}

func NewRepository(db *dbutil.NativeQuerier) *Repository {
	return &Repository{db: db}
}

// Poll fetches spans newer than `since` for the given team, applying optional filters.
func (r *Repository) Poll(teamID int64, since time.Time, filters LiveTailFilters) ([]liveSpanDTO, error) {
	now := time.Now()
	sinceMs := since.UnixMilli()
	nowMs := now.UnixMilli()

	frag := ` WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN ? AND ? AND s.timestamp > ? AND ` + rootspan.Condition("s")
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		timebucket.SpansBucketStart(sinceMs / 1000),
		timebucket.SpansBucketStart(nowMs / 1000),
		since,
	}

	if len(filters.Services) > 0 {
		in, vals := dbutil.InClauseFromStrings(filters.Services)
		frag += ` AND s.service_name IN ` + in
		args = append(args, vals...)
	}
	if filters.Status == "ERROR" {
		frag += ` AND (s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400)`
	} else if filters.Status != "" {
		frag += ` AND s.status_code_string = ?`
		args = append(args, filters.Status)
	}
	if filters.SpanKind != "" {
		frag += ` AND s.kind_string = ?`
		args = append(args, filters.SpanKind)
	}
	if filters.Operation != "" {
		frag += ` AND positionCaseInsensitive(s.name, ?) > 0`
		args = append(args, filters.Operation)
	}
	if filters.HTTPMethod != "" {
		frag += ` AND upper(s.http_method) = upper(?)`
		args = append(args, filters.HTTPMethod)
	}
	if filters.SearchText != "" {
		frag += ` AND (
			positionCaseInsensitive(s.trace_id, ?) > 0 OR
			positionCaseInsensitive(s.service_name, ?) > 0 OR
			positionCaseInsensitive(s.name, ?) > 0 OR
			positionCaseInsensitive(s.status_message, ?) > 0
		)`
		args = append(args, filters.SearchText, filters.SearchText, filters.SearchText, filters.SearchText)
	}

	query := `
		SELECT s.span_id, s.trace_id, s.service_name, s.name AS operation_name,
		       s.duration_nano / 1000000.0 AS duration_ms,
		       s.status_code_string AS status,
		       s.http_method, s.response_status_code AS http_status_code,
		       s.kind_string AS span_kind,
		       s.has_error, s.timestamp
		FROM observability.spans s` + frag + `
		ORDER BY s.timestamp DESC
		LIMIT ?`
	args = append(args, maxSpansPerPoll)

	var rows []liveSpanDTO
	return rows, r.db.Select(context.Background(), &rows, query, args...)
}
