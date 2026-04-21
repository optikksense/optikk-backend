// Package livetail serves /v1/traces/live — a short-window keyset-paginated
// stream of recent spans. Raw `observability.spans` is correct here: every
// poll is bounded by `ts_bucket_start` (hot tail) plus optional filters on
// kind_string, name, or free-text search against status_message / trace_id.
// Rollups would collapse the per-span fields the stream returns (trace_id,
// span_id, status_message, kind_string). Permanent raw.
package livetail

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	rootspan "github.com/Optikk-Org/optikk-backend/internal/modules/traces/shared/rootspan"
)

const maxSpansPerPoll = 100

type Repository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *Repository {
	return &Repository{db: db}
}

// Poll fetches spans newer than `since` for the given team, applying optional filters.
func (r *Repository) Poll(ctx context.Context, teamID int64, since time.Time, filters LiveTailFilters) (*PollResult, error) {
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
		frag += ` AND s.service_name IN (?)`
		args = append(args, filters.Services)
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
		ORDER BY s.timestamp ASC
		LIMIT ?`
	args = append(args, maxSpansPerPoll+1)

	var rows []liveSpanDTO
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}

	result := &PollResult{
		Spans: make([]LiveSpan, 0, len(rows)),
	}

	if len(rows) > maxSpansPerPoll {
		result.DroppedCount = int64(len(rows) - maxSpansPerPoll)
		rows = rows[:maxSpansPerPoll]
	}

	for _, row := range rows {
		result.Spans = append(result.Spans, LiveSpan(row))
	}

	return result, nil
}
