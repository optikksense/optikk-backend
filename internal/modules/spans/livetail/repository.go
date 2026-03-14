package livetail

import (
	"time"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

const maxSpansPerPoll = 100

type Repository struct {
	db dbutil.Querier
}

func NewRepository(db dbutil.Querier) *Repository {
	return &Repository{db: db}
}

// Poll fetches spans newer than `since` for the given team, applying optional filters.
func (r *Repository) Poll(teamID int64, since time.Time, filters LiveTailFilters) ([]LiveSpan, error) {
	now := time.Now()
	sinceMs := since.UnixMilli()
	nowMs := now.UnixMilli()

	frag := ` WHERE s.team_id = ? AND s.ts_bucket_start BETWEEN ? AND ? AND s.timestamp > ? AND s.parent_span_id = ''`
	args := []any{
		uint32(teamID),
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

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	spans := make([]LiveSpan, 0, len(rows))
	for _, row := range rows {
		spans = append(spans, LiveSpan{
			SpanID:        dbutil.StringFromAny(row["span_id"]),
			TraceID:       dbutil.StringFromAny(row["trace_id"]),
			ServiceName:   dbutil.StringFromAny(row["service_name"]),
			OperationName: dbutil.StringFromAny(row["operation_name"]),
			DurationMs:    dbutil.Float64FromAny(row["duration_ms"]),
			Status:        dbutil.StringFromAny(row["status"]),
			HTTPMethod:    dbutil.StringFromAny(row["http_method"]),
			HTTPStatus:    dbutil.StringFromAny(row["http_status_code"]),
			SpanKind:      dbutil.StringFromAny(row["span_kind"]),
			HasError:      dbutil.BoolFromAny(row["has_error"]),
			Timestamp:     dbutil.TimeFromAny(row["timestamp"]),
		})
	}
	return spans, nil
}
