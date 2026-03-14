package errorfingerprint

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

type Repository struct {
	db dbutil.Querier
}

func NewRepository(db dbutil.Querier) *Repository {
	return &Repository{db: db}
}

// ListFingerprints groups errors by (service, operation, exception_type, status_message)
// and returns them sorted by count descending.
// The fingerprint is computed at query time using cityHash64.
func (r *Repository) ListFingerprints(teamID int64, startMs, endMs int64, serviceName string, limit int) ([]ErrorFingerprint, error) {
	query := `
		SELECT hex(cityHash64(s.service_name, s.name, s.mat_exception_type, s.status_message)) AS fingerprint,
		       s.service_name,
		       s.name AS operation_name,
		       s.mat_exception_type AS exception_type,
		       s.status_message,
		       min(s.timestamp) AS first_seen,
		       max(s.timestamp) AS last_seen,
		       count() AS cnt,
		       any(s.trace_id) AS sample_trace_id
		FROM observability.spans s
		WHERE s.team_id = ?
		  AND s.ts_bucket_start BETWEEN ? AND ?
		  AND s.timestamp BETWEEN ? AND ?
		  AND (s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400)`
	args := []any{
		uint32(teamID),
		timebucket.SpansBucketStart(startMs / 1000),
		timebucket.SpansBucketStart(endMs / 1000),
		dbutil.SqlTime(startMs),
		dbutil.SqlTime(endMs),
	}
	if serviceName != "" {
		query += ` AND s.service_name = ?`
		args = append(args, serviceName)
	}
	query += `
		GROUP BY s.service_name, s.name, s.mat_exception_type, s.status_message
		ORDER BY cnt DESC
		LIMIT ?`
	args = append(args, limit)

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	fps := make([]ErrorFingerprint, 0, len(rows))
	for _, row := range rows {
		fps = append(fps, ErrorFingerprint{
			Fingerprint:   dbutil.StringFromAny(row["fingerprint"]),
			ServiceName:   dbutil.StringFromAny(row["service_name"]),
			OperationName: dbutil.StringFromAny(row["operation_name"]),
			ExceptionType: dbutil.StringFromAny(row["exception_type"]),
			StatusMessage: dbutil.StringFromAny(row["status_message"]),
			FirstSeen:     dbutil.TimeFromAny(row["first_seen"]),
			LastSeen:      dbutil.TimeFromAny(row["last_seen"]),
			Count:         dbutil.Int64FromAny(row["cnt"]),
			SampleTraceID: dbutil.StringFromAny(row["sample_trace_id"]),
		})
	}
	return fps, nil
}

// GetFingerprintTrend returns occurrence count over time for a specific error fingerprint.
func (r *Repository) GetFingerprintTrend(teamID int64, startMs, endMs int64, serviceName, operationName, exceptionType, statusMessage string) ([]FingerprintTrendPoint, error) {
	bucket := timebucket.ExprForColumn(startMs, endMs, "s.timestamp")
	query := fmt.Sprintf(`
		SELECT %s AS ts,
		       count() AS cnt
		FROM observability.spans s
		WHERE s.team_id = ?
		  AND s.ts_bucket_start BETWEEN ? AND ?
		  AND s.timestamp BETWEEN ? AND ?
		  AND (s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400)
		  AND s.service_name = ?
		  AND s.name = ?
		  AND s.mat_exception_type = ?
		  AND s.status_message = ?
		GROUP BY ts
		ORDER BY ts ASC
	`, bucket)
	args := []any{
		uint32(teamID),
		timebucket.SpansBucketStart(startMs / 1000),
		timebucket.SpansBucketStart(endMs / 1000),
		dbutil.SqlTime(startMs),
		dbutil.SqlTime(endMs),
		serviceName,
		operationName,
		exceptionType,
		statusMessage,
	}

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	points := make([]FingerprintTrendPoint, 0, len(rows))
	for _, row := range rows {
		points = append(points, FingerprintTrendPoint{
			Timestamp: dbutil.TimeFromAny(row["ts"]),
			Count:     dbutil.Int64FromAny(row["cnt"]),
		})
	}
	return points, nil
}
