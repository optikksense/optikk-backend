package errorfingerprint

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/observability/observability-backend-go/internal/database"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

type Repository struct {
	db *dbutil.NativeQuerier
}

func NewRepository(db *dbutil.NativeQuerier) *Repository {
	return &Repository{db: db}
}

// ListFingerprints groups errors by (service, operation, exception_type, status_message)
// and returns them sorted by count descending.
func (r *Repository) ListFingerprints(teamID int64, startMs, endMs int64, serviceName string, limit int) ([]errorFingerprintDTO, error) {
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
		WHERE s.team_id = @teamID
		  AND s.ts_bucket_start BETWEEN ? AND ?
		  AND s.timestamp BETWEEN @start AND @end
		  AND (s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400)`
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		timebucket.SpansBucketStart(startMs / 1000),
		timebucket.SpansBucketStart(endMs / 1000),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
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

	var rows []errorFingerprintDTO
	return rows, r.db.Select(context.Background(), &rows, query, args...)
}

// GetFingerprintTrend returns occurrence count over time for a specific error fingerprint.
func (r *Repository) GetFingerprintTrend(teamID int64, startMs, endMs int64, serviceName, operationName, exceptionType, statusMessage string) ([]fingerprintTrendPointDTO, error) {
	bucket := timebucket.ExprForColumn(startMs, endMs, "s.timestamp")
	query := fmt.Sprintf(`
		SELECT %s AS ts,
		       count() AS cnt
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.ts_bucket_start BETWEEN ? AND ?
		  AND s.timestamp BETWEEN @start AND @end
		  AND (s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400)
		  AND s.service_name = ?
		  AND s.name = ?
		  AND s.mat_exception_type = ?
		  AND s.status_message = ?
		GROUP BY ts
		ORDER BY ts ASC
	`, bucket)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		timebucket.SpansBucketStart(startMs / 1000),
		timebucket.SpansBucketStart(endMs / 1000),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		serviceName,
		operationName,
		exceptionType,
		statusMessage,
	}

	var rows []fingerprintTrendPointDTO
	return rows, r.db.Select(context.Background(), &rows, query, args...)
}
