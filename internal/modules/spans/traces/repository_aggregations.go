package traces

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

func (r *ClickHouseRepository) GetErrorGroups(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string, limit int) ([]errorGroupRow, error) {
	query := `
		SELECT s.service_name AS service_name, s.name as operation_name, s.status_message, s.response_status_code as http_status_code,
		       COUNT(*) as error_count,
		       MAX(s.timestamp) as last_occurrence,
		       MIN(s.timestamp) as first_occurrence,
		       (groupArray(s.trace_id) as trace_ids)[1] as sample_trace_id
		FROM observability.spans s
		WHERE s.team_id = @teamID AND (s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400) AND s.ts_bucket_start BETWEEN ? AND ? AND s.timestamp BETWEEN @start AND @end`
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),
		timebucket.SpansBucketStart(startMs / 1000),
		timebucket.SpansBucketStart(endMs / 1000),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
	if serviceName != "" {
		query += ` AND s.service_name = ?`
		args = append(args, serviceName)
	}
	query += ` GROUP BY s.service_name, s.name, s.status_message, s.response_status_code
	           ORDER BY error_count DESC LIMIT ?`
	args = append(args, limit)

	var rows []errorGroupRow
	return rows, r.db.Select(ctx, &rows, query, args...)
}

func (r *ClickHouseRepository) GetErrorTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]errorTimeSeriesRow, error) {
	bucket := rawTimeBucketExpr(startMs, endMs, "s.timestamp")
	query := fmt.Sprintf(`
		SELECT service_name,
		       timestamp,
		       total_count,
		       error_count,
		       if(total_count > 0, error_count*100.0/total_count, 0) AS error_rate
		FROM (
			SELECT s.service_name AS service_name,
			       %s AS timestamp,
			       count()                   AS total_count,
			       countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400) AS error_count
			FROM observability.spans s
			WHERE s.team_id = @teamID AND s.parent_span_id = '' AND s.ts_bucket_start BETWEEN ? AND ? AND s.timestamp BETWEEN @start AND @end`, bucket)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),
		timebucket.SpansBucketStart(startMs / 1000),
		timebucket.SpansBucketStart(endMs / 1000),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
	if serviceName != "" {
		query += ` AND s.service_name = ?`
		args = append(args, serviceName)
	}
	query += fmt.Sprintf(` GROUP BY s.service_name, %s
		)
		ORDER BY timestamp ASC`, bucket)

	var rows []errorTimeSeriesRow
	return rows, r.db.Select(ctx, &rows, query, args...)
}

func (r *ClickHouseRepository) GetLatencyHistogram(ctx context.Context, teamID int64, startMs, endMs int64, serviceName, operationName string) ([]latencyHistogramRow, error) {
	query := `
		SELECT
			CASE
				WHEN duration_ms < 10 THEN '0-10ms'
				WHEN duration_ms < 25 THEN '10-25ms'
				WHEN duration_ms < 50 THEN '25-50ms'
				WHEN duration_ms < 100 THEN '50-100ms'
				WHEN duration_ms < 250 THEN '100-250ms'
				WHEN duration_ms < 500 THEN '250-500ms'
				WHEN duration_ms < 1000 THEN '500ms-1s'
				WHEN duration_ms < 2500 THEN '1s-2.5s'
				WHEN duration_ms < 5000 THEN '2.5s-5s'
				ELSE '>5s'
			END as bucket_label,
			CASE
				WHEN duration_ms < 10 THEN 0
				WHEN duration_ms < 25 THEN 10
				WHEN duration_ms < 50 THEN 25
				WHEN duration_ms < 100 THEN 50
				WHEN duration_ms < 250 THEN 100
				WHEN duration_ms < 500 THEN 250
				WHEN duration_ms < 1000 THEN 500
				WHEN duration_ms < 2500 THEN 1000
				WHEN duration_ms < 5000 THEN 2500
				ELSE 5000
			END as bucket_min,
			COUNT(*) as span_count
		FROM (
			SELECT s.duration_nano / 1000000.0 as duration_ms
			FROM observability.spans s
			WHERE s.team_id = @teamID AND s.parent_span_id = '' AND s.ts_bucket_start BETWEEN ? AND ? AND s.timestamp BETWEEN @start AND @end`
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),
		timebucket.SpansBucketStart(startMs / 1000),
		timebucket.SpansBucketStart(endMs / 1000),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
	if serviceName != "" {
		query += ` AND s.service_name = ?`
		args = append(args, serviceName)
	}
	if operationName != "" {
		query += ` AND s.name = ?`
		args = append(args, operationName)
	}
	query += `) GROUP BY bucket_label, bucket_min ORDER BY bucket_min ASC`

	var rows []latencyHistogramRow
	return rows, r.db.Select(ctx, &rows, query, args...)
}

func (r *ClickHouseRepository) GetLatencyHeatmap(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]latencyHeatmapRow, error) {
	bucket := rawTimeBucketExpr(startMs, endMs, "timestamp")
	query := fmt.Sprintf(`
		SELECT %s as time_bucket,
		       CASE
				WHEN duration_ms < 50 THEN '0-50ms'
				WHEN duration_ms < 100 THEN '50-100ms'
				WHEN duration_ms < 250 THEN '100-250ms'
				WHEN duration_ms < 500 THEN '250-500ms'
				WHEN duration_ms < 1000 THEN '500ms-1s'
				ELSE '>1s'
			END as latency_bucket,
			COUNT(*) as span_count
		FROM (
			SELECT s.timestamp, s.duration_nano / 1000000.0 as duration_ms
			FROM observability.spans s
			WHERE s.team_id = @teamID AND s.parent_span_id = '' AND s.ts_bucket_start BETWEEN ? AND ? AND s.timestamp BETWEEN @start AND @end`, bucket)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),
		timebucket.SpansBucketStart(startMs / 1000),
		timebucket.SpansBucketStart(endMs / 1000),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
	if serviceName != "" {
		query += ` AND s.service_name = ?`
		args = append(args, serviceName)
	}
	query += fmt.Sprintf(`) GROUP BY %s, latency_bucket
	           ORDER BY time_bucket ASC, latency_bucket ASC`, bucket)

	var rows []latencyHeatmapRow
	return rows, r.db.Select(ctx, &rows, query, args...)
}
