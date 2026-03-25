package servicemap

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/internal/platform/timebucket"
)

type Repository interface {
	GetTopologyNodes(ctx context.Context, teamID int64, startMs, endMs int64) ([]topologyNodeRow, error)
	GetTopologyEdges(ctx context.Context, teamID int64, startMs, endMs int64) ([]TopologyEdge, error)
	GetUpstreamDownstream(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) ([]serviceDependencyRow, error)
	GetExternalDependencies(ctx context.Context, teamID int64, startMs, endMs int64) ([]ExternalDependency, error)
	GetClientServerLatency(ctx context.Context, teamID int64, startMs, endMs int64, operationName string) ([]clientServerLatencyRow, error)
}

type ClickHouseRepository struct {
	db *database.NativeQuerier
}

func NewRepository(db *database.NativeQuerier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetTopologyNodes(ctx context.Context, teamID int64, startMs, endMs int64) ([]topologyNodeRow, error) {
	var rows []topologyNodeRow
	err := r.db.Select(ctx, &rows, `
		SELECT service_name,
		       request_count,
		       error_count,
		       avg_latency
		FROM (
			SELECT s.service_name AS service_name,
			       toInt64(count())                       AS request_count,
			       toInt64(countIf(`+ErrorCondition()+`)) AS error_count,
			       avg(s.duration_nano / 1000000.0)       AS avg_latency
			FROM observability.spans s
			WHERE s.team_id = @teamID AND `+RootSpanCondition()+` AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end
			GROUP BY s.service_name
		)
		ORDER BY request_count DESC
	`, database.SpanBaseParams(teamID, startMs, endMs)...)
	return rows, err
}

func (r *ClickHouseRepository) GetTopologyEdges(ctx context.Context, teamID int64, startMs, endMs int64) ([]TopologyEdge, error) {
	var rows []TopologyEdge
	err := r.db.Select(ctx, &rows, `
		SELECT source,
		       target,
		       call_count,
		       avg_latency,
		       p95_latency_ms,
		       if(call_count > 0, error_count*100.0/call_count, 0) AS error_rate
		FROM (
			SELECT s1.service_name                                                   AS source,
			       s2.service_name                                                   AS target,
			       toInt64(count())                                                 AS call_count,
			       toInt64(countIf(s1.has_error = true OR toUInt16OrZero(s1.response_status_code) >= 400)) AS error_count,
			       avg(s1.duration_nano / 1000000.0)                                AS avg_latency,
			       quantile(0.95)(s1.duration_nano / 1000000.0)                     AS p95_latency_ms
			FROM observability.spans s1
			JOIN observability.spans s2 ON s1.team_id = s2.team_id AND s1.trace_id = s2.trace_id AND s1.span_id = s2.parent_span_id
				AND s2.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s2.timestamp BETWEEN @start AND @end
			WHERE s1.team_id = @teamID AND s1.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s1.kind = 3 AND s1.timestamp BETWEEN @start AND @end
			  AND s1.service_name != s2.service_name
			GROUP BY s1.service_name, s2.service_name
		)
		ORDER BY call_count DESC
		LIMIT `+fmt.Sprintf("%d", MaxEdges),
		database.SpanBaseParams(teamID, startMs, endMs)...)
	return rows, err
}

func (r *ClickHouseRepository) GetUpstreamDownstream(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) ([]serviceDependencyRow, error) {
	var rows []serviceDependencyRow
	params := append(database.SpanBaseParams(teamID, startMs, endMs), clickhouse.Named("serviceName", serviceName))
	err := r.db.Select(ctx, &rows, `
		SELECT source,
		       target,
		       call_count,
		       p95_latency_ms,
		       if(call_count > 0, error_count * 100.0 / call_count, 0) AS error_rate
		FROM (
		    SELECT s1.service_name                        AS source,
		           s2.service_name                        AS target,
		           toInt64(count())                       AS call_count,
		           toInt64(countIf(s1.has_error = true OR toUInt16OrZero(s1.response_status_code) >= 400)) AS error_count,
		           quantile(0.95)(s1.duration_nano / 1000000.0) AS p95_latency_ms
		    FROM observability.spans s1
		    JOIN observability.spans s2 ON s1.team_id = s2.team_id AND s1.trace_id = s2.trace_id AND s1.span_id = s2.parent_span_id
		        AND s2.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s2.timestamp BETWEEN @start AND @end
		    WHERE s1.team_id = @teamID AND s1.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s1.kind = 3 AND s1.timestamp BETWEEN @start AND @end
		      AND s1.service_name != s2.service_name
		      AND (s1.service_name = @serviceName OR s2.service_name = @serviceName)
		    GROUP BY s1.service_name, s2.service_name
		)
		ORDER BY call_count DESC
		LIMIT 200
	`, params...)
	return rows, err
}

func (r *ClickHouseRepository) GetExternalDependencies(ctx context.Context, teamID int64, startMs, endMs int64) ([]ExternalDependency, error) {
	externalHostExpr := `coalesce(
		nullIf(s.mat_host_name, ''),
		nullIf(s.attributes.'peer.address'::String, ''),
		nullIf(s.http_host, ''),
		nullIf(s.external_http_url, ''),
		nullIf(s.http_url, '')
	)`
	var rows []ExternalDependency
	err := r.db.Select(ctx, &rows, fmt.Sprintf(`
		WITH known_hosts AS (
		    SELECT DISTINCT mat_host_name AS host_name
		    FROM observability.spans
		    WHERE team_id = @teamID AND ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND mat_host_name != ''
		)
		SELECT s.service_name                               AS source_service,
		       %s                                           AS external_host,
		       toInt64(count())                             AS call_count,
		       quantile(0.95)(s.duration_nano / 1000000.0) AS p95_latency_ms,
		       if(count() > 0,
		           countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400) * 100.0 / count(),
		           0) AS error_rate
		FROM observability.spans s
		LEFT ANTI JOIN known_hosts kh ON %s = kh.host_name
		WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.kind = 3 AND s.timestamp BETWEEN @start AND @end
		  AND %s != ''
		GROUP BY s.service_name, external_host
		ORDER BY call_count DESC
		LIMIT 100
	`, externalHostExpr, externalHostExpr, externalHostExpr),
		database.SpanBaseParams(teamID, startMs, endMs)...)
	return rows, err
}

func (r *ClickHouseRepository) GetClientServerLatency(ctx context.Context, teamID int64, startMs, endMs int64, operationName string) ([]clientServerLatencyRow, error) {
	bucket := timebucket.ExprForColumnTime(startMs, endMs, "s.timestamp")
	query := fmt.Sprintf(`
		SELECT %s AS time_bucket,
		       s.name AS operation_name,
		       quantileIf(0.95)(s.duration_nano / 1000000.0, s.kind = 3) AS client_p95_ms,
		       quantileIf(0.95)(s.duration_nano / 1000000.0, s.kind = 2) AS server_p95_ms
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end
		  AND s.kind IN (2, 3)`, bucket)
	args := database.SpanBaseParams(teamID, startMs, endMs)
	if operationName != "" {
		query += ` AND s.name = @operationName`
		args = append(args, clickhouse.Named("operationName", operationName))
	}
	query += fmt.Sprintf(` GROUP BY %s, s.name ORDER BY time_bucket ASC LIMIT 2000`, bucket)

	var rows []clientServerLatencyRow
	return rows, r.db.Select(ctx, &rows, query, args...)
}
