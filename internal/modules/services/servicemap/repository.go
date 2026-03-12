package servicemap

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

type Repository interface {
	GetUpstreamDownstream(teamID int64, serviceName string, startMs, endMs int64) ([]ServiceDependencyDetail, error)
	GetExternalDependencies(teamID int64, startMs, endMs int64) ([]ExternalDependency, error)
	GetClientServerLatency(teamID int64, startMs, endMs int64, operationName string) ([]ClientServerLatencyPoint, error)
}

type ClickHouseRepository struct {
	db dbutil.Querier
}

func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetUpstreamDownstream(teamID int64, serviceName string, startMs, endMs int64) ([]ServiceDependencyDetail, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT source,
		       target,
		       call_count,
		       p95_latency_ms,
		       if(call_count > 0, error_count * 100.0 / call_count, 0) AS error_rate
		FROM (
			SELECT s1.service_name                        AS source,
			       s2.service_name                        AS target,
			       count()                                AS call_count,
			       countIf(s1.has_error = true OR toUInt16OrZero(s1.response_status_code) >= 400) AS error_count,
			       quantile(0.95)(s1.duration_nano / 1000000.0) AS p95_latency_ms
			FROM observability.spans s1
			JOIN observability.spans s2 ON s1.team_id = s2.team_id AND s1.trace_id = s2.trace_id AND s1.span_id = s2.parent_span_id
				AND s2.ts_bucket_start BETWEEN ? AND ? AND s2.timestamp BETWEEN ? AND ?
			WHERE s1.team_id = ? AND s1.ts_bucket_start BETWEEN ? AND ? AND s1.kind = 3 AND s1.timestamp BETWEEN ? AND ?
			  AND s1.service_name != s2.service_name
			  AND (s1.service_name = ? OR s2.service_name = ?)
			GROUP BY s1.service_name, s2.service_name
		)
		ORDER BY call_count DESC
		LIMIT 200
	`, timebucket.SpansBucketStart(startMs/1000), timebucket.SpansBucketStart(endMs/1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs),
		teamID, timebucket.SpansBucketStart(startMs/1000), timebucket.SpansBucketStart(endMs/1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs), serviceName, serviceName)
	if err != nil {
		return nil, err
	}

	result := make([]ServiceDependencyDetail, 0, len(rows))
	for _, row := range rows {
		source := dbutil.StringFromAny(row["source"])
		target := dbutil.StringFromAny(row["target"])
		direction := "downstream"
		if target == serviceName {
			direction = "upstream"
		}
		result = append(result, ServiceDependencyDetail{
			Source:       source,
			Target:       target,
			CallCount:    dbutil.Int64FromAny(row["call_count"]),
			P95LatencyMs: dbutil.Float64FromAny(row["p95_latency_ms"]),
			ErrorRate:    dbutil.Float64FromAny(row["error_rate"]),
			Direction:    direction,
		})
	}
	return result, nil
}

func (r *ClickHouseRepository) GetExternalDependencies(teamID int64, startMs, endMs int64) ([]ExternalDependency, error) {
	externalHostExpr := `coalesce(
		nullIf(s.mat_host_name, ''),
		nullIf(s.attributes.'peer.address'::String, ''),
		nullIf(s.http_host, ''),
		nullIf(s.external_http_url, ''),
		nullIf(s.http_url, '')
	)`
	rows, err := dbutil.QueryMaps(r.db, fmt.Sprintf(`
		WITH known_hosts AS (
		    SELECT DISTINCT mat_host_name AS host_name
		    FROM observability.spans
		    WHERE team_id = ? AND ts_bucket_start BETWEEN ? AND ? AND mat_host_name != ''
		)
		SELECT s.service_name                              AS source_service,
		       %s                                         AS external_host,
		       count()                                    AS call_count,
		       quantile(0.95)(s.duration_nano / 1000000.0) AS p95_latency_ms,
		       if(count() > 0,
		           countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400) * 100.0 / count(),
		           0) AS error_rate
		FROM observability.spans s
		LEFT ANTI JOIN known_hosts kh ON %s = kh.host_name
		WHERE s.team_id = ? AND s.ts_bucket_start BETWEEN ? AND ? AND s.kind = 3 AND s.timestamp BETWEEN ? AND ?
		  AND %s != ''
		GROUP BY s.service_name, external_host
		ORDER BY call_count DESC
		LIMIT 100
	`, externalHostExpr, externalHostExpr, externalHostExpr),
		teamID, timebucket.SpansBucketStart(startMs/1000), timebucket.SpansBucketStart(endMs/1000),
		teamID, timebucket.SpansBucketStart(startMs/1000), timebucket.SpansBucketStart(endMs/1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}

	result := make([]ExternalDependency, 0, len(rows))
	for _, row := range rows {
		result = append(result, ExternalDependency{
			SourceService: dbutil.StringFromAny(row["source_service"]),
			ExternalHost:  dbutil.StringFromAny(row["external_host"]),
			CallCount:     dbutil.Int64FromAny(row["call_count"]),
			P95LatencyMs:  dbutil.Float64FromAny(row["p95_latency_ms"]),
			ErrorRate:     dbutil.Float64FromAny(row["error_rate"]),
		})
	}
	return result, nil
}

func (r *ClickHouseRepository) GetClientServerLatency(teamID int64, startMs, endMs int64, operationName string) ([]ClientServerLatencyPoint, error) {
	bucket := timebucket.ExprForColumn(startMs, endMs, "s.timestamp")
	query := fmt.Sprintf(`
		SELECT %s AS time_bucket,
		       s.name AS operation_name,
		       quantileIf(0.95)(s.duration_nano / 1000000.0, s.kind = 3) AS client_p95_ms,
		       quantileIf(0.95)(s.duration_nano / 1000000.0, s.kind = 2) AS server_p95_ms
		FROM observability.spans s
		WHERE s.team_id = ? AND s.ts_bucket_start BETWEEN ? AND ? AND s.timestamp BETWEEN ? AND ?
		  AND s.kind IN (2, 3)`, bucket)
	args := []any{teamID, timebucket.SpansBucketStart(startMs / 1000), timebucket.SpansBucketStart(endMs / 1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if operationName != "" {
		query += ` AND s.name = ?`
		args = append(args, operationName)
	}
	query += fmt.Sprintf(` GROUP BY %s, s.name ORDER BY time_bucket ASC LIMIT 2000`, bucket)

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	result := make([]ClientServerLatencyPoint, 0, len(rows))
	for _, row := range rows {
		clientP95 := dbutil.Float64FromAny(row["client_p95_ms"])
		serverP95 := dbutil.Float64FromAny(row["server_p95_ms"])
		gap := clientP95 - serverP95
		if gap < 0 {
			gap = 0
		}
		result = append(result, ClientServerLatencyPoint{
			Timestamp:     dbutil.TimeFromAny(row["time_bucket"]),
			OperationName: dbutil.StringFromAny(row["operation_name"]),
			ClientP95Ms:   clientP95,
			ServerP95Ms:   serverP95,
			NetworkGapMs:  gap,
		})
	}
	return result, nil
}
