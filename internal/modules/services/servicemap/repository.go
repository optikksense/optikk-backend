package servicemap

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

// Repository defines data access for service map endpoints.
type Repository interface {
	GetUpstreamDownstream(teamUUID, serviceName string, startMs, endMs int64) ([]ServiceDependencyDetail, error)
	GetExternalDependencies(teamUUID string, startMs, endMs int64) ([]ExternalDependency, error)
	GetClientServerLatency(teamUUID string, startMs, endMs int64, operationName string) ([]ClientServerLatencyPoint, error)
}

// ClickHouseRepository implements Repository against ClickHouse.
type ClickHouseRepository struct {
	db dbutil.Querier
}

// NewRepository creates a new service map repository.
func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// GetUpstreamDownstream returns all services that call or are called by serviceName,
// with p95 latency and error rate derived from client spans.
func (r *ClickHouseRepository) GetUpstreamDownstream(teamUUID, serviceName string, startMs, endMs int64) ([]ServiceDependencyDetail, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT source,
		       target,
		       call_count,
		       p95_latency_ms,
		       if(call_count > 0, error_count * 100.0 / call_count, 0) AS error_rate
		FROM (
			SELECT r1.service_name                        AS source,
			       r2.service_name                        AS target,
			       count()                                AS call_count,
			       countIf(s1.has_error = true OR s1.response_status_code >= '400') AS error_count,
			       quantile(0.95)(s1.duration_nano / 1000000.0) AS p95_latency_ms
			FROM observability.spans s1
			ANY INNER JOIN observability.resources r1 ON s1.team_id = r1.team_id AND s1.resource_fingerprint = r1.fingerprint
			JOIN observability.spans s2 ON s1.team_id = s2.team_id AND s1.trace_id = s2.trace_id AND s1.span_id = s2.parent_span_id
			ANY INNER JOIN observability.resources r2 ON s2.team_id = r2.team_id AND s2.resource_fingerprint = r2.fingerprint
			WHERE s1.team_id = ? AND s1.kind = 3 AND s1.timestamp BETWEEN ? AND ?
			  AND r1.service_name != r2.service_name
			  AND (r1.service_name = ? OR r2.service_name = ?)
			GROUP BY r1.service_name, r2.service_name
		)
		ORDER BY call_count DESC
		LIMIT 200
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs), serviceName, serviceName)
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

// GetExternalDependencies returns calls to hosts not present in the resources table
// (i.e. external/third-party endpoints) based on http.url or peer.address attributes.
func (r *ClickHouseRepository) GetExternalDependencies(teamUUID string, startMs, endMs int64) ([]ExternalDependency, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT r.service_name                             AS source_service,
		       s.peer_address                             AS external_host,
		       count()                                    AS call_count,
		       quantile(0.95)(s.duration_nano / 1000000.0) AS p95_latency_ms,
		       if(count() > 0,
		           countIf(s.has_error = true OR s.response_status_code >= '400') * 100.0 / count(),
		           0) AS error_rate
		FROM observability.spans s
		ANY LEFT JOIN observability.resources r ON s.team_id = r.team_id AND s.resource_fingerprint = r.fingerprint
		WHERE s.team_id = ? AND s.kind = 3 AND s.timestamp BETWEEN ? AND ?
		  AND s.peer_address != ''
		  AND s.peer_address NOT IN (
		      SELECT host_name FROM observability.resources WHERE team_id = ?
		  )
		GROUP BY r.service_name, s.peer_address
		ORDER BY call_count DESC
		LIMIT 100
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs), teamUUID)
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

// GetClientServerLatency returns a time-series of client vs server p95 latency per operation,
// computing the network gap as clientP95 - serverP95.
func (r *ClickHouseRepository) GetClientServerLatency(teamUUID string, startMs, endMs int64, operationName string) ([]ClientServerLatencyPoint, error) {
	bucket := timebucket.ExprForColumn(startMs, endMs, "s.timestamp")
	query := fmt.Sprintf(`
		SELECT %s AS time_bucket,
		       s.name AS operation_name,
		       quantileIf(0.95)(s.duration_nano / 1000000.0, s.kind = 3) AS client_p95_ms,
		       quantileIf(0.95)(s.duration_nano / 1000000.0, s.kind = 2) AS server_p95_ms
		FROM observability.spans s
		WHERE s.team_id = ? AND s.timestamp BETWEEN ? AND ?
		  AND s.kind IN (2, 3)`, bucket)
	args := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if operationName != "" {
		query += ` AND s.name = ?`
		args = append(args, operationName)
	}
	query += fmt.Sprintf(` GROUP BY %s, s.name ORDER BY time_bucket ASC`, bucket)

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
