package impl

import (
	dbutil "github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/internal/modules/insights/model"
)

// ClickHouseRepository encapsulates data access logic for insights.
type ClickHouseRepository struct {
	db dbutil.Querier
}

// NewRepository creates a new Insights Repository.
func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// GetInsightResourceUtilization queries CPU/memory/disk/network utilization by service and instance.
func (r *ClickHouseRepository) GetInsightResourceUtilization(teamUUID string, startMs, endMs int64) ([]model.ServiceResource, []model.InstanceResource, []model.InfraResource, []model.ResourceBucket, error) {
	byServiceRaw, err := dbutil.QueryMaps(r.db, `
		SELECT service_name,
		       coalesce(
		           if(
		               countIf(metric_name IN ('system.cpu.utilization', 'system.cpu.usage', 'process.cpu.usage') AND isFinite(value)) > 0,
		               avgIf(
		                   if(value <= 1.0, value * 100.0, value),
		                   metric_name IN ('system.cpu.utilization', 'system.cpu.usage', 'process.cpu.usage') AND isFinite(value)
		               ),
		               NULL
		           ),
		           if(
		               countIf(JSONExtractFloat(attributes, 'system.cpu.utilization') > 0) > 0,
		               avgIf(
		                   if(
		                       JSONExtractFloat(attributes, 'system.cpu.utilization') <= 1.0,
		                       JSONExtractFloat(attributes, 'system.cpu.utilization') * 100.0,
		                       JSONExtractFloat(attributes, 'system.cpu.utilization')
		                   ),
		                   JSONExtractFloat(attributes, 'system.cpu.utilization') > 0
		               ),
		               NULL
		           )
		       ) as avg_cpu_util,
		       coalesce(
		           if(
		               countIf(metric_name = 'system.memory.utilization' AND isFinite(value)) > 0,
		               avgIf(if(value <= 1.0, value * 100.0, value), metric_name = 'system.memory.utilization' AND isFinite(value)),
		               NULL
		           ),
		           if(
		               sumIf(value, metric_name = 'jvm.memory.max' AND value > 0 AND isFinite(value)) > 0,
		               100.0 * sumIf(value, metric_name = 'jvm.memory.used' AND value >= 0 AND isFinite(value))
		                   / nullIf(sumIf(value, metric_name = 'jvm.memory.max' AND value > 0 AND isFinite(value)), 0),
		               NULL
		           ),
		           if(
		               countIf(JSONExtractFloat(attributes, 'system.memory.utilization') > 0) > 0,
		               avgIf(
		                   if(
		                       JSONExtractFloat(attributes, 'system.memory.utilization') <= 1.0,
		                       JSONExtractFloat(attributes, 'system.memory.utilization') * 100.0,
		                       JSONExtractFloat(attributes, 'system.memory.utilization')
		                   ),
		                   JSONExtractFloat(attributes, 'system.memory.utilization') > 0
		               ),
		               NULL
		           )
		       ) as avg_memory_util,
		       coalesce(
		           if(
		               countIf(metric_name = 'system.disk.utilization' AND isFinite(value)) > 0,
		               avgIf(if(value <= 1.0, value * 100.0, value), metric_name = 'system.disk.utilization' AND isFinite(value)),
		               NULL
		           ),
		           if(
		               sumIf(value, metric_name = 'disk.total' AND value > 0 AND isFinite(value)) > 0,
		               100.0 * (
		                   1.0
		                   - (
		                       sumIf(value, metric_name = 'disk.free' AND value >= 0 AND isFinite(value))
		                       / nullIf(sumIf(value, metric_name = 'disk.total' AND value > 0 AND isFinite(value)), 0)
		                   )
		               ),
		               NULL
		           ),
		           if(
		               countIf(JSONExtractFloat(attributes, 'system.disk.utilization') > 0) > 0,
		               avgIf(
		                   if(
		                       JSONExtractFloat(attributes, 'system.disk.utilization') <= 1.0,
		                       JSONExtractFloat(attributes, 'system.disk.utilization') * 100.0,
		                       JSONExtractFloat(attributes, 'system.disk.utilization')
		                   ),
		                   JSONExtractFloat(attributes, 'system.disk.utilization') > 0
		               ),
		               NULL
		           )
		       ) as avg_disk_util,
		       coalesce(
		           if(
		               countIf(metric_name = 'system.network.utilization' AND isFinite(value)) > 0,
		               avgIf(if(value <= 1.0, value * 100.0, value), metric_name = 'system.network.utilization' AND isFinite(value)),
		               NULL
		           ),
		           if(
		               countIf(metric_name = 'http.server.requests.active.active' AND isFinite(value)) > 0,
		               avgIf(
		                   if(value <= 1.0, value * 100.0, least(value, 100.0)),
		                   metric_name = 'http.server.requests.active.active' AND isFinite(value)
		               ),
		               NULL
		           ),
		           if(
		               countIf(JSONExtractFloat(attributes, 'system.network.utilization') > 0) > 0,
		               avgIf(
		                   if(
		                       JSONExtractFloat(attributes, 'system.network.utilization') <= 1.0,
		                       JSONExtractFloat(attributes, 'system.network.utilization') * 100.0,
		                       JSONExtractFloat(attributes, 'system.network.utilization')
		                   ),
		                   JSONExtractFloat(attributes, 'system.network.utilization') > 0
		               ),
		               NULL
		           )
		       ) as avg_network_util,
		       coalesce(
		           if(
		               countIf(metric_name = 'db.connection.pool.utilization' AND isFinite(value)) > 0,
		               avgIf(if(value <= 1.0, value * 100.0, value), metric_name = 'db.connection.pool.utilization' AND isFinite(value)),
		               NULL
		           ),
		           if(
		               sumIf(value, metric_name = 'hikaricp.connections.max' AND value > 0 AND isFinite(value)) > 0,
		               100.0 * sumIf(value, metric_name = 'hikaricp.connections.active' AND value >= 0 AND isFinite(value))
		                   / nullIf(sumIf(value, metric_name = 'hikaricp.connections.max' AND value > 0 AND isFinite(value)), 0),
		               NULL
		           ),
		           if(
		               countIf(metric_name = 'executor.pool.size' AND isFinite(value)) > 0,
		               avgIf(if(value <= 1.0, value * 100.0, least(value, 100.0)), metric_name = 'executor.pool.size' AND isFinite(value)),
		               NULL
		           ),
		           if(
		               countIf(JSONExtractFloat(attributes, 'db.connection_pool.utilization') > 0) > 0,
		               avgIf(
		                   if(
		                       JSONExtractFloat(attributes, 'db.connection_pool.utilization') <= 1.0,
		                       JSONExtractFloat(attributes, 'db.connection_pool.utilization') * 100.0,
		                       JSONExtractFloat(attributes, 'db.connection_pool.utilization')
		                   ),
		                   JSONExtractFloat(attributes, 'db.connection_pool.utilization') > 0
		               ),
		               NULL
		           )
		       ) as avg_connection_pool_util,
		       COUNT(*) as sample_count
		FROM metrics
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		  AND (
		      metric_name IN (
		          'system.cpu.utilization',
		          'system.cpu.usage',
		          'process.cpu.usage',
		          'system.memory.utilization',
		          'jvm.memory.used',
		          'jvm.memory.max',
		          'system.disk.utilization',
		          'disk.free',
		          'disk.total',
		          'system.network.utilization',
		          'http.server.requests.active.active',
		          'db.connection.pool.utilization',
		          'hikaricp.connections.active',
		          'hikaricp.connections.max',
		          'executor.pool.size'
		      )
		      OR JSONExtractFloat(attributes, 'system.cpu.utilization') > 0
		      OR JSONExtractFloat(attributes, 'system.memory.utilization') > 0
		      OR JSONExtractFloat(attributes, 'system.disk.utilization') > 0
		      OR JSONExtractFloat(attributes, 'system.network.utilization') > 0
		      OR JSONExtractFloat(attributes, 'db.connection_pool.utilization') > 0
		  )
		GROUP BY service_name
		HAVING service_name != ''
		ORDER BY service_name ASC
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, nil, nil, nil, err
	}

	byService := make([]model.ServiceResource, len(byServiceRaw))
	for i, row := range byServiceRaw {
		byService[i] = model.ServiceResource{
			ServiceName:           dbutil.StringFromAny(row["service_name"]),
			AvgCpuUtil:            dbutil.NullableFloat64FromAny(row["avg_cpu_util"]),
			AvgMemoryUtil:         dbutil.NullableFloat64FromAny(row["avg_memory_util"]),
			AvgDiskUtil:           dbutil.NullableFloat64FromAny(row["avg_disk_util"]),
			AvgNetworkUtil:        dbutil.NullableFloat64FromAny(row["avg_network_util"]),
			AvgConnectionPoolUtil: dbutil.NullableFloat64FromAny(row["avg_connection_pool_util"]),
			SampleCount:           dbutil.Int64FromAny(row["sample_count"]),
		}
	}

	byInstanceRaw, err := dbutil.QueryMaps(r.db, `
		SELECT host, pod, container, service_name,
		       coalesce(
		           if(
		               countIf(metric_name IN ('system.cpu.utilization', 'system.cpu.usage', 'process.cpu.usage') AND isFinite(value)) > 0,
		               avgIf(
		                   if(value <= 1.0, value * 100.0, value),
		                   metric_name IN ('system.cpu.utilization', 'system.cpu.usage', 'process.cpu.usage') AND isFinite(value)
		               ),
		               NULL
		           ),
		           if(
		               countIf(JSONExtractFloat(attributes, 'system.cpu.utilization') > 0) > 0,
		               avgIf(
		                   if(
		                       JSONExtractFloat(attributes, 'system.cpu.utilization') <= 1.0,
		                       JSONExtractFloat(attributes, 'system.cpu.utilization') * 100.0,
		                       JSONExtractFloat(attributes, 'system.cpu.utilization')
		                   ),
		                   JSONExtractFloat(attributes, 'system.cpu.utilization') > 0
		               ),
		               NULL
		           )
		       ) as avg_cpu_util,
		       coalesce(
		           if(
		               countIf(metric_name = 'system.memory.utilization' AND isFinite(value)) > 0,
		               avgIf(if(value <= 1.0, value * 100.0, value), metric_name = 'system.memory.utilization' AND isFinite(value)),
		               NULL
		           ),
		           if(
		               sumIf(value, metric_name = 'jvm.memory.max' AND value > 0 AND isFinite(value)) > 0,
		               100.0 * sumIf(value, metric_name = 'jvm.memory.used' AND value >= 0 AND isFinite(value))
		                   / nullIf(sumIf(value, metric_name = 'jvm.memory.max' AND value > 0 AND isFinite(value)), 0),
		               NULL
		           ),
		           if(
		               countIf(JSONExtractFloat(attributes, 'system.memory.utilization') > 0) > 0,
		               avgIf(
		                   if(
		                       JSONExtractFloat(attributes, 'system.memory.utilization') <= 1.0,
		                       JSONExtractFloat(attributes, 'system.memory.utilization') * 100.0,
		                       JSONExtractFloat(attributes, 'system.memory.utilization')
		                   ),
		                   JSONExtractFloat(attributes, 'system.memory.utilization') > 0
		               ),
		               NULL
		           )
		       ) as avg_memory_util,
		       coalesce(
		           if(
		               countIf(metric_name = 'system.disk.utilization' AND isFinite(value)) > 0,
		               avgIf(if(value <= 1.0, value * 100.0, value), metric_name = 'system.disk.utilization' AND isFinite(value)),
		               NULL
		           ),
		           if(
		               sumIf(value, metric_name = 'disk.total' AND value > 0 AND isFinite(value)) > 0,
		               100.0 * (
		                   1.0
		                   - (
		                       sumIf(value, metric_name = 'disk.free' AND value >= 0 AND isFinite(value))
		                       / nullIf(sumIf(value, metric_name = 'disk.total' AND value > 0 AND isFinite(value)), 0)
		                   )
		               ),
		               NULL
		           ),
		           if(
		               countIf(JSONExtractFloat(attributes, 'system.disk.utilization') > 0) > 0,
		               avgIf(
		                   if(
		                       JSONExtractFloat(attributes, 'system.disk.utilization') <= 1.0,
		                       JSONExtractFloat(attributes, 'system.disk.utilization') * 100.0,
		                       JSONExtractFloat(attributes, 'system.disk.utilization')
		                   ),
		                   JSONExtractFloat(attributes, 'system.disk.utilization') > 0
		               ),
		               NULL
		           )
		       ) as avg_disk_util,
		       coalesce(
		           if(
		               countIf(metric_name = 'system.network.utilization' AND isFinite(value)) > 0,
		               avgIf(if(value <= 1.0, value * 100.0, value), metric_name = 'system.network.utilization' AND isFinite(value)),
		               NULL
		           ),
		           if(
		               countIf(metric_name = 'http.server.requests.active.active' AND isFinite(value)) > 0,
		               avgIf(
		                   if(value <= 1.0, value * 100.0, least(value, 100.0)),
		                   metric_name = 'http.server.requests.active.active' AND isFinite(value)
		               ),
		               NULL
		           ),
		           if(
		               countIf(JSONExtractFloat(attributes, 'system.network.utilization') > 0) > 0,
		               avgIf(
		                   if(
		                       JSONExtractFloat(attributes, 'system.network.utilization') <= 1.0,
		                       JSONExtractFloat(attributes, 'system.network.utilization') * 100.0,
		                       JSONExtractFloat(attributes, 'system.network.utilization')
		                   ),
		                   JSONExtractFloat(attributes, 'system.network.utilization') > 0
		               ),
		               NULL
		           )
		       ) as avg_network_util,
		       coalesce(
		           if(
		               countIf(metric_name = 'db.connection.pool.utilization' AND isFinite(value)) > 0,
		               avgIf(if(value <= 1.0, value * 100.0, value), metric_name = 'db.connection.pool.utilization' AND isFinite(value)),
		               NULL
		           ),
		           if(
		               sumIf(value, metric_name = 'hikaricp.connections.max' AND value > 0 AND isFinite(value)) > 0,
		               100.0 * sumIf(value, metric_name = 'hikaricp.connections.active' AND value >= 0 AND isFinite(value))
		                   / nullIf(sumIf(value, metric_name = 'hikaricp.connections.max' AND value > 0 AND isFinite(value)), 0),
		               NULL
		           ),
		           if(
		               countIf(metric_name = 'executor.pool.size' AND isFinite(value)) > 0,
		               avgIf(if(value <= 1.0, value * 100.0, least(value, 100.0)), metric_name = 'executor.pool.size' AND isFinite(value)),
		               NULL
		           ),
		           if(
		               countIf(JSONExtractFloat(attributes, 'db.connection_pool.utilization') > 0) > 0,
		               avgIf(
		                   if(
		                       JSONExtractFloat(attributes, 'db.connection_pool.utilization') <= 1.0,
		                       JSONExtractFloat(attributes, 'db.connection_pool.utilization') * 100.0,
		                       JSONExtractFloat(attributes, 'db.connection_pool.utilization')
		                   ),
		                   JSONExtractFloat(attributes, 'db.connection_pool.utilization') > 0
		               ),
		               NULL
		           )
		       ) as avg_connection_pool_util,
		       COUNT(*) as sample_count
		FROM metrics
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		  AND (
		      metric_name IN (
		          'system.cpu.utilization',
		          'system.cpu.usage',
		          'process.cpu.usage',
		          'system.memory.utilization',
		          'jvm.memory.used',
		          'jvm.memory.max',
		          'system.disk.utilization',
		          'disk.free',
		          'disk.total',
		          'system.network.utilization',
		          'http.server.requests.active.active',
		          'db.connection.pool.utilization',
		          'hikaricp.connections.active',
		          'hikaricp.connections.max',
		          'executor.pool.size'
		      )
		      OR JSONExtractFloat(attributes, 'system.cpu.utilization') > 0
		      OR JSONExtractFloat(attributes, 'system.memory.utilization') > 0
		      OR JSONExtractFloat(attributes, 'system.disk.utilization') > 0
		      OR JSONExtractFloat(attributes, 'system.network.utilization') > 0
		      OR JSONExtractFloat(attributes, 'db.connection_pool.utilization') > 0
		  )
		GROUP BY host, pod, container, service_name
		HAVING service_name != ''
		ORDER BY sample_count DESC
		LIMIT 200
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, nil, nil, nil, err
	}

	byInstance := make([]model.InstanceResource, len(byInstanceRaw))
	for i, row := range byInstanceRaw {
		byInstance[i] = model.InstanceResource{
			Host:                  dbutil.StringFromAny(row["host"]),
			Pod:                   dbutil.StringFromAny(row["pod"]),
			Container:             dbutil.StringFromAny(row["container"]),
			ServiceName:           dbutil.StringFromAny(row["service_name"]),
			AvgCpuUtil:            dbutil.NullableFloat64FromAny(row["avg_cpu_util"]),
			AvgMemoryUtil:         dbutil.NullableFloat64FromAny(row["avg_memory_util"]),
			AvgDiskUtil:           dbutil.NullableFloat64FromAny(row["avg_disk_util"]),
			AvgNetworkUtil:        dbutil.NullableFloat64FromAny(row["avg_network_util"]),
			AvgConnectionPoolUtil: dbutil.NullableFloat64FromAny(row["avg_connection_pool_util"]),
			SampleCount:           dbutil.Int64FromAny(row["sample_count"]),
		}
	}

	infraRaw, err := dbutil.QueryMaps(r.db, `
		SELECT host, pod, container,
		       COUNT(*) as span_count,
		       sum(if(status='ERROR', 1, 0)) as error_count,
		       avg(duration_ms) as avg_latency,
		       quantile(0.95)(duration_ms) as p95_latency,
		       arrayStringConcat(groupArray(DISTINCT service_name), ',') as services_csv
		FROM spans
		WHERE team_id = ? AND start_time BETWEEN ? AND ? AND host != ''
		GROUP BY host, pod, container
		ORDER BY span_count DESC
		LIMIT 100
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, nil, nil, nil, err
	}

	infra := make([]model.InfraResource, len(infraRaw))
	for i, row := range infraRaw {
		infra[i] = model.InfraResource{
			Host:        dbutil.StringFromAny(row["host"]),
			Pod:         dbutil.StringFromAny(row["pod"]),
			Container:   dbutil.StringFromAny(row["container"]),
			SpanCount:   dbutil.Int64FromAny(row["span_count"]),
			ErrorCount:  dbutil.Int64FromAny(row["error_count"]),
			AvgLatency:  dbutil.Float64FromAny(row["avg_latency"]),
			P95Latency:  dbutil.Float64FromAny(row["p95_latency"]),
			ServicesCsv: dbutil.StringFromAny(row["services_csv"]),
		}
	}

	timeseriesRaw, err := dbutil.QueryMaps(r.db, `
		SELECT formatDateTime(toStartOfMinute(timestamp), '%Y-%m-%dT%H:%i:%SZ') as time_bucket,
		       service_name as pod,
		       coalesce(
		           if(
		               countIf(metric_name IN ('system.cpu.utilization', 'system.cpu.usage', 'process.cpu.usage') AND isFinite(value)) > 0,
		               avgIf(
		                   if(value <= 1.0, value * 100.0, value),
		                   metric_name IN ('system.cpu.utilization', 'system.cpu.usage', 'process.cpu.usage') AND isFinite(value)
		               ),
		               NULL
		           ),
		           if(
		               countIf(JSONExtractFloat(attributes, 'system.cpu.utilization') > 0) > 0,
		               avgIf(
		                   if(
		                       JSONExtractFloat(attributes, 'system.cpu.utilization') <= 1.0,
		                       JSONExtractFloat(attributes, 'system.cpu.utilization') * 100.0,
		                       JSONExtractFloat(attributes, 'system.cpu.utilization')
		                   ),
		                   JSONExtractFloat(attributes, 'system.cpu.utilization') > 0
		               ),
		               NULL
		           )
		       ) as avg_cpu_util,
		       coalesce(
		           if(
		               countIf(metric_name = 'system.memory.utilization' AND isFinite(value)) > 0,
		               avgIf(if(value <= 1.0, value * 100.0, value), metric_name = 'system.memory.utilization' AND isFinite(value)),
		               NULL
		           ),
		           if(
		               sumIf(value, metric_name = 'jvm.memory.max' AND value > 0 AND isFinite(value)) > 0,
		               100.0 * sumIf(value, metric_name = 'jvm.memory.used' AND value >= 0 AND isFinite(value))
		                   / nullIf(sumIf(value, metric_name = 'jvm.memory.max' AND value > 0 AND isFinite(value)), 0),
		               NULL
		           ),
		           if(
		               countIf(JSONExtractFloat(attributes, 'system.memory.utilization') > 0) > 0,
		               avgIf(
		                   if(
		                       JSONExtractFloat(attributes, 'system.memory.utilization') <= 1.0,
		                       JSONExtractFloat(attributes, 'system.memory.utilization') * 100.0,
		                       JSONExtractFloat(attributes, 'system.memory.utilization')
		                   ),
		                   JSONExtractFloat(attributes, 'system.memory.utilization') > 0
		               ),
		               NULL
		           )
		       ) as avg_memory_util
		FROM metrics
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		  AND (
		      metric_name IN ('system.cpu.utilization', 'system.cpu.usage', 'process.cpu.usage', 'system.memory.utilization', 'jvm.memory.used', 'jvm.memory.max')
		      OR JSONExtractFloat(attributes, 'system.cpu.utilization') > 0
		      OR JSONExtractFloat(attributes, 'system.memory.utilization') > 0
		  )
		GROUP BY 1, 2
		HAVING pod != ''
		ORDER BY 1 ASC, 2 ASC
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, nil, nil, nil, err
	}

	timeseries := make([]model.ResourceBucket, len(timeseriesRaw))
	for i, row := range timeseriesRaw {
		timeseries[i] = model.ResourceBucket{
			Timestamp:     dbutil.StringFromAny(row["time_bucket"]),
			Pod:           dbutil.StringFromAny(row["pod"]),
			AvgCpuUtil:    dbutil.NullableFloat64FromAny(row["avg_cpu_util"]),
			AvgMemoryUtil: dbutil.NullableFloat64FromAny(row["avg_memory_util"]),
		}
	}

	return byService, byInstance, infra, timeseries, nil
}

// GetInsightSloSli queries SLO compliance status and timeseries.
func (r *ClickHouseRepository) GetInsightSloSli(teamUUID string, startMs, endMs int64, serviceName string) (model.SloSummary, []model.SloBucket, error) {
	query1 := `
		SELECT sum(count) as total_requests,
		       sum(if(status='ERROR', count, 0)) as error_count,
		       if(sum(count)>0, (sum(count)-sum(if(status='ERROR', count, 0)))*100.0/sum(count), 100.0) as availability_percent,
		       avg(avg) as avg_latency_ms,
		       avg(p95) as p95_latency_ms
		FROM metrics
		WHERE team_id = ? AND metric_category = 'http' AND timestamp BETWEEN ? AND ?
	`
	args1 := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query1 += ` AND service_name = ?`
		args1 = append(args1, serviceName)
	}

	summaryRaw, err := dbutil.QueryMap(r.db, query1, args1...)
	if err != nil {
		return model.SloSummary{}, nil, err
	}

	summary := model.SloSummary{
		TotalRequests:       dbutil.Int64FromAny(summaryRaw["total_requests"]),
		ErrorCount:          dbutil.Int64FromAny(summaryRaw["error_count"]),
		AvailabilityPercent: dbutil.Float64FromAny(summaryRaw["availability_percent"]),
		AvgLatencyMs:        dbutil.Float64FromAny(summaryRaw["avg_latency_ms"]),
		P95LatencyMs:        dbutil.Float64FromAny(summaryRaw["p95_latency_ms"]),
	}

	query2 := `
		SELECT formatDateTime(toStartOfMinute(timestamp), '%Y-%m-%d %H:%i:00') as time_bucket,
		       sum(count) as request_count,
		       sum(if(status='ERROR', count, 0)) as error_count,
		       if(sum(count)>0, (sum(count)-sum(if(status='ERROR', count, 0)))*100.0/sum(count), 100.0) as availability_percent,
		       avg(avg) as avg_latency_ms
		FROM metrics
		WHERE team_id = ? AND metric_category = 'http' AND timestamp BETWEEN ? AND ?
	`
	args2 := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query2 += ` AND service_name = ?`
		args2 = append(args2, serviceName)
	}
	query2 += ` GROUP BY 1 ORDER BY 1 ASC`

	timeseriesRaw, err := dbutil.QueryMaps(r.db, query2, args2...)
	if err != nil {
		return model.SloSummary{}, nil, err
	}

	timeseries := make([]model.SloBucket, len(timeseriesRaw))
	for i, row := range timeseriesRaw {
		timeseries[i] = model.SloBucket{
			Timestamp:           dbutil.StringFromAny(row["time_bucket"]),
			RequestCount:        dbutil.Int64FromAny(row["request_count"]),
			ErrorCount:          dbutil.Int64FromAny(row["error_count"]),
			AvailabilityPercent: dbutil.Float64FromAny(row["availability_percent"]),
			AvgLatencyMs:        dbutil.NullableFloat64FromAny(row["avg_latency_ms"]),
		}
	}

	return summary, timeseries, nil
}

// GetInsightLogsStream queries log stream, volume trend, and facets.
func (r *ClickHouseRepository) GetInsightLogsStream(teamUUID string, startMs, endMs int64, limit int) ([]model.LogStreamItem, int64, []model.LogVolumeBucket, []model.Facet, []model.Facet, error) {
	streamRaw, err := dbutil.QueryMaps(r.db, `
		SELECT timestamp, level, service_name, logger, message, trace_id, span_id,
		       host, pod, container, thread, exception
		FROM logs
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		ORDER BY timestamp DESC
		LIMIT ?
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs), limit)
	if err != nil {
		return nil, 0, nil, nil, nil, err
	}

	stream := make([]model.LogStreamItem, len(streamRaw))
	for i, row := range streamRaw {
		stream[i] = model.LogStreamItem{
			Timestamp:   dbutil.StringFromAny(row["timestamp"]),
			Level:       dbutil.StringFromAny(row["level"]),
			ServiceName: dbutil.StringFromAny(row["service_name"]),
			Logger:      dbutil.StringFromAny(row["logger"]),
			Message:     dbutil.StringFromAny(row["message"]),
			TraceID:     dbutil.StringFromAny(row["trace_id"]),
			SpanID:      dbutil.StringFromAny(row["span_id"]),
			Host:        dbutil.StringFromAny(row["host"]),
			Pod:         dbutil.StringFromAny(row["pod"]),
			Container:   dbutil.StringFromAny(row["container"]),
			Thread:      dbutil.StringFromAny(row["thread"]),
			Exception:   dbutil.StringFromAny(row["exception"]),
		}
	}

	total := dbutil.QueryCount(r.db, `SELECT COUNT(*) FROM logs WHERE team_id = ? AND timestamp BETWEEN ? AND ?`,
		teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

	volumeRaw, err := dbutil.QueryMaps(r.db, `
		SELECT formatDateTime(toStartOfMinute(timestamp), '%Y-%m-%d %H:%i:00') as time_bucket,
		       COUNT(*) as log_count,
		       sum(if(trace_id != '', 1, 0)) as correlated_log_count
		FROM logs
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		GROUP BY 1
		ORDER BY 1 ASC
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, 0, nil, nil, nil, err
	}

	volume := make([]model.LogVolumeBucket, len(volumeRaw))
	for i, row := range volumeRaw {
		volume[i] = model.LogVolumeBucket{
			Timestamp:          dbutil.StringFromAny(row["time_bucket"]),
			LogCount:           dbutil.Int64FromAny(row["log_count"]),
			CorrelatedLogCount: dbutil.Int64FromAny(row["correlated_log_count"]),
		}
	}

	levelFacetsRaw, err := dbutil.QueryMaps(r.db, `
		SELECT level as name, COUNT(*) as count FROM logs
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		GROUP BY level ORDER BY count DESC
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, 0, nil, nil, nil, err
	}

	levelFacets := make([]model.Facet, len(levelFacetsRaw))
	for i, row := range levelFacetsRaw {
		levelFacets[i] = model.Facet{
			Name:  dbutil.StringFromAny(row["name"]),
			Count: dbutil.Int64FromAny(row["count"]),
		}
	}

	serviceFacetsRaw, err := dbutil.QueryMaps(r.db, `
		SELECT service_name as name, COUNT(*) as count FROM logs
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		GROUP BY service_name ORDER BY count DESC LIMIT 20
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, 0, nil, nil, nil, err
	}

	serviceFacets := make([]model.Facet, len(serviceFacetsRaw))
	for i, row := range serviceFacetsRaw {
		serviceFacets[i] = model.Facet{
			Name:  dbutil.StringFromAny(row["name"]),
			Count: dbutil.Int64FromAny(row["count"]),
		}
	}

	return stream, total, volume, levelFacets, serviceFacets, nil
}

// GetInsightDatabaseCache queries DB query latency and cache-hit insights.
func (r *ClickHouseRepository) GetInsightDatabaseCache(teamUUID string, startMs, endMs int64) (model.DbCacheSummary, []model.DbTableMetric, []model.DbSystemBreakdown, error) {
	summaryRaw, err := dbutil.QueryMap(r.db, `
		SELECT avg(JSONExtractFloat(attributes, 'db.query.latency.ms'))  as avg_query_latency_ms,
		       quantile(0.95)(JSONExtractFloat(attributes, 'db.query.latency.ms')) as p95_query_latency_ms,
		       sum(if(JSONExtractString(attributes, 'db.system') != '', 1, 0)) as db_span_count,
		       sum(if(JSONExtractString(attributes, 'cache.hit') = 'true', 1, 0))  as cache_hits,
		       sum(if(JSONExtractString(attributes, 'cache.hit') = 'false', 1, 0)) as cache_misses,
		       avg(JSONExtractFloat(attributes, 'db.replication.lag.ms')) as avg_replication_lag_ms
		FROM spans
		WHERE team_id = ? AND start_time BETWEEN ? AND ?
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

	if err != nil {
		return model.DbCacheSummary{}, nil, nil, err
	}

	summary := model.DbCacheSummary{
		AvgQueryLatencyMs:   dbutil.NullableFloat64FromAny(summaryRaw["avg_query_latency_ms"]),
		P95QueryLatencyMs:   dbutil.NullableFloat64FromAny(summaryRaw["p95_query_latency_ms"]),
		DbSpanCount:         dbutil.Int64FromAny(summaryRaw["db_span_count"]),
		CacheHits:           dbutil.Int64FromAny(summaryRaw["cache_hits"]),
		CacheMisses:         dbutil.Int64FromAny(summaryRaw["cache_misses"]),
		AvgReplicationLagMs: dbutil.NullableFloat64FromAny(summaryRaw["avg_replication_lag_ms"]),
	}

	tableMetricsRaw, err := dbutil.QueryMaps(r.db, `
		SELECT coalesce(nullIf(JSONExtractString(attributes, 'db.sql.table'), ''), 'unknown') as table_name,
		       service_name,
		       coalesce(nullIf(JSONExtractString(attributes, 'db.system'), ''), 'unknown') as db_system,
		       avg(JSONExtractFloat(attributes, 'db.query.latency.ms'))  as avg_query_latency_ms,
		       max(JSONExtractFloat(attributes, 'db.query.latency.ms'))  as max_query_latency_ms,
		       sum(if(JSONExtractString(attributes, 'cache.hit') = 'true',  1, 0)) as cache_hits,
		       sum(if(JSONExtractString(attributes, 'cache.hit') = 'false', 1, 0)) as cache_misses,
		       COUNT(*) as query_count
		FROM spans
		WHERE team_id = ? AND start_time BETWEEN ? AND ?
		  AND JSONExtractString(attributes, 'db.system') != ''
		GROUP BY table_name, service_name, db_system
		ORDER BY avg_query_latency_ms DESC
		LIMIT 50
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return model.DbCacheSummary{}, nil, nil, err
	}

	tableMetrics := make([]model.DbTableMetric, len(tableMetricsRaw))
	for i, row := range tableMetricsRaw {
		tableMetrics[i] = model.DbTableMetric{
			TableName:         dbutil.StringFromAny(row["table_name"]),
			ServiceName:       dbutil.StringFromAny(row["service_name"]),
			DbSystem:          dbutil.StringFromAny(row["db_system"]),
			AvgQueryLatencyMs: dbutil.NullableFloat64FromAny(row["avg_query_latency_ms"]),
			MaxQueryLatencyMs: dbutil.NullableFloat64FromAny(row["max_query_latency_ms"]),
			CacheHits:         dbutil.Int64FromAny(row["cache_hits"]),
			CacheMisses:       dbutil.Int64FromAny(row["cache_misses"]),
			QueryCount:        dbutil.Int64FromAny(row["query_count"]),
		}
	}

	// System breakdown query — groups by db.system to show per-database-type stats
	systemRaw, err := dbutil.QueryMaps(r.db, `
		SELECT coalesce(nullIf(JSONExtractString(attributes, 'db.system'), ''), 'unknown') as db_system,
		       COUNT(*) as query_count,
		       avg(JSONExtractFloat(attributes, 'db.query.latency.ms')) as avg_query_latency_ms,
		       quantile(0.95)(JSONExtractFloat(attributes, 'db.query.latency.ms')) as p95_query_latency_ms,
		       sum(if(status = 'ERROR', 1, 0)) as error_count,
		       COUNT(*) as span_count
		FROM spans
		WHERE team_id = ? AND start_time BETWEEN ? AND ?
		  AND JSONExtractString(attributes, 'db.system') != ''
		GROUP BY db_system
		ORDER BY query_count DESC
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return model.DbCacheSummary{}, nil, nil, err
	}

	systemBreakdown := make([]model.DbSystemBreakdown, len(systemRaw))
	for i, row := range systemRaw {
		systemBreakdown[i] = model.DbSystemBreakdown{
			DbSystem:        dbutil.StringFromAny(row["db_system"]),
			QueryCount:      dbutil.Int64FromAny(row["query_count"]),
			AvgQueryLatency: dbutil.NullableFloat64FromAny(row["avg_query_latency_ms"]),
			P95QueryLatency: dbutil.NullableFloat64FromAny(row["p95_query_latency_ms"]),
			ErrorCount:      dbutil.Int64FromAny(row["error_count"]),
			SpanCount:       dbutil.Int64FromAny(row["span_count"]),
		}
	}

	return summary, tableMetrics, systemBreakdown, err
}

// GetInsightMessagingQueue queries queue depth, lag, and rates.
func (r *ClickHouseRepository) GetInsightMessagingQueue(teamUUID string, startMs, endMs int64) (model.MqSummary, []model.MqBucket, []model.MqTopQueue, error) {
	durationSecs := float64(endMs-startMs) / 1000.0
	if durationSecs <= 0 {
		durationSecs = 1.0
	}

	summaryRaw, err := dbutil.QueryMap(r.db, `
		SELECT round(if(sum(queue_depth_samples) > 0, sum(queue_depth_sum) / sum(queue_depth_samples), NULL), 2) as avg_queue_depth,
		       round(max(max_queue_depth), 2) as max_queue_depth,
		       round(if(sum(consumer_lag_samples) > 0, sum(consumer_lag_sum) / sum(consumer_lag_samples), NULL), 2) as avg_consumer_lag,
		       round(max(max_consumer_lag), 2) as max_consumer_lag,
		       round(sum(publish_events) / ?, 2) as avg_publish_rate,
		       round(sum(receive_events) / ?, 2) as avg_receive_rate,
		       round(sum(processing_errors), 2) as processing_errors
		FROM (
		    SELECT sumIf(JSONExtractFloat(attributes, 'queue.depth'), JSONExtractFloat(attributes, 'queue.depth') > 0) as queue_depth_sum,
		           countIf(JSONExtractFloat(attributes, 'queue.depth') > 0) as queue_depth_samples,
		           maxIf(JSONExtractFloat(attributes, 'queue.depth'), JSONExtractFloat(attributes, 'queue.depth') > 0) as max_queue_depth,
		           sumIf(JSONExtractFloat(attributes, 'messaging.kafka.consumer.lag'), JSONExtractFloat(attributes, 'messaging.kafka.consumer.lag') > 0) as consumer_lag_sum,
		           countIf(JSONExtractFloat(attributes, 'messaging.kafka.consumer.lag') > 0) as consumer_lag_samples,
		           maxIf(JSONExtractFloat(attributes, 'messaging.kafka.consumer.lag'), JSONExtractFloat(attributes, 'messaging.kafka.consumer.lag') > 0) as max_consumer_lag,
		           toFloat64(sum(
		               if(
		                   span_kind = 'PRODUCER'
		                   OR JSONExtractString(attributes, 'messaging.operation') = 'publish'
		                   OR (JSONExtractFloat(attributes, 'queue.depth') > 0 AND upper(http_method) = 'POST'),
		                   1, 0
		               )
		           )) as publish_events,
		           toFloat64(sum(
		               if(
		                   span_kind = 'CONSUMER'
		                   OR JSONExtractString(attributes, 'messaging.operation') = 'receive'
		                   OR (JSONExtractFloat(attributes, 'queue.depth') > 0 AND upper(http_method) = 'GET'),
		                   1, 0
		               )
		           )) as receive_events,
		           sum(if(isFinite(JSONExtractFloat(attributes, 'messaging.error.count')), JSONExtractFloat(attributes, 'messaging.error.count'), 0.0)) as processing_errors
		    FROM spans
		    WHERE team_id = ? AND start_time BETWEEN ? AND ?

		    UNION ALL

		    SELECT sumIf(value, metric_name IN ('queue.depth', 'messaging.queue.depth', 'executor.queued') AND isFinite(value)) as queue_depth_sum,
		           countIf(metric_name IN ('queue.depth', 'messaging.queue.depth', 'executor.queued') AND isFinite(value)) as queue_depth_samples,
		           maxIf(value, metric_name IN ('queue.depth', 'messaging.queue.depth', 'executor.queued') AND isFinite(value)) as max_queue_depth,
		           sumIf(value, metric_name IN ('messaging.kafka.consumer.lag', 'messaging.kafka.consumer.records.lag', 'kafka.consumer.lag', 'kafka.consumer.records.lag', 'kafka.consumer.records-lag', 'executor.queued') AND isFinite(value)) as consumer_lag_sum,
		           countIf(metric_name IN ('messaging.kafka.consumer.lag', 'messaging.kafka.consumer.records.lag', 'kafka.consumer.lag', 'kafka.consumer.records.lag', 'kafka.consumer.records-lag', 'executor.queued') AND isFinite(value)) as consumer_lag_samples,
		           maxIf(value, metric_name IN ('messaging.kafka.consumer.lag', 'messaging.kafka.consumer.records.lag', 'kafka.consumer.lag', 'kafka.consumer.records.lag', 'kafka.consumer.records-lag', 'executor.queued') AND isFinite(value)) as max_consumer_lag,
		           toFloat64(0) as publish_events,
		           toFloat64(0) as receive_events,
		           sumIf(value, metric_name = 'logback.events' AND lower(JSONExtractString(attributes, 'level')) IN ('error', 'fatal') AND isFinite(value)) as processing_errors
		    FROM metrics
		    WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		) merged
	`, durationSecs, durationSecs,
		teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs),
		teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return model.MqSummary{}, nil, nil, err
	}

	summary := model.MqSummary{
		AvgQueueDepth:    dbutil.NullableFloat64FromAny(summaryRaw["avg_queue_depth"]),
		MaxQueueDepth:    dbutil.NullableFloat64FromAny(summaryRaw["max_queue_depth"]),
		AvgConsumerLag:   dbutil.NullableFloat64FromAny(summaryRaw["avg_consumer_lag"]),
		MaxConsumerLag:   dbutil.NullableFloat64FromAny(summaryRaw["max_consumer_lag"]),
		AvgPublishRate:   dbutil.Float64FromAny(summaryRaw["avg_publish_rate"]),
		AvgReceiveRate:   dbutil.Float64FromAny(summaryRaw["avg_receive_rate"]),
		ProcessingErrors: dbutil.Float64FromAny(summaryRaw["processing_errors"]),
	}

	timeseriesRaw, err := dbutil.QueryMaps(r.db, `
		SELECT time_bucket,
		       service_name,
		       queue_name,
		       any(messaging_system) as messaging_system,
		       round(if(sum(queue_depth_samples) > 0, sum(queue_depth_sum) / sum(queue_depth_samples), NULL), 2) as avg_queue_depth,
		       round(if(sum(consumer_lag_samples) > 0, sum(consumer_lag_sum) / sum(consumer_lag_samples), NULL), 2) as avg_consumer_lag,
		       round(sum(publish_events) / 60.0, 2) as avg_publish_rate,
		       round(sum(receive_events) / 60.0, 2) as avg_receive_rate
		FROM (
		    SELECT formatDateTime(toStartOfMinute(start_time), '%Y-%m-%dT%H:%i:%SZ') as time_bucket,
		           if(service_name != '', service_name, 'unknown') as service_name,
		           coalesce(
		               nullIf(JSONExtractString(attributes, 'messaging.queue.name'), ''),
		               nullIf(JSONExtractString(attributes, 'messaging.destination'), ''),
		               'unknown'
		           ) as queue_name,
		           coalesce(nullIf(JSONExtractString(attributes, 'messaging.system'), ''), 'kafka') as messaging_system,
		           sumIf(JSONExtractFloat(attributes, 'queue.depth'), JSONExtractFloat(attributes, 'queue.depth') > 0) as queue_depth_sum,
		           countIf(JSONExtractFloat(attributes, 'queue.depth') > 0) as queue_depth_samples,
		           sumIf(JSONExtractFloat(attributes, 'messaging.kafka.consumer.lag'), JSONExtractFloat(attributes, 'messaging.kafka.consumer.lag') > 0) as consumer_lag_sum,
		           countIf(JSONExtractFloat(attributes, 'messaging.kafka.consumer.lag') > 0) as consumer_lag_samples,
		           toFloat64(sum(
		               if(
		                   span_kind = 'PRODUCER'
		                   OR JSONExtractString(attributes, 'messaging.operation') = 'publish'
		                   OR (JSONExtractFloat(attributes, 'queue.depth') > 0 AND upper(http_method) = 'POST'),
		                   1, 0
		               )
		           )) as publish_events,
		           toFloat64(sum(
		               if(
		                   span_kind = 'CONSUMER'
		                   OR JSONExtractString(attributes, 'messaging.operation') = 'receive'
		                   OR (JSONExtractFloat(attributes, 'queue.depth') > 0 AND upper(http_method) = 'GET'),
		                   1, 0
		               )
		           )) as receive_events
		    FROM spans
		    WHERE team_id = ? AND start_time BETWEEN ? AND ?
		    GROUP BY 1, 2, 3, 4

		    UNION ALL

		    SELECT formatDateTime(toStartOfMinute(timestamp), '%Y-%m-%dT%H:%i:%SZ') as time_bucket,
		           if(service_name != '', service_name, 'unknown') as service_name,
		           coalesce(
		               nullIf(JSONExtractString(attributes, 'messaging.queue.name'), ''),
		               nullIf(JSONExtractString(attributes, 'messaging.destination'), ''),
		               nullIf(JSONExtractString(attributes, 'queue.name'), ''),
		               nullIf(JSONExtractString(attributes, 'name'), ''),
		               'unknown'
		           ) as queue_name,
		           coalesce(nullIf(JSONExtractString(attributes, 'messaging.system'), ''), 'kafka') as messaging_system,
		           sumIf(value, metric_name IN ('queue.depth', 'messaging.queue.depth', 'executor.queued') AND isFinite(value)) as queue_depth_sum,
		           countIf(metric_name IN ('queue.depth', 'messaging.queue.depth', 'executor.queued') AND isFinite(value)) as queue_depth_samples,
		           sumIf(value, metric_name IN ('messaging.kafka.consumer.lag', 'messaging.kafka.consumer.records.lag', 'kafka.consumer.lag', 'kafka.consumer.records.lag', 'kafka.consumer.records-lag', 'executor.queued') AND isFinite(value)) as consumer_lag_sum,
		           countIf(metric_name IN ('messaging.kafka.consumer.lag', 'messaging.kafka.consumer.records.lag', 'kafka.consumer.lag', 'kafka.consumer.records.lag', 'kafka.consumer.records-lag', 'executor.queued') AND isFinite(value)) as consumer_lag_samples,
		           toFloat64(0) as publish_events,
		           toFloat64(0) as receive_events
		    FROM metrics
		    WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		      AND metric_name IN (
		          'queue.depth',
		          'messaging.queue.depth',
		          'executor.queued',
		          'messaging.kafka.consumer.lag',
		          'messaging.kafka.consumer.records.lag',
		          'kafka.consumer.lag',
		          'kafka.consumer.records.lag',
		          'kafka.consumer.records-lag'
		      )
		    GROUP BY 1, 2, 3, 4
		) merged
		GROUP BY time_bucket, service_name, queue_name
		ORDER BY time_bucket ASC, service_name ASC, queue_name ASC
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs), teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return model.MqSummary{}, nil, nil, err
	}

	timeseries := make([]model.MqBucket, len(timeseriesRaw))
	for i, row := range timeseriesRaw {
		timeseries[i] = model.MqBucket{
			Timestamp:       dbutil.StringFromAny(row["time_bucket"]),
			ServiceName:     dbutil.StringFromAny(row["service_name"]),
			QueueName:       dbutil.StringFromAny(row["queue_name"]),
			MessagingSystem: dbutil.StringFromAny(row["messaging_system"]),
			AvgQueueDepth:   dbutil.NullableFloat64FromAny(row["avg_queue_depth"]),
			AvgConsumerLag:  dbutil.NullableFloat64FromAny(row["avg_consumer_lag"]),
			AvgPublishRate:  dbutil.Float64FromAny(row["avg_publish_rate"]),
			AvgReceiveRate:  dbutil.Float64FromAny(row["avg_receive_rate"]),
		}
	}

	topQueuesRaw, err := dbutil.QueryMaps(r.db, `
		SELECT queue_name,
		       service_name,
		       any(messaging_system) as messaging_system,
		       round(if(sum(queue_depth_samples) > 0, sum(queue_depth_sum) / sum(queue_depth_samples), NULL), 2) as avg_queue_depth,
		       round(max(max_consumer_lag), 2) as max_consumer_lag,
		       round(sum(publish_events) / ?, 2) as avg_publish_rate,
		       round(sum(receive_events) / ?, 2) as avg_receive_rate,
		       toInt64(sum(sample_count)) as sample_count
		FROM (
		    SELECT coalesce(
		               nullIf(JSONExtractString(attributes, 'messaging.queue.name'), ''),
		               nullIf(JSONExtractString(attributes, 'messaging.destination'), ''),
		               'unknown'
		           ) as queue_name,
		           if(service_name != '', service_name, 'unknown') as service_name,
		           coalesce(nullIf(JSONExtractString(attributes, 'messaging.system'), ''), 'kafka') as messaging_system,
		           sumIf(JSONExtractFloat(attributes, 'queue.depth'), JSONExtractFloat(attributes, 'queue.depth') > 0) as queue_depth_sum,
		           countIf(JSONExtractFloat(attributes, 'queue.depth') > 0) as queue_depth_samples,
		           maxIf(JSONExtractFloat(attributes, 'messaging.kafka.consumer.lag'), JSONExtractFloat(attributes, 'messaging.kafka.consumer.lag') > 0) as max_consumer_lag,
		           toFloat64(sum(
		               if(
		                   span_kind = 'PRODUCER'
		                   OR JSONExtractString(attributes, 'messaging.operation') = 'publish'
		                   OR (JSONExtractFloat(attributes, 'queue.depth') > 0 AND upper(http_method) = 'POST'),
		                   1, 0
		               )
		           )) as publish_events,
		           toFloat64(sum(
		               if(
		                   span_kind = 'CONSUMER'
		                   OR JSONExtractString(attributes, 'messaging.operation') = 'receive'
		                   OR (JSONExtractFloat(attributes, 'queue.depth') > 0 AND upper(http_method) = 'GET'),
		                   1, 0
		               )
		           )) as receive_events,
		           toInt64(count()) as sample_count
		    FROM spans
		    WHERE team_id = ? AND start_time BETWEEN ? AND ?
		    GROUP BY queue_name, service_name, messaging_system

		    UNION ALL

		    SELECT coalesce(
		               nullIf(JSONExtractString(attributes, 'messaging.queue.name'), ''),
		               nullIf(JSONExtractString(attributes, 'messaging.destination'), ''),
		               nullIf(JSONExtractString(attributes, 'queue.name'), ''),
		               nullIf(JSONExtractString(attributes, 'name'), ''),
		               'unknown'
		           ) as queue_name,
		           if(service_name != '', service_name, 'unknown') as service_name,
		           coalesce(nullIf(JSONExtractString(attributes, 'messaging.system'), ''), 'kafka') as messaging_system,
		           sumIf(value, metric_name IN ('queue.depth', 'messaging.queue.depth', 'executor.queued') AND isFinite(value)) as queue_depth_sum,
		           countIf(metric_name IN ('queue.depth', 'messaging.queue.depth', 'executor.queued') AND isFinite(value)) as queue_depth_samples,
		           maxIf(value, metric_name IN ('messaging.kafka.consumer.lag', 'messaging.kafka.consumer.records.lag', 'kafka.consumer.lag', 'kafka.consumer.records.lag', 'kafka.consumer.records-lag', 'executor.queued') AND isFinite(value)) as max_consumer_lag,
		           toFloat64(0) as publish_events,
		           toFloat64(0) as receive_events,
		           toInt64(count()) as sample_count
		    FROM metrics
		    WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		      AND metric_name IN (
		          'queue.depth',
		          'messaging.queue.depth',
		          'executor.queued',
		          'messaging.kafka.consumer.lag',
		          'messaging.kafka.consumer.records.lag',
		          'kafka.consumer.lag',
		          'kafka.consumer.records.lag',
		          'kafka.consumer.records-lag'
		      )
		    GROUP BY queue_name, service_name, messaging_system
		) merged
		GROUP BY queue_name, service_name
		ORDER BY avg_queue_depth DESC, max_consumer_lag DESC, sample_count DESC
		LIMIT 50
	`, durationSecs, durationSecs,
		teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs),
		teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return model.MqSummary{}, nil, nil, err
	}

	topQueues := make([]model.MqTopQueue, len(topQueuesRaw))
	for i, row := range topQueuesRaw {
		topQueues[i] = model.MqTopQueue{
			QueueName:       dbutil.StringFromAny(row["queue_name"]),
			ServiceName:     dbutil.StringFromAny(row["service_name"]),
			MessagingSystem: dbutil.StringFromAny(row["messaging_system"]),
			AvgQueueDepth:   dbutil.NullableFloat64FromAny(row["avg_queue_depth"]),
			MaxConsumerLag:  dbutil.NullableFloat64FromAny(row["max_consumer_lag"]),
			AvgPublishRate:  dbutil.Float64FromAny(row["avg_publish_rate"]),
			AvgReceiveRate:  dbutil.Float64FromAny(row["avg_receive_rate"]),
			SampleCount:     dbutil.Int64FromAny(row["sample_count"]),
		}
	}

	return summary, timeseries, topQueues, nil
}
