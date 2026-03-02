package resource_utilisation

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

// resBucketExpr returns a ClickHouse time-bucketing expression for adaptive granularity.
func resBucketExpr(startMs, endMs int64) string {
	hours := (endMs - startMs) / 3_600_000
	switch {
	case hours <= 3:
		return "formatDateTime(toStartOfMinute(timestamp), '%Y-%m-%dT%H:%i:%SZ')"
	case hours <= 24:
		return "formatDateTime(toStartOfFiveMinutes(timestamp), '%Y-%m-%dT%H:%i:%SZ')"
	case hours <= 168:
		return "formatDateTime(toStartOfHour(timestamp), '%Y-%m-%dT%H:%i:%SZ')"
	default:
		return "formatDateTime(toStartOfDay(timestamp), '%Y-%m-%dT%H:%i:%SZ')"
	}
}

// Repository encapsulates data access logic for resource utilization.
type Repository interface {
	GetAvgCPU(teamUUID string, startMs, endMs int64) (MetricValue, error)
	GetAvgMemory(teamUUID string, startMs, endMs int64) (MetricValue, error)
	GetAvgNetwork(teamUUID string, startMs, endMs int64) (MetricValue, error)
	GetAvgConnPool(teamUUID string, startMs, endMs int64) (MetricValue, error)
	GetCPUUsagePercentage(teamUUID string, startMs, endMs int64) ([]ResourceBucket, error)
	GetMemoryUsagePercentage(teamUUID string, startMs, endMs int64) ([]ResourceBucket, error)
	GetResourceUsageByService(teamUUID string, startMs, endMs int64) ([]ServiceResource, error)
	GetResourceUsageByInstance(teamUUID string, startMs, endMs int64) ([]InstanceResource, error)
}

type ClickHouseRepository struct {
	db dbutil.Querier
}

// NewRepository creates a new Resource Utilization Repository.
func NewRepository(db dbutil.Querier) Repository {
	return &ClickHouseRepository{db: db}
}

// getAvgMetric is a helper to run the massive grouping query and return a single average.
func (r *ClickHouseRepository) getAvgMetric(metricField string, teamUUID string, startMs, endMs int64) (MetricValue, error) {
	// We calculate the average of the service-level averages, just like the frontend did.
	query := `
		SELECT avg(metric_val) as value FROM (
			` + getByServiceQuerySelect(metricField) + `
			` + getByServiceQueryFromWhere() + `
		) WHERE metric_val IS NOT NULL
	`
	row, err := dbutil.QueryMap(r.db, query, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return MetricValue{Value: 0}, err
	}
	return MetricValue{Value: dbutil.Float64FromAny(row["value"])}, nil
}

func getByServiceQuerySelect(singleMetric string) string {
	cpuCol := `coalesce(
		if(countIf(metric_name IN ('system.cpu.utilization', 'system.cpu.usage', 'process.cpu.usage') AND isFinite(value)) > 0, avgIf(if(value <= 1.0, value * 100.0, value), metric_name IN ('system.cpu.utilization', 'system.cpu.usage', 'process.cpu.usage') AND isFinite(value)), NULL),
		if(countIf(JSONExtractFloat(attributes, 'system.cpu.utilization') > 0) > 0, avgIf(if(JSONExtractFloat(attributes, 'system.cpu.utilization') <= 1.0, JSONExtractFloat(attributes, 'system.cpu.utilization') * 100.0, JSONExtractFloat(attributes, 'system.cpu.utilization')), JSONExtractFloat(attributes, 'system.cpu.utilization') > 0), NULL)
	)`
	memCol := `coalesce(
		if(countIf(metric_name = 'system.memory.utilization' AND isFinite(value)) > 0, avgIf(if(value <= 1.0, value * 100.0, value), metric_name = 'system.memory.utilization' AND isFinite(value)), NULL),
		if(sumIf(value, metric_name = 'jvm.memory.max' AND value > 0 AND isFinite(value)) > 0, 100.0 * sumIf(value, metric_name = 'jvm.memory.used' AND value >= 0 AND isFinite(value)) / nullIf(sumIf(value, metric_name = 'jvm.memory.max' AND value > 0 AND isFinite(value)), 0), NULL),
		if(countIf(JSONExtractFloat(attributes, 'system.memory.utilization') > 0) > 0, avgIf(if(JSONExtractFloat(attributes, 'system.memory.utilization') <= 1.0, JSONExtractFloat(attributes, 'system.memory.utilization') * 100.0, JSONExtractFloat(attributes, 'system.memory.utilization')), JSONExtractFloat(attributes, 'system.memory.utilization') > 0), NULL)
	)`
	diskCol := `coalesce(
		if(countIf(metric_name = 'system.disk.utilization' AND isFinite(value)) > 0, avgIf(if(value <= 1.0, value * 100.0, value), metric_name = 'system.disk.utilization' AND isFinite(value)), NULL),
		if(sumIf(value, metric_name = 'disk.total' AND value > 0 AND isFinite(value)) > 0, 100.0 * (1.0 - (sumIf(value, metric_name = 'disk.free' AND value >= 0 AND isFinite(value)) / nullIf(sumIf(value, metric_name = 'disk.total' AND value > 0 AND isFinite(value)), 0))), NULL),
		if(countIf(JSONExtractFloat(attributes, 'system.disk.utilization') > 0) > 0, avgIf(if(JSONExtractFloat(attributes, 'system.disk.utilization') <= 1.0, JSONExtractFloat(attributes, 'system.disk.utilization') * 100.0, JSONExtractFloat(attributes, 'system.disk.utilization')), JSONExtractFloat(attributes, 'system.disk.utilization') > 0), NULL)
	)`
	netCol := `coalesce(
		if(countIf(metric_name = 'system.network.utilization' AND isFinite(value)) > 0, avgIf(if(value <= 1.0, value * 100.0, value), metric_name = 'system.network.utilization' AND isFinite(value)), NULL),
		if(countIf(metric_name = 'http.server.requests.active.active' AND value > 0 AND isFinite(value)) > 0, avgIf(if(value <= 1.0, value * 100.0, least(value, 100.0)), metric_name = 'http.server.requests.active.active' AND value > 0 AND isFinite(value)), NULL),
		if(countIf(metric_name = 'http.server.request.count' AND value > 0 AND isFinite(value)) > 0, avgIf(least(value, 100.0), metric_name = 'http.server.request.count' AND value > 0 AND isFinite(value)), NULL),
		if(countIf(JSONExtractFloat(attributes, 'system.network.utilization') > 0) > 0, avgIf(if(JSONExtractFloat(attributes, 'system.network.utilization') <= 1.0, JSONExtractFloat(attributes, 'system.network.utilization') * 100.0, JSONExtractFloat(attributes, 'system.network.utilization')), JSONExtractFloat(attributes, 'system.network.utilization') > 0), NULL)
	)`
	connCol := `coalesce(
		if(countIf(metric_name = 'db.connection.pool.utilization' AND isFinite(value)) > 0, avgIf(if(value <= 1.0, value * 100.0, value), metric_name = 'db.connection.pool.utilization' AND isFinite(value)), NULL),
		if(sumIf(value, metric_name = 'hikaricp.connections.max' AND value > 0 AND isFinite(value)) > 0, 100.0 * sumIf(value, metric_name = 'hikaricp.connections.active' AND value >= 0 AND isFinite(value)) / nullIf(sumIf(value, metric_name = 'hikaricp.connections.max' AND value > 0 AND isFinite(value)), 0), NULL),
		if(countIf(metric_name = 'executor.pool.size' AND value > 0 AND isFinite(value)) > 0, avgIf(if(value <= 1.0, value * 100.0, least(value, 100.0)), metric_name = 'executor.pool.size' AND value > 0 AND isFinite(value)), NULL),
		if(countIf(metric_name = 'http.server.request.count' AND value > 0 AND isFinite(value)) > 0, avgIf(least(value, 100.0), metric_name = 'http.server.request.count' AND value > 0 AND isFinite(value)), NULL),
		if(countIf(JSONExtractFloat(attributes, 'db.connection_pool.utilization') > 0) > 0, avgIf(if(JSONExtractFloat(attributes, 'db.connection_pool.utilization') <= 1.0, JSONExtractFloat(attributes, 'db.connection_pool.utilization') * 100.0, JSONExtractFloat(attributes, 'db.connection_pool.utilization')), JSONExtractFloat(attributes, 'db.connection_pool.utilization') > 0), NULL)
	)`

	if singleMetric == "cpu" {
		return "SELECT " + cpuCol + " as metric_val"
	} else if singleMetric == "memory" {
		return "SELECT " + memCol + " as metric_val"
	} else if singleMetric == "network" {
		return "SELECT " + netCol + " as metric_val"
	} else if singleMetric == "connection_pool" {
		return "SELECT " + connCol + " as metric_val"
	}

	return `SELECT service_name,
		` + cpuCol + ` as avg_cpu_util,
		` + memCol + ` as avg_memory_util,
		` + diskCol + ` as avg_disk_util,
		` + netCol + ` as avg_network_util,
		` + connCol + ` as avg_connection_pool_util,
		COUNT(*) as sample_count`
}

func getByInstanceQuerySelectFull() string {
	cpuCol := `coalesce(
		if(countIf(metric_name IN ('system.cpu.utilization', 'system.cpu.usage', 'process.cpu.usage') AND isFinite(value)) > 0, avgIf(if(value <= 1.0, value * 100.0, value), metric_name IN ('system.cpu.utilization', 'system.cpu.usage', 'process.cpu.usage') AND isFinite(value)), NULL),
		if(countIf(JSONExtractFloat(attributes, 'system.cpu.utilization') > 0) > 0, avgIf(if(JSONExtractFloat(attributes, 'system.cpu.utilization') <= 1.0, JSONExtractFloat(attributes, 'system.cpu.utilization') * 100.0, JSONExtractFloat(attributes, 'system.cpu.utilization')), JSONExtractFloat(attributes, 'system.cpu.utilization') > 0), NULL)
	)`
	memCol := `coalesce(
		if(countIf(metric_name = 'system.memory.utilization' AND isFinite(value)) > 0, avgIf(if(value <= 1.0, value * 100.0, value), metric_name = 'system.memory.utilization' AND isFinite(value)), NULL),
		if(sumIf(value, metric_name = 'jvm.memory.max' AND value > 0 AND isFinite(value)) > 0, 100.0 * sumIf(value, metric_name = 'jvm.memory.used' AND value >= 0 AND isFinite(value)) / nullIf(sumIf(value, metric_name = 'jvm.memory.max' AND value > 0 AND isFinite(value)), 0), NULL),
		if(countIf(JSONExtractFloat(attributes, 'system.memory.utilization') > 0) > 0, avgIf(if(JSONExtractFloat(attributes, 'system.memory.utilization') <= 1.0, JSONExtractFloat(attributes, 'system.memory.utilization') * 100.0, JSONExtractFloat(attributes, 'system.memory.utilization')), JSONExtractFloat(attributes, 'system.memory.utilization') > 0), NULL)
	)`
	diskCol := `coalesce(
		if(countIf(metric_name = 'system.disk.utilization' AND isFinite(value)) > 0, avgIf(if(value <= 1.0, value * 100.0, value), metric_name = 'system.disk.utilization' AND isFinite(value)), NULL),
		if(sumIf(value, metric_name = 'disk.total' AND value > 0 AND isFinite(value)) > 0, 100.0 * (1.0 - (sumIf(value, metric_name = 'disk.free' AND value >= 0 AND isFinite(value)) / nullIf(sumIf(value, metric_name = 'disk.total' AND value > 0 AND isFinite(value)), 0))), NULL),
		if(countIf(JSONExtractFloat(attributes, 'system.disk.utilization') > 0) > 0, avgIf(if(JSONExtractFloat(attributes, 'system.disk.utilization') <= 1.0, JSONExtractFloat(attributes, 'system.disk.utilization') * 100.0, JSONExtractFloat(attributes, 'system.disk.utilization')), JSONExtractFloat(attributes, 'system.disk.utilization') > 0), NULL)
	)`
	netCol := `coalesce(
		if(countIf(metric_name = 'system.network.utilization' AND isFinite(value)) > 0, avgIf(if(value <= 1.0, value * 100.0, value), metric_name = 'system.network.utilization' AND isFinite(value)), NULL),
		if(countIf(metric_name = 'http.server.requests.active.active' AND value > 0 AND isFinite(value)) > 0, avgIf(if(value <= 1.0, value * 100.0, least(value, 100.0)), metric_name = 'http.server.requests.active.active' AND value > 0 AND isFinite(value)), NULL),
		if(countIf(metric_name = 'http.server.request.count' AND value > 0 AND isFinite(value)) > 0, avgIf(least(value, 100.0), metric_name = 'http.server.request.count' AND value > 0 AND isFinite(value)), NULL),
		if(countIf(JSONExtractFloat(attributes, 'system.network.utilization') > 0) > 0, avgIf(if(JSONExtractFloat(attributes, 'system.network.utilization') <= 1.0, JSONExtractFloat(attributes, 'system.network.utilization') * 100.0, JSONExtractFloat(attributes, 'system.network.utilization')), JSONExtractFloat(attributes, 'system.network.utilization') > 0), NULL)
	)`
	connCol := `coalesce(
		if(countIf(metric_name = 'db.connection.pool.utilization' AND isFinite(value)) > 0, avgIf(if(value <= 1.0, value * 100.0, value), metric_name = 'db.connection.pool.utilization' AND isFinite(value)), NULL),
		if(sumIf(value, metric_name = 'hikaricp.connections.max' AND value > 0 AND isFinite(value)) > 0, 100.0 * sumIf(value, metric_name = 'hikaricp.connections.active' AND value >= 0 AND isFinite(value)) / nullIf(sumIf(value, metric_name = 'hikaricp.connections.max' AND value > 0 AND isFinite(value)), 0), NULL),
		if(countIf(metric_name = 'executor.pool.size' AND value > 0 AND isFinite(value)) > 0, avgIf(if(value <= 1.0, value * 100.0, least(value, 100.0)), metric_name = 'executor.pool.size' AND value > 0 AND isFinite(value)), NULL),
		if(countIf(metric_name = 'http.server.request.count' AND value > 0 AND isFinite(value)) > 0, avgIf(least(value, 100.0), metric_name = 'http.server.request.count' AND value > 0 AND isFinite(value)), NULL),
		if(countIf(JSONExtractFloat(attributes, 'db.connection_pool.utilization') > 0) > 0, avgIf(if(JSONExtractFloat(attributes, 'db.connection_pool.utilization') <= 1.0, JSONExtractFloat(attributes, 'db.connection_pool.utilization') * 100.0, JSONExtractFloat(attributes, 'db.connection_pool.utilization')), JSONExtractFloat(attributes, 'db.connection_pool.utilization') > 0), NULL)
	)`
	return `SELECT host, pod, container, service_name,
		` + cpuCol + ` as avg_cpu_util,
		` + memCol + ` as avg_memory_util,
		` + diskCol + ` as avg_disk_util,
		` + netCol + ` as avg_network_util,
		` + connCol + ` as avg_connection_pool_util,
		COUNT(*) as sample_count`
}

func getByServiceQueryFromWhere() string {
	return `FROM metrics
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		  AND (
		      metric_name IN (
		          'system.cpu.utilization', 'system.cpu.usage', 'process.cpu.usage', 'system.memory.utilization', 'jvm.memory.used', 'jvm.memory.max',
		          'system.disk.utilization', 'disk.free', 'disk.total', 'system.network.utilization', 'http.server.requests.active.active',
		          'http.server.request.count', 'db.connection.pool.utilization', 'hikaricp.connections.active', 'hikaricp.connections.max', 'executor.pool.size'
		      )
		      OR JSONExtractFloat(attributes, 'system.cpu.utilization') > 0 OR JSONExtractFloat(attributes, 'system.memory.utilization') > 0
		      OR JSONExtractFloat(attributes, 'system.disk.utilization') > 0 OR JSONExtractFloat(attributes, 'system.network.utilization') > 0
		      OR JSONExtractFloat(attributes, 'db.connection_pool.utilization') > 0
		  )
		GROUP BY service_name
		HAVING service_name != ''`
}

func (r *ClickHouseRepository) GetAvgCPU(teamUUID string, startMs, endMs int64) (MetricValue, error) {
	return r.getAvgMetric("cpu", teamUUID, startMs, endMs)
}

func (r *ClickHouseRepository) GetAvgMemory(teamUUID string, startMs, endMs int64) (MetricValue, error) {
	return r.getAvgMetric("memory", teamUUID, startMs, endMs)
}

func (r *ClickHouseRepository) GetAvgNetwork(teamUUID string, startMs, endMs int64) (MetricValue, error) {
	return r.getAvgMetric("network", teamUUID, startMs, endMs)
}

func (r *ClickHouseRepository) GetAvgConnPool(teamUUID string, startMs, endMs int64) (MetricValue, error) {
	return r.getAvgMetric("connection_pool", teamUUID, startMs, endMs)
}

func (r *ClickHouseRepository) GetCPUUsagePercentage(teamUUID string, startMs, endMs int64) ([]ResourceBucket, error) {
	bucket := resBucketExpr(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket,
		       service_name as pod,
		       `+getByServiceQuerySelect("cpu")[7:]+`
		FROM metrics
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		  AND (
		      metric_name IN ('system.cpu.utilization', 'system.cpu.usage', 'process.cpu.usage')
		      OR JSONExtractFloat(attributes, 'system.cpu.utilization') > 0
		  )
		GROUP BY 1, 2
		HAVING pod != ''
		ORDER BY 1 ASC, 2 ASC
	`, bucket)
	rows, err := dbutil.QueryMaps(r.db, query, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}
	buckets := make([]ResourceBucket, len(rows))
	for i, row := range rows {
		buckets[i] = ResourceBucket{
			Timestamp: dbutil.StringFromAny(row["time_bucket"]),
			Pod:       dbutil.StringFromAny(row["pod"]),
			Value:     dbutil.NullableFloat64FromAny(row["metric_val"]),
		}
	}
	return buckets, nil
}

func (r *ClickHouseRepository) GetMemoryUsagePercentage(teamUUID string, startMs, endMs int64) ([]ResourceBucket, error) {
	bucket := resBucketExpr(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket,
		       service_name as pod,
		       `+getByServiceQuerySelect("memory")[7:]+`
		FROM metrics
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		  AND (
		      metric_name IN ('system.memory.utilization', 'jvm.memory.used', 'jvm.memory.max')
		      OR JSONExtractFloat(attributes, 'system.memory.utilization') > 0
		  )
		GROUP BY 1, 2
		HAVING pod != ''
		ORDER BY 1 ASC, 2 ASC
	`, bucket)
	rows, err := dbutil.QueryMaps(r.db, query, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}
	buckets := make([]ResourceBucket, len(rows))
	for i, row := range rows {
		buckets[i] = ResourceBucket{
			Timestamp: dbutil.StringFromAny(row["time_bucket"]),
			Pod:       dbutil.StringFromAny(row["pod"]),
			Value:     dbutil.NullableFloat64FromAny(row["metric_val"]),
		}
	}
	return buckets, nil
}

func (r *ClickHouseRepository) GetResourceUsageByService(teamUUID string, startMs, endMs int64) ([]ServiceResource, error) {
	query := getByServiceQuerySelect("") + " " + getByServiceQueryFromWhere() + " ORDER BY service_name ASC"
	byServiceRaw, err := dbutil.QueryMaps(r.db, query, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}
	byService := make([]ServiceResource, len(byServiceRaw))
	for i, row := range byServiceRaw {
		byService[i] = ServiceResource{
			ServiceName:           dbutil.StringFromAny(row["service_name"]),
			AvgCpuUtil:            dbutil.NullableFloat64FromAny(row["avg_cpu_util"]),
			AvgMemoryUtil:         dbutil.NullableFloat64FromAny(row["avg_memory_util"]),
			AvgDiskUtil:           dbutil.NullableFloat64FromAny(row["avg_disk_util"]),
			AvgNetworkUtil:        dbutil.NullableFloat64FromAny(row["avg_network_util"]),
			AvgConnectionPoolUtil: dbutil.NullableFloat64FromAny(row["avg_connection_pool_util"]),
			SampleCount:           dbutil.Int64FromAny(row["sample_count"]),
		}
	}
	return byService, nil
}

func (r *ClickHouseRepository) GetResourceUsageByInstance(teamUUID string, startMs, endMs int64) ([]InstanceResource, error) {
	query := getByInstanceQuerySelectFull() + `
		FROM metrics
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		  AND (
		      metric_name IN (
		          'system.cpu.utilization', 'system.cpu.usage', 'process.cpu.usage', 'system.memory.utilization', 'jvm.memory.used', 'jvm.memory.max',
		          'system.disk.utilization', 'disk.free', 'disk.total', 'system.network.utilization', 'http.server.requests.active.active',
		          'http.server.request.count', 'db.connection.pool.utilization', 'hikaricp.connections.active', 'hikaricp.connections.max', 'executor.pool.size'
		      )
		      OR JSONExtractFloat(attributes, 'system.cpu.utilization') > 0 OR JSONExtractFloat(attributes, 'system.memory.utilization') > 0
		      OR JSONExtractFloat(attributes, 'system.disk.utilization') > 0 OR JSONExtractFloat(attributes, 'system.network.utilization') > 0
		      OR JSONExtractFloat(attributes, 'db.connection_pool.utilization') > 0
		  )
		GROUP BY host, pod, container, service_name
		HAVING service_name != ''
		ORDER BY sample_count DESC
		LIMIT 200
	`
	byInstanceRaw, err := dbutil.QueryMaps(r.db, query, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}
	byInstance := make([]InstanceResource, len(byInstanceRaw))
	for i, row := range byInstanceRaw {
		byInstance[i] = InstanceResource{
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
	return byInstance, nil
}
