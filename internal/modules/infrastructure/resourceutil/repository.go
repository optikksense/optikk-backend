package resourceutil //nolint:misspell

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
	"github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/connpool"
	"github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/cpu"
	"github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/disk"
	"github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/infraconsts"
	"github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/memory"
	"github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/network"
)

// Repository is a thin orchestrator that delegates metric queries to the
// domain-specific cpu, memory, disk, network, and connpool submodules and
// composes cross-resource aggregation responses.
type Repository interface {
	GetAvgCPU(ctx context.Context, teamID int64, startMs, endMs int64) (MetricValue, error)
	GetAvgMemory(ctx context.Context, teamID int64, startMs, endMs int64) (MetricValue, error)
	GetAvgNetwork(ctx context.Context, teamID int64, startMs, endMs int64) (MetricValue, error)
	GetAvgConnPool(ctx context.Context, teamID int64, startMs, endMs int64) (MetricValue, error)
	GetCPUUsagePercentage(ctx context.Context, teamID int64, startMs, endMs int64) ([]ResourceBucket, error)
	GetMemoryUsagePercentage(ctx context.Context, teamID int64, startMs, endMs int64) ([]ResourceBucket, error)
	GetResourceUsageByService(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServiceResource, error)
	GetResourceUsageByInstance(ctx context.Context, teamID int64, startMs, endMs int64) ([]InstanceResource, error)
}

type compositeRepository struct {
	chDB clickhouse.Conn
	cpu  *cpu.Service
	mem  *memory.Service
	dsk  *disk.Service
	net  *network.Service
	conn *connpool.Service
}

// NewRepository creates a composite repository that delegates to domain
// submodule services (the high-level methods now live on Service after the
// kafka-style refactor — repos hold queries only).
func NewRepository(
	chDB clickhouse.Conn,
	cpuSvc *cpu.Service,
	memSvc *memory.Service,
	dskSvc *disk.Service,
	netSvc *network.Service,
	connSvc *connpool.Service,
) Repository {
	return &compositeRepository{
		chDB: chDB,
		cpu:  cpuSvc,
		mem:  memSvc,
		dsk:  dskSvc,
		net:  netSvc,
		conn: connSvc,
	}
}

// ---- Delegated single-metric endpoints (backward compatible) ----

func (r *compositeRepository) GetAvgCPU(ctx context.Context, teamID int64, startMs, endMs int64) (MetricValue, error) {
	v, err := r.cpu.GetAvgCPU(ctx, teamID, startMs, endMs)
	return MetricValue{Value: v.Value}, err
}

func (r *compositeRepository) GetAvgMemory(ctx context.Context, teamID int64, startMs, endMs int64) (MetricValue, error) {
	v, err := r.mem.GetAvgMemory(ctx, teamID, startMs, endMs)
	return MetricValue{Value: v.Value}, err
}

func (r *compositeRepository) GetAvgNetwork(ctx context.Context, teamID int64, startMs, endMs int64) (MetricValue, error) {
	v, err := r.net.GetAvgNetwork(ctx, teamID, startMs, endMs)
	return MetricValue{Value: v.Value}, err
}

func (r *compositeRepository) GetAvgConnPool(ctx context.Context, teamID int64, startMs, endMs int64) (MetricValue, error) {
	v, err := r.conn.GetAvgConnPool(ctx, teamID, startMs, endMs)
	return MetricValue{Value: v.Value}, err
}

func (r *compositeRepository) GetCPUUsagePercentage(ctx context.Context, teamID int64, startMs, endMs int64) ([]ResourceBucket, error) {
	rows, err := r.cpu.GetCPUUsagePercentage(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	out := make([]ResourceBucket, len(rows))
	for i, row := range rows {
		out[i] = ResourceBucket{Timestamp: row.Timestamp, Pod: row.Pod, Value: row.Value}
	}
	return out, nil
}

func (r *compositeRepository) GetMemoryUsagePercentage(ctx context.Context, teamID int64, startMs, endMs int64) ([]ResourceBucket, error) {
	rows, err := r.mem.GetMemoryUsagePercentage(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	out := make([]ResourceBucket, len(rows))
	for i, row := range rows {
		out[i] = ResourceBucket{Timestamp: row.Timestamp, Pod: row.Pod, Value: row.Value}
	}
	return out, nil
}

// ---- Composite cross-resource endpoints ----

func (r *compositeRepository) GetResourceUsageByService(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServiceResource, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name IN @metricNames
		)
		SELECT
		    service     AS service,
		    metric_name AS metric_name,
		    avg(value)  AS avg_value
		FROM observability.metrics
		PREWHERE team_id        = @teamID
		     AND ts_bucket_hour BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name IN @metricNames
		  AND timestamp BETWEEN @start AND @end
		  AND service != ''
		GROUP BY service, metric_name`

	bucketStart, bucketEnd := timebucket.MetricsHourBucket(startMs/1000), timebucket.MetricsHourBucket(endMs/1000).Add(time.Hour)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec
		clickhouse.Named("bucketStart", bucketStart),
		clickhouse.Named("bucketEnd", bucketEnd),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricNames", infraconsts.AllResourceMetrics),
	}

	type row struct {
		ServiceName string  `ch:"service"`
		MetricName  string  `ch:"metric_name"`
		AvgValue    float64 `ch:"avg_value"`
	}
	var rows []row
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.chDB, "resourceutil.GetResourceUsageByService", &rows, query, args...); err != nil {
		return nil, err
	}

	// Fold rows by service. Service holds the cross-resource composition; the
	// repo just returns raw rows.
	serviceMap := make(map[string]*ServiceResource, len(rows))
	order := make([]string, 0, 16)
	for _, mr := range rows {
		sr, ok := serviceMap[mr.ServiceName]
		if !ok {
			sr = &ServiceResource{ServiceName: mr.ServiceName}
			serviceMap[mr.ServiceName] = sr
			order = append(order, mr.ServiceName)
		}
		v := mr.AvgValue
		switch mr.MetricName {
		case infraconsts.MetricSystemCPUUtilization:
			sr.AvgCpuUtil = &v
		case infraconsts.MetricSystemMemoryUtilization:
			sr.AvgMemoryUtil = &v
		case infraconsts.MetricSystemDiskUtilization:
			sr.AvgDiskUtil = &v
		case infraconsts.MetricSystemNetworkUtilization:
			sr.AvgNetworkUtil = &v
		case infraconsts.MetricDBConnectionPoolUtilization:
			sr.AvgConnectionPoolUtil = &v
		}
	}

	result := make([]ServiceResource, 0, len(order))
	for _, name := range order {
		result = append(result, *serviceMap[name])
	}
	return result, nil
}

func (r *compositeRepository) GetResourceUsageByInstance(ctx context.Context, teamID int64, startMs, endMs int64) ([]InstanceResource, error) {
	cpuList, err := r.cpu.GetCPUByInstance(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	connList, _ := r.conn.GetConnPoolByInstance(ctx, teamID, startMs, endMs)

	type instKey struct{ Host, Pod, Container, Service string }

	instanceMap := make(map[instKey]*InstanceResource, len(cpuList))
	order := make([]instKey, 0, len(cpuList))
	for _, inst := range cpuList {
		k := instKey{inst.Host, inst.Pod, inst.Container, inst.ServiceName}
		instanceMap[k] = &InstanceResource{
			Host: inst.Host, Pod: inst.Pod, Container: inst.Container,
			ServiceName: inst.ServiceName, AvgCpuUtil: inst.Value,
		}
		order = append(order, k)
	}

	for _, inst := range connList {
		k := instKey{inst.Host, inst.Pod, inst.Container, inst.ServiceName}
		if ir, ok := instanceMap[k]; ok {
			ir.AvgConnectionPoolUtil = inst.Value
		} else {
			instanceMap[k] = &InstanceResource{
				Host: inst.Host, Pod: inst.Pod, Container: inst.Container,
				ServiceName: inst.ServiceName, AvgConnectionPoolUtil: inst.Value,
			}
			order = append(order, k)
		}
	}

	// For memory/disk/network, query each instance individually.
	for k, ir := range instanceMap {
		memVal, _ := r.mem.GetMemoryByInstance(ctx, teamID, k.Host, k.Pod, k.Container, k.Service, startMs, endMs)
		ir.AvgMemoryUtil = memVal

		diskVal, _ := r.dsk.GetDiskByInstance(ctx, teamID, k.Host, k.Pod, k.Container, k.Service, startMs, endMs)
		ir.AvgDiskUtil = diskVal

		netVal, _ := r.net.GetNetworkByInstance(ctx, teamID, k.Host, k.Pod, k.Container, k.Service, startMs, endMs)
		ir.AvgNetworkUtil = netVal
	}

	result := make([]InstanceResource, 0, len(instanceMap))
	seen := make(map[instKey]bool, len(order))
	for _, k := range order {
		if seen[k] {
			continue
		}
		seen[k] = true
		result = append(result, *instanceMap[k])
	}
	return result, nil
}
