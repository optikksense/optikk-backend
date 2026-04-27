package resourceutil //nolint:misspell

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
		"github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/connpool"
	"github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/cpu"
	"github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/disk"
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
	cpu  cpu.Repository
	mem  memory.Repository
	dsk  disk.Repository
	net  network.Repository
	conn connpool.Repository
}

// NewRepository creates a composite repository that delegates to domain submodules.
func NewRepository(
	chDB clickhouse.Conn,
	cpuRepo cpu.Repository,
	memRepo memory.Repository,
	dskRepo disk.Repository,
	netRepo network.Repository,
	connRepo connpool.Repository,
) Repository {
	return &compositeRepository{
		chDB: chDB,
		cpu:  cpuRepo,
		mem:  memRepo,
		dsk:  dskRepo,
		net:  netRepo,
		conn: connRepo,
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
	table := "observability.spans"
	query := fmt.Sprintf(`
		SELECT service                                                        AS service,
		       metric_name                                                    AS metric_name,
		       sum(value_avg_num) / nullIf(toFloat64(sum(sample_count)), 0) AS avg_value
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		GROUP BY service, metric_name`, table)

	type row struct {
		ServiceName string   `ch:"service"`
		MetricName  string   `ch:"metric_name"`
		AvgValue    *float64 `ch:"avg_value"`
	}
	var rows []row
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.chDB, "resourceutil.GetResourceUsageByService", &rows, query,
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	); err != nil {
		return nil, err
	}

	// Fold rows by service — same response shape as before.
	serviceMap := make(map[string]*ServiceResource, len(rows))
	order := make([]string, 0, 16)
	for _, r := range rows {
		sr, ok := serviceMap[r.ServiceName]
		if !ok {
			sr = &ServiceResource{ServiceName: r.ServiceName}
			serviceMap[r.ServiceName] = sr
			order = append(order, r.ServiceName)
		}
		switch r.MetricName {
		case "system.cpu.utilization":
			sr.AvgCpuUtil = r.AvgValue
		case "system.memory.utilization":
			sr.AvgMemoryUtil = r.AvgValue
		case "system.disk.utilization":
			sr.AvgDiskUtil = r.AvgValue
		case "system.network.io":
			sr.AvgNetworkUtil = r.AvgValue
		case "db.client.connections.usage":
			sr.AvgConnectionPoolUtil = r.AvgValue
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
