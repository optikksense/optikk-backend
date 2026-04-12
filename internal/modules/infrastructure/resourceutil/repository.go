package resourceutil //nolint:misspell

import (
	"context"

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
	cpu  cpu.Repository
	mem  memory.Repository
	dsk  disk.Repository
	net  network.Repository
	conn connpool.Repository
}

// NewRepository creates a composite repository that delegates to domain submodules.
func NewRepository(
	cpuRepo cpu.Repository,
	memRepo memory.Repository,
	dskRepo disk.Repository,
	netRepo network.Repository,
	connRepo connpool.Repository,
) Repository {
	return &compositeRepository{
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
	// CPU and connpool return full lists; memory/disk/network are per-entity.
	cpuList, err := r.cpu.GetCPUByService(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	connList, _ := r.conn.GetConnPoolByService(ctx, teamID, startMs, endMs)

	// Build result map seeded from CPU (broadest metric set).
	serviceMap := make(map[string]*ServiceResource, len(cpuList))
	order := make([]string, 0, len(cpuList))
	for _, s := range cpuList {
		serviceMap[s.ServiceName] = &ServiceResource{ServiceName: s.ServiceName, AvgCpuUtil: s.Value}
		order = append(order, s.ServiceName)
	}

	// Merge connpool list results.
	for _, s := range connList {
		if sr, ok := serviceMap[s.ServiceName]; ok {
			sr.AvgConnectionPoolUtil = s.Value
		} else {
			serviceMap[s.ServiceName] = &ServiceResource{ServiceName: s.ServiceName, AvgConnectionPoolUtil: s.Value}
			order = append(order, s.ServiceName)
		}
	}

	// For memory/disk/network, query each service individually.
	for name, sr := range serviceMap {
		memVal, _ := r.mem.GetMemoryByService(ctx, teamID, name, startMs, endMs)
		sr.AvgMemoryUtil = memVal

		diskVal, _ := r.dsk.GetDiskByService(ctx, teamID, name, startMs, endMs)
		sr.AvgDiskUtil = diskVal

		netVal, _ := r.net.GetNetworkByService(ctx, teamID, name, startMs, endMs)
		sr.AvgNetworkUtil = netVal
	}

	// Preserve insertion order, deduplicate.
	result := make([]ServiceResource, 0, len(serviceMap))
	seen := make(map[string]bool, len(order))
	for _, name := range order {
		if seen[name] {
			continue
		}
		seen[name] = true
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
