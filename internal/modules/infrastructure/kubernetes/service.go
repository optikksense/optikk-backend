package kubernetes

import (
	"cmp"
	"context"
	"math"
	"slices"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
)

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) GetContainerCPU(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]ContainerBucket, error) {
	rows, err := s.repo.QueryContainerCounter(ctx, teamID, startMs, endMs, MetricContainerCPUTime, node)
	if err != nil {
		return nil, err
	}
	return foldContainerCounter(rows, startMs, endMs), nil
}

func (s *Service) GetCPUThrottling(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]ContainerBucket, error) {
	rows, err := s.repo.QueryContainerCounter(ctx, teamID, startMs, endMs, MetricContainerCPUThrottledTime, node)
	if err != nil {
		return nil, err
	}
	return foldContainerCounter(rows, startMs, endMs), nil
}

func (s *Service) GetContainerMemory(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]ContainerBucket, error) {
	rows, err := s.repo.QueryContainerGauge(ctx, teamID, startMs, endMs, MetricContainerMemoryUsage, node)
	if err != nil {
		return nil, err
	}
	return foldContainerGauge(rows, startMs, endMs), nil
}

func (s *Service) GetOOMKills(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]ContainerBucket, error) {
	rows, err := s.repo.QueryContainerCounter(ctx, teamID, startMs, endMs, MetricContainerOOMKillCount, node)
	if err != nil {
		return nil, err
	}
	return foldContainerCounter(rows, startMs, endMs), nil
}

func (s *Service) GetPodRestarts(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]PodStat, error) {
	rows, err := s.repo.QueryPodRestarts(ctx, teamID, startMs, endMs, node)
	if err != nil {
		return nil, err
	}
	out := make([]PodStat, len(rows))
	for i, r := range rows {
		out[i] = PodStat{PodName: r.Pod, Namespace: r.Namespace, Restarts: r.Restarts}
	}
	return out, nil
}

func (s *Service) GetNodeAllocatable(ctx context.Context, teamID int64, startMs, endMs int64, node string) (NodeAllocatable, error) {
	rows, err := s.repo.QueryNodeAllocatable(ctx, teamID, startMs, endMs, node)
	if err != nil {
		return NodeAllocatable{}, err
	}
	var result NodeAllocatable
	for _, r := range rows {
		if math.IsNaN(r.Value) || math.IsInf(r.Value, 0) {
			continue
		}
		switch r.MetricName {
		case MetricK8sNodeAllocatableCPU:
			result.CPUCores = r.Value
		case MetricK8sNodeAllocatableMemory:
			result.MemoryBytes = r.Value
		}
	}
	return result, nil
}

func (s *Service) GetPodPhases(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]PhaseStat, error) {
	rows, err := s.repo.QueryPodPhases(ctx, teamID, startMs, endMs, node)
	if err != nil {
		return nil, err
	}
	out := make([]PhaseStat, len(rows))
	for i, r := range rows {
		out[i] = PhaseStat{Phase: r.Phase, Count: r.Count}
	}
	return out, nil
}

func (s *Service) GetReplicaStatus(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]ReplicaStat, error) {
	rows, err := s.repo.QueryReplicaStatus(ctx, teamID, startMs, endMs, node)
	if err != nil {
		return nil, err
	}
	byRS := map[string]*ReplicaStat{}
	order := []string{}
	for _, r := range rows {
		rs, ok := byRS[r.ReplicaSet]
		if !ok {
			rs = &ReplicaStat{ReplicaSet: r.ReplicaSet}
			byRS[r.ReplicaSet] = rs
			order = append(order, r.ReplicaSet)
		}
		switch r.MetricName {
		case MetricK8sReplicaSetDesired:
			rs.Desired = int64(r.Value)
		case MetricK8sReplicaSetAvailable:
			rs.Available = int64(r.Value)
		}
	}
	out := make([]ReplicaStat, 0, len(order))
	for _, name := range order {
		out = append(out, *byRS[name])
	}
	return out, nil
}

func (s *Service) GetVolumeUsage(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]VolumeStat, error) {
	rows, err := s.repo.QueryVolumeUsage(ctx, teamID, startMs, endMs, node)
	if err != nil {
		return nil, err
	}
	byVol := map[string]*VolumeStat{}
	order := []string{}
	for _, r := range rows {
		v, ok := byVol[r.VolumeName]
		if !ok {
			v = &VolumeStat{VolumeName: r.VolumeName}
			byVol[r.VolumeName] = v
			order = append(order, r.VolumeName)
		}
		switch r.MetricName {
		case MetricK8sVolumeCapacity:
			v.CapacityBytes = r.Value
		case MetricK8sVolumeInodes:
			v.Inodes = int64(r.Value)
		}
	}
	out := make([]VolumeStat, 0, len(order))
	for _, name := range order {
		out = append(out, *byVol[name])
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Folds.
// ---------------------------------------------------------------------------

func foldContainerCounter(rows []ContainerRow, startMs, endMs int64) []ContainerBucket {
	type key struct {
		ts        time.Time
		container string
	}
	sums := map[key]float64{}
	windowMs := endMs - startMs
	for _, r := range rows {
		k := key{ts: timebucket.DisplayBucket(r.Timestamp.Unix(), windowMs), container: r.Container}
		sums[k] += r.Value
	}
	out := make([]ContainerBucket, 0, len(sums))
	for k, sum := range sums {
		v := sum
		out = append(out, ContainerBucket{Timestamp: formatTime(k.ts), Container: k.container, Value: &v})
	}
	slices.SortFunc(out, sortContainerBucket)
	return out
}

func foldContainerGauge(rows []ContainerRow, startMs, endMs int64) []ContainerBucket {
	type key struct {
		ts        time.Time
		container string
	}
	type acc struct{ sum, count float64 }
	agg := map[key]*acc{}
	windowMs := endMs - startMs
	for _, r := range rows {
		k := key{ts: timebucket.DisplayBucket(r.Timestamp.Unix(), windowMs), container: r.Container}
		x, ok := agg[k]
		if !ok {
			x = &acc{}
			agg[k] = x
		}
		x.sum += r.Value
		x.count++
	}
	out := make([]ContainerBucket, 0, len(agg))
	for k, x := range agg {
		var vp *float64
		if x.count > 0 {
			v := x.sum / x.count
			vp = &v
		}
		out = append(out, ContainerBucket{Timestamp: formatTime(k.ts), Container: k.container, Value: vp})
	}
	slices.SortFunc(out, sortContainerBucket)
	return out
}

func sortContainerBucket(a, b ContainerBucket) int {
	if c := cmp.Compare(a.Timestamp, b.Timestamp); c != 0 {
		return c
	}
	return cmp.Compare(a.Container, b.Container)
}

func formatTime(t time.Time) string {
	return t.UTC().Format("2006-01-02 15:04:05")
}
