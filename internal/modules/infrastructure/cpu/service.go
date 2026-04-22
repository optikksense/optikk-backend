package cpu

import (
	"context"
	"math"
	"sort"

	"github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/infraconsts"
)

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

// GetAvgCPU folds the 3-metric utilization family across the window.
func (s *Service) GetAvgCPU(ctx context.Context, teamID int64, startMs, endMs int64) (MetricValue, error) {
	rows, err := s.repo.QueryCPUUtilizationAgg(ctx, teamID, startMs, endMs)
	if err != nil {
		return MetricValue{}, err
	}
	avg := foldCPUMetricRows(rows)
	if avg == nil {
		return MetricValue{Value: 0}, nil
	}
	return MetricValue{Value: *avg}, nil
}

// GetCPUByInstance returns one row per (host, pod, container, service) with the 3-metric fold applied.
func (s *Service) GetCPUByInstance(ctx context.Context, teamID int64, startMs, endMs int64) ([]CPUInstanceMetric, error) {
	rows, err := s.repo.QueryCPUUtilizationByInstance(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	type instKey struct{ host, pod, container, service string }
	byInst := map[instKey][]CPUMetricNameRow{}
	order := []instKey{}
	for _, r := range rows {
		k := instKey{r.Host, r.Pod, r.Container, r.Service}
		if _, ok := byInst[k]; !ok {
			order = append(order, k)
		}
		byInst[k] = append(byInst[k], CPUMetricNameRow{MetricName: r.MetricName, Value: r.Value})
	}
	out := make([]CPUInstanceMetric, 0, len(order))
	for _, k := range order {
		out = append(out, CPUInstanceMetric{
			Host:        k.host,
			Pod:         k.pod,
			Container:   k.container,
			ServiceName: k.service,
			Value:       foldCPUMetricRows(byInst[k]),
		})
	}
	return out, nil
}

// GetCPUTopHosts returns the top-N hosts by blended CPU utilization, ranked
// DESC. The 3-metric blend is Go-side, so ranking happens after the fold.
func (s *Service) GetCPUTopHosts(ctx context.Context, teamID int64, startMs, endMs int64, limit int) ([]HostValue, error) {
	rows, err := s.repo.QueryCPUByHost(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	byHost := map[string][]CPUMetricNameRow{}
	order := []string{}
	for _, r := range rows {
		if _, ok := byHost[r.Host]; !ok {
			order = append(order, r.Host)
		}
		byHost[r.Host] = append(byHost[r.Host], CPUMetricNameRow{MetricName: r.MetricName, Value: r.Value})
	}
	out := make([]HostValue, 0, len(order))
	for _, host := range order {
		v := foldCPUMetricRows(byHost[host])
		if v == nil {
			continue
		}
		out = append(out, HostValue{Host: host, Value: *v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Value > out[j].Value })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Folds + normalization.
// ---------------------------------------------------------------------------

// foldCPUMetricRows blends the 3-metric utilization family into one percentage.
func foldCPUMetricRows(rows []CPUMetricNameRow) *float64 {
	byMetric := map[string]float64{}
	for _, r := range rows {
		byMetric[r.MetricName] = r.Value
	}
	var values []float64
	add := func(v float64) {
		if nv := normalizeUtilization(v); nv != nil {
			values = append(values, *nv)
		}
	}
	if v, ok := byMetric[infraconsts.MetricSystemCPUUtilization]; ok {
		add(v)
	}
	if v, ok := byMetric[infraconsts.MetricSystemCPUUsage]; ok {
		add(v)
	}
	if v, ok := byMetric[infraconsts.MetricProcessCPUUsage]; ok {
		add(v)
	}
	return averageFloats(values)
}

// normalizeUtilization applies the ≤1.0 → *100 percentage convention. Drops
// NaN/Inf, negative values, and values already above 100% (the original code
// rejects v > 1.0 * 100 = 100 as a sentinel for already-normalized garbage).
func normalizeUtilization(v float64) *float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 || v > infraconsts.PercentageThreshold*100 {
		return nil
	}
	if v <= infraconsts.PercentageThreshold {
		v = v * infraconsts.PercentageMultiplier
	}
	return &v
}

func averageFloats(values []float64) *float64 {
	var sum float64
	count := 0
	for _, v := range values {
		if !math.IsNaN(v) && !math.IsInf(v, 0) && v >= 0 {
			sum += v
			count++
		}
	}
	if count == 0 {
		return nil
	}
	avg := sum / float64(count)
	return &avg
}
