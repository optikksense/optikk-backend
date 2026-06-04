package cpu

import (
	"context"
	"math"

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

<<<<<<< HEAD

=======
>>>>>>> f512576e76eb5e661aabd2a3202a40891770b326
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
