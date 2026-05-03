package cpu

import (
	"cmp"
	"context"
	"math"
	"slices"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
	"github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/infraconsts"
)

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

// GetCPUTime returns per-(display_bucket, state) totals from system.cpu.time.
func (s *Service) GetCPUTime(ctx context.Context, teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	rows, err := s.repo.QueryCPUTimeByState(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	return foldStateBuckets(rows, startMs, endMs), nil
}

// GetProcessCount returns per-(display_bucket, state) gauge averages for system.process.count.
func (s *Service) GetProcessCount(ctx context.Context, teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	rows, err := s.repo.QueryProcessCountByState(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	return foldStateGauge(rows, startMs, endMs), nil
}

// GetCPUUsagePercentage folds the 3-metric utilization family per (timestamp, pod).
func (s *Service) GetCPUUsagePercentage(ctx context.Context, teamID int64, startMs, endMs int64) ([]ResourceBucket, error) {
	rows, err := s.repo.QueryCPUUtilizationByPod(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	type key struct {
		t   time.Time
		pod string
	}
	folded := map[key][]float64{}
	windowMs := endMs - startMs
	for _, r := range rows {
		v := normalizeUtilization(r.Value)
		if v == nil {
			continue
		}
		k := key{t: timebucket.DisplayBucket(r.Timestamp.Unix(), windowMs), pod: r.Pod}
		folded[k] = append(folded[k], *v)
	}
	out := make([]ResourceBucket, 0, len(folded))
	for k, vals := range folded {
		out = append(out, ResourceBucket{
			Timestamp: formatTime(k.t),
			Pod:       k.pod,
			Value:     averageFloats(vals),
		})
	}
	slices.SortFunc(out, func(a, b ResourceBucket) int {
		if c := cmp.Compare(a.Timestamp, b.Timestamp); c != 0 {
			return c
		}
		return cmp.Compare(a.Pod, b.Pod)
	})
	return out, nil
}

// GetLoadAverage returns the 1m/5m/15m load averages aggregated across the window.
func (s *Service) GetLoadAverage(ctx context.Context, teamID int64, startMs, endMs int64) (LoadAverageResult, error) {
	rows, err := s.repo.QueryLoadAverages(ctx, teamID, startMs, endMs)
	if err != nil {
		return LoadAverageResult{}, err
	}
	var result LoadAverageResult
	for _, r := range rows {
		if math.IsNaN(r.Value) || math.IsInf(r.Value, 0) {
			continue
		}
		switch r.MetricName {
		case infraconsts.MetricSystemCPULoadAvg1m:
			result.Load1m = r.Value
		case infraconsts.MetricSystemCPULoadAvg5m:
			result.Load5m = r.Value
		case infraconsts.MetricSystemCPULoadAvg15m:
			result.Load15m = r.Value
		}
	}
	return result, nil
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

// GetCPUByService returns one row per service with the 3-metric fold applied per service.
func (s *Service) GetCPUByService(ctx context.Context, teamID int64, startMs, endMs int64) ([]CPUServiceMetric, error) {
	rows, err := s.repo.QueryCPUUtilizationByService(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	byService := map[string][]CPUMetricNameRow{}
	order := []string{}
	for _, r := range rows {
		if _, ok := byService[r.Service]; !ok {
			order = append(order, r.Service)
		}
		byService[r.Service] = append(byService[r.Service], CPUMetricNameRow{MetricName: r.MetricName, Value: r.Value})
	}
	out := make([]CPUServiceMetric, 0, len(order))
	for _, name := range order {
		out = append(out, CPUServiceMetric{ServiceName: name, Value: foldCPUMetricRows(byService[name])})
	}
	return out, nil
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

// ---------------------------------------------------------------------------
// Folds + normalization.
// ---------------------------------------------------------------------------

func foldStateBuckets(rows []CPUStateRow, startMs, endMs int64) []StateBucket {
	type key struct {
		ts    time.Time
		state string
	}
	sums := map[key]float64{}
	windowMs := endMs - startMs
	for _, r := range rows {
		k := key{ts: timebucket.DisplayBucket(r.Timestamp.Unix(), windowMs), state: r.State}
		sums[k] += r.Value
	}
	out := make([]StateBucket, 0, len(sums))
	for k, sum := range sums {
		v := sum
		out = append(out, StateBucket{Timestamp: formatTime(k.ts), State: k.state, Value: &v})
	}
	slices.SortFunc(out, sortStateBucket)
	return out
}

func foldStateGauge(rows []CPUStateRow, startMs, endMs int64) []StateBucket {
	type key struct {
		ts    time.Time
		state string
	}
	type acc struct{ sum, count float64 }
	agg := map[key]*acc{}
	windowMs := endMs - startMs
	for _, r := range rows {
		k := key{ts: timebucket.DisplayBucket(r.Timestamp.Unix(), windowMs), state: r.State}
		x, ok := agg[k]
		if !ok {
			x = &acc{}
			agg[k] = x
		}
		x.sum += r.Value
		x.count++
	}
	out := make([]StateBucket, 0, len(agg))
	for k, x := range agg {
		var vp *float64
		if x.count > 0 {
			v := x.sum / x.count
			vp = &v
		}
		out = append(out, StateBucket{Timestamp: formatTime(k.ts), State: k.state, Value: vp})
	}
	slices.SortFunc(out, sortStateBucket)
	return out
}

func sortStateBucket(a, b StateBucket) int {
	if c := cmp.Compare(a.Timestamp, b.Timestamp); c != 0 {
		return c
	}
	return cmp.Compare(a.State, b.State)
}

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

func formatTime(t time.Time) string {
	return t.UTC().Format("2006-01-02 15:04:05")
}
