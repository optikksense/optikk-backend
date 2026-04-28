package memory

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

func (s *Service) GetMemoryUsage(ctx context.Context, teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	rows, err := s.repo.QueryMemoryUsageByState(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	return foldStateCounter(rows, startMs, endMs), nil
}

func (s *Service) GetSwapUsage(ctx context.Context, teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	rows, err := s.repo.QuerySwapUsageByState(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	return foldStateGauge(rows, startMs, endMs), nil
}

func (s *Service) GetMemoryUsagePercentage(ctx context.Context, teamID int64, startMs, endMs int64) ([]ResourceBucket, error) {
	rows, err := s.repo.QueryMemoryUsageByPod(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	type key struct {
		t   time.Time
		pod string
	}
	folded := map[key][]MemoryMetricNameRow{}
	windowMs := endMs - startMs
	for _, r := range rows {
		k := key{t: timebucket.DisplayBucket(r.Timestamp.Unix(), windowMs), pod: r.Pod}
		folded[k] = append(folded[k], MemoryMetricNameRow{MetricName: r.MetricName, Value: r.Value})
	}
	out := make([]ResourceBucket, 0, len(folded))
	for k, group := range folded {
		out = append(out, ResourceBucket{
			Timestamp: formatTime(k.t),
			Pod:       k.pod,
			Value:     foldMemoryMetricRows(group),
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

func (s *Service) GetAvgMemory(ctx context.Context, teamID int64, startMs, endMs int64) (MetricValue, error) {
	rows, err := s.repo.QueryMemoryUtilizationAgg(ctx, teamID, startMs, endMs)
	if err != nil {
		return MetricValue{}, err
	}
	avg := foldMemoryMetricRows(rows)
	if avg == nil {
		return MetricValue{Value: 0}, nil
	}
	return MetricValue{Value: *avg}, nil
}

func (s *Service) GetMemoryByService(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (*float64, error) {
	rows, err := s.repo.QueryMemoryUtilizationForService(ctx, teamID, startMs, endMs, serviceName)
	if err != nil {
		return nil, err
	}
	return foldMemoryMetricRows(rows), nil
}

func (s *Service) GetMemoryByInstance(ctx context.Context, teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
	_ = container
	rows, err := s.repo.QueryMemoryUtilizationForInstance(ctx, teamID, startMs, endMs, host, pod, serviceName)
	if err != nil {
		return nil, err
	}
	return foldMemoryMetricRows(rows), nil
}

// ---------------------------------------------------------------------------
// Folds + normalization.
// ---------------------------------------------------------------------------

// foldStateCounter sums values per (display_bucket, state).
func foldStateCounter(rows []MemoryStateRow, startMs, endMs int64) []StateBucket {
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

func foldStateGauge(rows []MemoryStateRow, startMs, endMs int64) []StateBucket {
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

// foldMemoryMetricRows blends the 4-metric memory family into a single
// utilization percentage:
//   - system.memory.utilization: gauge fraction → ≤1.0 → *100
//   - jvm.memory.used / jvm.memory.max: ratio used/max * 100
//
// Only metrics present produce contributions; remaining are skipped (preserves
// the original calculateAverage-of-non-empty-slice behaviour).
func foldMemoryMetricRows(rows []MemoryMetricNameRow) *float64 {
	by := make(map[string]float64, len(rows))
	for _, r := range rows {
		by[r.MetricName] = r.Value
	}
	var values []float64
	if v, ok := by[infraconsts.MetricSystemMemoryUtilization]; ok {
		if !math.IsNaN(v) && !math.IsInf(v, 0) && v >= 0 {
			if v <= infraconsts.PercentageThreshold {
				v = v * infraconsts.PercentageMultiplier
			}
			values = append(values, v)
		}
	}
	if max := by[infraconsts.MetricJVMMemoryMax]; max > 0 {
		used := by[infraconsts.MetricJVMMemoryUsed]
		values = append(values, infraconsts.PercentageMultiplier*used/max)
	}
	return averageFloats(values)
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
