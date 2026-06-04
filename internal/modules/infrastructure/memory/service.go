package memory

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

func (s *Service) GetMemoryByInstance(ctx context.Context, teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
	_ = container
	rows, err := s.repo.QueryMemoryUtilizationForInstance(ctx, teamID, startMs, endMs, host, pod, serviceName)
	if err != nil {
		return nil, err
	}
	return foldMemoryMetricRows(rows), nil
}

<<<<<<< HEAD

=======
>>>>>>> f512576e76eb5e661aabd2a3202a40891770b326
// ---------------------------------------------------------------------------
// Folds + normalization.
// ---------------------------------------------------------------------------

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
