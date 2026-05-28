package disk

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

func (s *Service) GetAvgDisk(ctx context.Context, teamID int64, startMs, endMs int64) (MetricValue, error) {
	rows, err := s.repo.QueryDiskUtilizationAgg(ctx, teamID, startMs, endMs)
	if err != nil {
		return MetricValue{}, err
	}
	avg := foldDiskMetricRows(rows)
	if avg == nil {
		return MetricValue{Value: 0}, nil
	}
	return MetricValue{Value: *avg}, nil
}

func (s *Service) GetDiskByInstance(ctx context.Context, teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
	_ = container // not currently used as a CH filter; preserved in signature for caller symmetry.
	rows, err := s.repo.QueryDiskUtilizationForInstance(ctx, teamID, startMs, endMs, host, pod, serviceName)
	if err != nil {
		return nil, err
	}
	return foldDiskMetricRows(rows), nil
}

// ---------------------------------------------------------------------------
// Folds + normalization.
// ---------------------------------------------------------------------------

// foldDiskMetricRows blends the disk utilization family. Only metrics whose
// values lie in the percentage range [0, 100] are kept; raw byte counts from
// disk.free / disk.total are filtered out by the > PercentageThreshold*100
// rejection (preserves the original behaviour exactly).
func foldDiskMetricRows(rows []DiskMetricNameRow) *float64 {
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
	if v, ok := byMetric[infraconsts.MetricSystemDiskUtilization]; ok {
		add(v)
	}
	if v, ok := byMetric[infraconsts.MetricDiskFree]; ok {
		add(v)
	}
	if v, ok := byMetric[infraconsts.MetricDiskTotal]; ok {
		add(v)
	}
	return averageFloats(values)
}

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
