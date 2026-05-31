package network

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

// GetAvgNetwork averages per-service `system.network.utilization` after the
// ≤1.0 → *100 normalization. Mirrors the original behaviour: average across
// services that have valid data in the window.
func (s *Service) GetAvgNetwork(ctx context.Context, teamID int64, startMs, endMs int64) (MetricValue, error) {
	rows, err := s.repo.QueryNetworkUtilizationByService(ctx, teamID, startMs, endMs)
	if err != nil {
		return MetricValue{Value: 0}, err
	}
	values := make([]float64, 0, len(rows))
	for _, r := range rows {
		if v := normalizeNetworkUtil(r.Value); v != nil && *v >= 0 {
			values = append(values, *v)
		}
	}
	avg := averageFloats(values)
	if avg == nil {
		return MetricValue{Value: 0}, nil
	}
	return MetricValue{Value: *avg}, nil
}

func (s *Service) GetNetworkByInstance(ctx context.Context, teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
	_ = container // not currently used as a CH filter; preserved in signature for caller symmetry.
	row, err := s.repo.QueryNetworkUtilizationForInstance(ctx, teamID, startMs, endMs, host, pod, serviceName)
	if err != nil {
		return nil, err
	}
	return normalizeNetworkUtil(row.Value), nil
}

// ---------------------------------------------------------------------------
// Normalization helpers.
// ---------------------------------------------------------------------------

// normalizeNetworkUtil applies the ≤1.0 → *100 percentage convention so that
// metrics emitted as fractions (0.42) and percentages (42) both render as
// the same dashboard value.
func normalizeNetworkUtil(v float64) *float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
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
