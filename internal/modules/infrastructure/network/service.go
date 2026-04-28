package network

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

// GetNetworkIO returns per-direction byte totals folded into display buckets.
func (s *Service) GetNetworkIO(ctx context.Context, teamID int64, startMs, endMs int64) ([]DirectionBucket, error) {
	rows, err := s.repo.QueryNetworkCounterByDirection(ctx, teamID, startMs, endMs, infraconsts.MetricSystemNetworkIO)
	if err != nil {
		return nil, err
	}
	return foldDirectionBuckets(rows, startMs, endMs), nil
}

func (s *Service) GetNetworkPackets(ctx context.Context, teamID int64, startMs, endMs int64) ([]DirectionBucket, error) {
	rows, err := s.repo.QueryNetworkCounterByDirection(ctx, teamID, startMs, endMs, infraconsts.MetricSystemNetworkPackets)
	if err != nil {
		return nil, err
	}
	return foldDirectionBuckets(rows, startMs, endMs), nil
}

// GetNetworkErrors returns per-direction error counts. The DTO names the dim
// "state" (StateBucket) but the underlying OTel attribute is direction —
// they are the same concept, this is a JSON-contract quirk.
func (s *Service) GetNetworkErrors(ctx context.Context, teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	rows, err := s.repo.QueryNetworkCounterByDirection(ctx, teamID, startMs, endMs, infraconsts.MetricSystemNetworkErrors)
	if err != nil {
		return nil, err
	}
	dirs := foldDirectionBuckets(rows, startMs, endMs)
	out := make([]StateBucket, len(dirs))
	for i, d := range dirs {
		out[i] = StateBucket{Timestamp: d.Timestamp, State: d.Direction, Value: d.Value}
	}
	return out, nil
}

func (s *Service) GetNetworkDropped(ctx context.Context, teamID int64, startMs, endMs int64) ([]ResourceBucket, error) {
	rows, err := s.repo.QueryNetworkCounterTotal(ctx, teamID, startMs, endMs, infraconsts.MetricSystemNetworkDropped)
	if err != nil {
		return nil, err
	}
	type acc struct{ sum float64 }
	bucketSums := map[time.Time]*acc{}
	windowMs := endMs - startMs
	for _, r := range rows {
		k := timebucket.DisplayBucket(r.Timestamp.Unix(), windowMs)
		x, ok := bucketSums[k]
		if !ok {
			x = &acc{}
			bucketSums[k] = x
		}
		x.sum += r.Value
	}
	out := make([]ResourceBucket, 0, len(bucketSums))
	for ts, x := range bucketSums {
		v := x.sum
		out = append(out, ResourceBucket{Timestamp: formatTime(ts), Pod: "", Value: &v})
	}
	slices.SortFunc(out, func(a, b ResourceBucket) int { return cmp.Compare(a.Timestamp, b.Timestamp) })
	return out, nil
}

func (s *Service) GetNetworkConnections(ctx context.Context, teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	rows, err := s.repo.QueryNetworkGaugeByState(ctx, teamID, startMs, endMs, infraconsts.MetricSystemNetworkConnections)
	if err != nil {
		return nil, err
	}
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
	slices.SortFunc(out, func(a, b StateBucket) int {
		if c := cmp.Compare(a.Timestamp, b.Timestamp); c != 0 {
			return c
		}
		return cmp.Compare(a.State, b.State)
	})
	return out, nil
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

func (s *Service) GetNetworkByService(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (*float64, error) {
	row, err := s.repo.QueryNetworkUtilizationForService(ctx, teamID, startMs, endMs, serviceName)
	if err != nil {
		return nil, err
	}
	return normalizeNetworkUtil(row.Value), nil
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
// Folds + normalization helpers.
// ---------------------------------------------------------------------------

// foldDirectionBuckets sums values per (display_bucket, direction) for
// counter-style direction-keyed metrics.
func foldDirectionBuckets(rows []NetworkDirectionRow, startMs, endMs int64) []DirectionBucket {
	type key struct {
		ts        time.Time
		direction string
	}
	sums := map[key]float64{}
	windowMs := endMs - startMs
	for _, r := range rows {
		k := key{ts: timebucket.DisplayBucket(r.Timestamp.Unix(), windowMs), direction: r.Direction}
		sums[k] += r.Value
	}
	out := make([]DirectionBucket, 0, len(sums))
	for k, sum := range sums {
		v := sum
		out = append(out, DirectionBucket{Timestamp: formatTime(k.ts), Direction: k.direction, Value: &v})
	}
	slices.SortFunc(out, func(a, b DirectionBucket) int {
		if c := cmp.Compare(a.Timestamp, b.Timestamp); c != 0 {
			return c
		}
		return cmp.Compare(a.Direction, b.Direction)
	})
	return out
}

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

func formatTime(t time.Time) string {
	return t.UTC().Format("2006-01-02 15:04:05")
}
