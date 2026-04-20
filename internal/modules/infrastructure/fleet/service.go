package fleet

import (
	"context"
	"fmt"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/sketch"
)

type Service interface {
	GetFleetPods(ctx context.Context, teamID int64, startMs, endMs int64) ([]FleetPod, error)
}

type fleetService struct {
	repo    Repository
	sketchQ *sketch.Querier
}

func NewService(repo Repository, sketchQ *sketch.Querier) Service {
	return &fleetService{repo: repo, sketchQ: sketchQ}
}

// teamIDString converts the int64 tenant id to the string form used by all
// sketch keys.
func teamIDString(teamID int64) string { return fmt.Sprintf("%d", teamID) }

func (s *fleetService) GetFleetPods(ctx context.Context, teamID int64, startMs, endMs int64) ([]FleetPod, error) {
	rows, err := s.repo.GetFleetPods(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}

	// P95 per-pod: no per-pod sketch kind exists. Merge SpanLatencyService by
	// the services attached to each pod using DimSpanService as the prefix set
	// (empty tail means "exact service dim"). Sum-then-merge via DDSketch is
	// handled implicitly by PercentilesByDimPrefix — matching dims are folded
	// into a single sketch before quantile compute.
	var perService map[string][]float64
	if s.sketchQ != nil && len(rows) > 0 {
		uniq := map[string]struct{}{}
		for _, r := range rows {
			for _, svc := range r.Services {
				if svc != "" {
					uniq[sketch.DimSpanService(svc)] = struct{}{}
				}
			}
		}
		if len(uniq) > 0 {
			prefixes := make([]string, 0, len(uniq))
			for p := range uniq {
				prefixes = append(prefixes, p)
			}
			perService, _ = s.sketchQ.PercentilesByDimPrefix(ctx, sketch.SpanLatencyService, teamIDString(teamID), startMs, endMs, prefixes, 0.95)
		}
	}

	out := make([]FleetPod, len(rows))
	for i, d := range rows {
		avgLatency := 0.0
		if d.LatencyMsCount > 0 {
			avgLatency = d.LatencyMsSum / float64(d.LatencyMsCount)
		}
		p95 := d.P95Latency
		if perService != nil {
			// Pick the max p95 across the services present on this pod.
			// Merging DDSketches across services would require a second pass;
			// the max is a safe representative for a pod-scoped p95 view and
			// matches the intent of surfacing the "slowest service on this pod".
			best := 0.0
			for _, svc := range d.Services {
				if svc == "" {
					continue
				}
				v, ok := perService[sketch.DimSpanService(svc)]
				if !ok || len(v) == 0 {
					continue
				}
				if v[0] > best {
					best = v[0]
				}
			}
			if best > 0 {
				p95 = best
			}
		}
		out[i] = FleetPod{
			PodName:      d.PodName,
			Host:         d.HostName,
			Services:     d.Services,
			RequestCount: d.RequestCount,
			ErrorCount:   d.ErrorCount,
			ErrorRate:    d.ErrorRate,
			AvgLatencyMs: avgLatency,
			P95LatencyMs: p95,
			LastSeen:     d.LastSeen.Format(time.RFC3339),
		}
	}
	return out, nil
}
