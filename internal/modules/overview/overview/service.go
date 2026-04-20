package overview

import (
	"context"
	"fmt"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/sketch"
	"golang.org/x/sync/errgroup"
)

type Service struct {
	repo     Repository
	sketchQ  *sketch.Querier
}

func NewService(repo Repository, sketchQ *sketch.Querier) *Service {
	return &Service{repo: repo, sketchQ: sketchQ}
}

// teamIDString converts the int64 tenant id to the string form used by all
// sketch keys.
func teamIDString(teamID int64) string { return fmt.Sprintf("%d", teamID) }

func (s *Service) GetRequestRate(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]RequestRatePoint, error) {
	rows, err := s.repo.GetRequestRate(ctx, teamID, startMs, endMs, serviceName)
	if err != nil {
		return nil, err
	}
	return mapRequestRateRows(rows), nil
}

func (s *Service) GetErrorRate(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]ErrorRatePoint, error) {
	rows, err := s.repo.GetErrorRate(ctx, teamID, startMs, endMs, serviceName)
	if err != nil {
		return nil, err
	}
	return mapErrorRateRows(rows), nil
}

// GetP95Latency reads per-minute SpanLatencyService sketches from Redis and
// emits a p95-per-(service, bucket) time series. All compute happens in Go;
// the repository's SQL path is a no-op placeholder.
func (s *Service) GetP95Latency(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]P95LatencyPoint, error) {
	if s.sketchQ == nil {
		return nil, nil
	}
	const stepSecs = 60 // 1-minute buckets match SpanLatencyService's native grain
	byDim, err := s.sketchQ.PercentilesTimeseries(ctx, sketch.SpanLatencyService, teamIDString(teamID), startMs, endMs, stepSecs, 0.95)
	if err != nil {
		return nil, err
	}
	rows := make([]p95LatencyRow, 0)
	for dim, points := range byDim {
		if serviceName != "" && dim != serviceName {
			continue
		}
		for _, p := range points {
			if len(p.Values) == 0 {
				continue
			}
			rows = append(rows, p95LatencyRow{
				Timestamp:   time.Unix(p.BucketTs, 0).UTC(),
				ServiceName: dim,
				P95:         p.Values[0],
			})
		}
	}
	return mapP95LatencyRows(rows), nil
}

func (s *Service) GetServices(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServiceMetric, error) {
	rows, err := s.repo.GetServices(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	if s.sketchQ != nil && len(rows) > 0 {
		pcts, _ := s.sketchQ.Percentiles(ctx, sketch.SpanLatencyService, teamIDString(teamID), startMs, endMs, 0.5, 0.95, 0.99)
		for i := range rows {
			dim := sketch.DimSpanService(rows[i].ServiceName)
			if v, ok := pcts[dim]; ok && len(v) == 3 {
				rows[i].P50Latency = v[0]
				rows[i].P95Latency = v[1]
				rows[i].P99Latency = v[2]
			}
		}
	}
	return mapServiceMetricRows(rows), nil
}

func (s *Service) GetTopEndpoints(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]EndpointMetric, error) {
	rows, err := s.repo.GetTopEndpoints(ctx, teamID, startMs, endMs, serviceName)
	if err != nil {
		return nil, err
	}
	if s.sketchQ != nil && len(rows) > 0 {
		pcts, _ := s.sketchQ.Percentiles(ctx, sketch.SpanLatencyEndpoint, teamIDString(teamID), startMs, endMs, 0.5, 0.95, 0.99)
		for i := range rows {
			dim := sketch.DimSpanEndpoint(rows[i].ServiceName, rows[i].OperationName, rows[i].EndpointName, rows[i].HTTPMethod)
			if v, ok := pcts[dim]; ok && len(v) == 3 {
				rows[i].P50Latency = v[0]
				rows[i].P95Latency = v[1]
				rows[i].P99Latency = v[2]
			}
		}
	}
	return mapEndpointMetricRows(rows), nil
}

func (s *Service) GetSummary(ctx context.Context, teamID int64, startMs, endMs int64) (GlobalSummary, error) {
	row, err := s.repo.GetSummary(ctx, teamID, startMs, endMs)
	if err != nil {
		return GlobalSummary{}, err
	}
	if s.sketchQ != nil {
		// Merge every SpanLatencyService dim for this tenant into a single
		// sketch and read the aggregate percentiles. PercentilesByDimPrefix
		// with an empty prefix = "match all dims".
		pcts, _ := s.sketchQ.PercentilesByDimPrefix(ctx, sketch.SpanLatencyService, teamIDString(teamID), startMs, endMs, []string{""}, 0.5, 0.95, 0.99)
		if v, ok := pcts[""]; ok && len(v) == 3 {
			row.P50Latency = v[0]
			row.P95Latency = v[1]
			row.P99Latency = v[2]
		}
	}
	return mapGlobalSummaryRow(row), nil
}

// GetBatchSummary runs the above-fold Summary-tab queries (global summary +
// per-service metrics) concurrently and returns them in one payload. Collapses
// 2 HTTP round-trips into 1; server-side fan-out via errgroup means response
// time is bounded by max(child) instead of sum(child). The timeseries queries
// stay separate so the frontend can defer them until charts scroll into view.
func (s *Service) GetBatchSummary(ctx context.Context, teamID int64, startMs, endMs int64) (BatchSummaryResponse, error) {
	g, gctx := errgroup.WithContext(ctx)

	var (
		summary  GlobalSummary
		services []ServiceMetric
	)

	g.Go(func() error {
		v, err := s.GetSummary(gctx, teamID, startMs, endMs)
		summary = v
		return err
	})
	g.Go(func() error {
		v, err := s.GetServices(gctx, teamID, startMs, endMs)
		services = v
		return err
	})

	if err := g.Wait(); err != nil {
		return BatchSummaryResponse{}, err
	}

	return BatchSummaryResponse{
		Summary:  summary,
		Services: services,
	}, nil
}
