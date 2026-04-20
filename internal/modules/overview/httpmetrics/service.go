package httpmetrics

import (
	"context"
	"fmt"

	"github.com/Optikk-Org/optikk-backend/internal/infra/sketch"
)

type Service interface {
	GetRequestRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]StatusCodeBucket, error)
	GetRequestDuration(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error)
	GetActiveRequests(ctx context.Context, teamID int64, startMs, endMs int64) ([]TimeBucket, error)
	GetRequestBodySize(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error)
	GetResponseBodySize(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error)
	GetClientDuration(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error)
	GetDNSDuration(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error)
	GetTLSDuration(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error)
	GetTopRoutesByVolume(ctx context.Context, teamID int64, startMs, endMs int64) ([]RouteMetric, error)
	GetTopRoutesByLatency(ctx context.Context, teamID int64, startMs, endMs int64) ([]RouteMetric, error)
	GetRouteErrorRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]RouteMetric, error)
	GetRouteErrorTimeseries(ctx context.Context, teamID int64, startMs, endMs int64) ([]RouteTimeseriesPoint, error)
	GetStatusDistribution(ctx context.Context, teamID int64, startMs, endMs int64) ([]StatusGroupBucket, error)
	GetErrorTimeseries(ctx context.Context, teamID int64, startMs, endMs int64) ([]ErrorTimeseriesPoint, error)
	GetTopExternalHosts(ctx context.Context, teamID int64, startMs, endMs int64) ([]ExternalHostMetric, error)
	GetExternalHostLatency(ctx context.Context, teamID int64, startMs, endMs int64) ([]ExternalHostMetric, error)
	GetExternalHostErrorRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]ExternalHostMetric, error)
}

type HTTPMetricsService struct {
	repo    Repository
	sketchQ *sketch.Querier
}

func NewService(repo Repository, sketchQ *sketch.Querier) Service {
	return &HTTPMetricsService{repo: repo, sketchQ: sketchQ}
}

// teamIDString converts the int64 tenant id to the string form used by all
// sketch keys.
func teamIDString(teamID int64) string { return fmt.Sprintf("%d", teamID) }

// dimSpanEndpointRouteSegment is the zero-indexed segment of the
// SpanLatencyEndpoint dim that holds the route/endpoint (service|operation|
// *endpoint*|method).
const dimSpanEndpointRouteSegment = 2

// dimHttpClientHostSegment is the zero-indexed segment of the
// HttpClientDuration dim that holds the host/target
// (scope|*hostTarget*|method|statusCode).
const dimHttpClientHostSegment = 1

func (s *HTTPMetricsService) GetRequestRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]StatusCodeBucket, error) {
	return s.repo.GetRequestRate(ctx, teamID, startMs, endMs)
}

func (s *HTTPMetricsService) GetRequestDuration(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	row, err := s.repo.GetRequestDuration(ctx, teamID, startMs, endMs)
	if err != nil {
		return HistogramSummary{}, err
	}
	summary := summaryFromRow(row)
	s.fillSummaryFromKind(ctx, &summary, sketch.HttpServerDuration, teamID, startMs, endMs)
	return summary, nil
}

func (s *HTTPMetricsService) GetActiveRequests(ctx context.Context, teamID int64, startMs, endMs int64) ([]TimeBucket, error) {
	rows, err := s.repo.GetActiveRequests(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	out := make([]TimeBucket, len(rows))
	for i, row := range rows {
		v := safeAvg(row.ValSum, row.ValCount)
		out[i] = TimeBucket{
			Timestamp: row.Timestamp,
			Value:     &v,
		}
	}
	return out, nil
}

func (s *HTTPMetricsService) GetRequestBodySize(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	row, err := s.repo.GetRequestBodySize(ctx, teamID, startMs, endMs)
	if err != nil {
		return HistogramSummary{}, err
	}
	// No dedicated sketch kind for request/response body sizes — percentiles
	// stay at the zero placeholder until one is wired on ingest.
	return summaryFromRow(row), nil
}

func (s *HTTPMetricsService) GetResponseBodySize(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	row, err := s.repo.GetResponseBodySize(ctx, teamID, startMs, endMs)
	if err != nil {
		return HistogramSummary{}, err
	}
	return summaryFromRow(row), nil
}

func (s *HTTPMetricsService) GetClientDuration(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	row, err := s.repo.GetClientDuration(ctx, teamID, startMs, endMs)
	if err != nil {
		return HistogramSummary{}, err
	}
	summary := summaryFromRow(row)
	s.fillSummaryFromKind(ctx, &summary, sketch.HttpClientDuration, teamID, startMs, endMs)
	return summary, nil
}

func (s *HTTPMetricsService) GetDNSDuration(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	row, err := s.repo.GetDNSDuration(ctx, teamID, startMs, endMs)
	if err != nil {
		return HistogramSummary{}, err
	}
	// No sketch kind for dns.lookup.duration / tls.connect.duration.
	return summaryFromRow(row), nil
}

func (s *HTTPMetricsService) GetTLSDuration(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	row, err := s.repo.GetTLSDuration(ctx, teamID, startMs, endMs)
	if err != nil {
		return HistogramSummary{}, err
	}
	return summaryFromRow(row), nil
}

func (s *HTTPMetricsService) GetTopRoutesByVolume(ctx context.Context, teamID int64, startMs, endMs int64) ([]RouteMetric, error) {
	return s.repo.GetTopRoutesByVolume(ctx, teamID, startMs, endMs)
}

func (s *HTTPMetricsService) GetTopRoutesByLatency(ctx context.Context, teamID int64, startMs, endMs int64) ([]RouteMetric, error) {
	rows, err := s.repo.GetTopRoutesByLatency(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	if s.sketchQ != nil && len(rows) > 0 {
		routes := make([]string, len(rows))
		for i, row := range rows {
			routes[i] = row.Route
		}
		pcts, _ := s.sketchQ.PercentilesByDimSegment(ctx, sketch.SpanLatencyEndpoint, teamIDString(teamID), startMs, endMs, dimSpanEndpointRouteSegment, routes, 0.95)
		for i := range rows {
			if v, ok := pcts[rows[i].Route]; ok && len(v) == 1 {
				rows[i].P95Ms = v[0]
			}
		}
	}
	return rows, nil
}

func (s *HTTPMetricsService) GetRouteErrorRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]RouteMetric, error) {
	return s.repo.GetRouteErrorRate(ctx, teamID, startMs, endMs)
}

func (s *HTTPMetricsService) GetRouteErrorTimeseries(ctx context.Context, teamID int64, startMs, endMs int64) ([]RouteTimeseriesPoint, error) {
	return s.repo.GetRouteErrorTimeseries(ctx, teamID, startMs, endMs)
}

func (s *HTTPMetricsService) GetStatusDistribution(ctx context.Context, teamID int64, startMs, endMs int64) ([]StatusGroupBucket, error) {
	return s.repo.GetStatusDistribution(ctx, teamID, startMs, endMs)
}

func (s *HTTPMetricsService) GetErrorTimeseries(ctx context.Context, teamID int64, startMs, endMs int64) ([]ErrorTimeseriesPoint, error) {
	return s.repo.GetErrorTimeseries(ctx, teamID, startMs, endMs)
}

func (s *HTTPMetricsService) GetTopExternalHosts(ctx context.Context, teamID int64, startMs, endMs int64) ([]ExternalHostMetric, error) {
	return s.repo.GetTopExternalHosts(ctx, teamID, startMs, endMs)
}

func (s *HTTPMetricsService) GetExternalHostLatency(ctx context.Context, teamID int64, startMs, endMs int64) ([]ExternalHostMetric, error) {
	rows, err := s.repo.GetExternalHostLatency(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	if s.sketchQ != nil && len(rows) > 0 {
		hosts := make([]string, len(rows))
		for i, row := range rows {
			hosts[i] = row.Host
		}
		pcts, _ := s.sketchQ.PercentilesByDimSegment(ctx, sketch.HttpClientDuration, teamIDString(teamID), startMs, endMs, dimHttpClientHostSegment, hosts, 0.95)
		for i := range rows {
			if v, ok := pcts[rows[i].Host]; ok && len(v) == 1 {
				rows[i].P95Ms = v[0]
			}
		}
	}
	return rows, nil
}

func (s *HTTPMetricsService) GetExternalHostErrorRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]ExternalHostMetric, error) {
	return s.repo.GetExternalHostErrorRate(ctx, teamID, startMs, endMs)
}

// fillSummaryFromKind merges every dim for the given kind and writes the
// aggregate percentiles onto the summary. A single tenant-wide roll-up is the
// right shape for /http/request-duration et al.
func (s *HTTPMetricsService) fillSummaryFromKind(ctx context.Context, summary *HistogramSummary, kind sketch.Kind, teamID int64, startMs, endMs int64) {
	if s.sketchQ == nil {
		return
	}
	pcts, _ := s.sketchQ.PercentilesByDimPrefix(ctx, kind, teamIDString(teamID), startMs, endMs, []string{""}, 0.5, 0.95, 0.99)
	if v, ok := pcts[""]; ok && len(v) == 3 {
		summary.P50 = v[0]
		summary.P95 = v[1]
		summary.P99 = v[2]
	}
}

func summaryFromRow(row histogramSummaryRow) HistogramSummary {
	avg := 0.0
	if row.HistCount > 0 {
		avg = row.HistSum / float64(row.HistCount)
	}
	return HistogramSummary{
		P50: row.P50,
		P95: row.P95,
		P99: row.P99,
		Avg: avg,
	}
}

func safeAvg(sum float64, count int64) float64 {
	if count <= 0 {
		return 0
	}
	return sum / float64(count)
}
