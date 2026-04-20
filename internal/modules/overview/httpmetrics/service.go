package httpmetrics

import (
	"context"
	"fmt"
	"sort"
	"strconv"

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

// topNErrorRoutes is the number of rows the client expects back from the
// route/host error-rate endpoints. SQL no longer does the LIMIT because we
// pivot raw rows and aggregate in Go.
const topNErrorRoutes = 20

// isErrorStatus is the canonical "this sample is an error" predicate. It
// returns true when the span has the error flag set or when the numeric
// response status is 400 or higher; empty or non-numeric status codes do not
// count as errors.
func isErrorStatus(hasError bool, statusCode string) bool {
	if hasError {
		return true
	}
	if statusCode == "" {
		return false
	}
	code, err := strconv.Atoi(statusCode)
	if err != nil {
		return false
	}
	return code >= 400
}

// statusGroupFor maps a raw HTTP status code string to the bucket name used
// by the status-distribution widget. Any non-numeric / zero code collapses to
// "other", mirroring the CH behavior where a non-numeric status parses to 0.
func statusGroupFor(statusCode string) string {
	code, err := strconv.Atoi(statusCode)
	if err != nil {
		return "other"
	}
	switch {
	case code >= 200 && code <= 299:
		return "2xx"
	case code >= 300 && code <= 399:
		return "3xx"
	case code >= 400 && code <= 499:
		return "4xx"
	case code >= 500:
		return "5xx"
	default:
		return "other"
	}
}

func (s *HTTPMetricsService) GetRequestRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]StatusCodeBucket, error) {
	rows, err := s.repo.GetRequestRate(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	out := make([]StatusCodeBucket, len(rows))
	for i, row := range rows {
		out[i] = StatusCodeBucket{
			Timestamp:  row.Timestamp,
			StatusCode: row.StatusCode,
			Count:      row.ReqCount,
		}
	}
	return out, nil
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
	rows, err := s.repo.GetTopRoutesByVolume(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	out := make([]RouteMetric, len(rows))
	for i, row := range rows {
		out[i] = RouteMetric{Route: row.Route, ReqCount: row.ReqCount}
	}
	return out, nil
}

func (s *HTTPMetricsService) GetTopRoutesByLatency(ctx context.Context, teamID int64, startMs, endMs int64) ([]RouteMetric, error) {
	rows, err := s.repo.GetTopRoutesByLatency(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	out := make([]RouteMetric, len(rows))
	for i, row := range rows {
		out[i] = RouteMetric{Route: row.Route, ReqCount: row.ReqCount}
	}
	if s.sketchQ != nil && len(out) > 0 {
		routes := make([]string, len(out))
		for i, row := range out {
			routes[i] = row.Route
		}
		pcts, _ := s.sketchQ.PercentilesByDimSegment(ctx, sketch.SpanLatencyEndpoint, teamIDString(teamID), startMs, endMs, dimSpanEndpointRouteSegment, routes, 0.95)
		for i := range out {
			if v, ok := pcts[out[i].Route]; ok && len(v) == 1 {
				out[i].P95Ms = v[0]
			}
		}
	}
	return out, nil
}

func (s *HTTPMetricsService) GetRouteErrorRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]RouteMetric, error) {
	rows, err := s.repo.GetRouteErrorPivot(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	type agg struct {
		req  uint64
		errs uint64
	}
	byRoute := make(map[string]*agg, len(rows))
	for _, row := range rows {
		a, ok := byRoute[row.Route]
		if !ok {
			a = &agg{}
			byRoute[row.Route] = a
		}
		a.req += row.SampleCount
		if isErrorStatus(row.HasError, row.StatusCode) {
			a.errs += row.SampleCount
		}
	}
	out := make([]RouteMetric, 0, len(byRoute))
	for route, a := range byRoute {
		pct := 0.0
		if a.req > 0 {
			pct = float64(a.errs) * 100.0 / float64(a.req)
		}
		out = append(out, RouteMetric{Route: route, ReqCount: a.req, ErrorPct: pct})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ErrorPct != out[j].ErrorPct {
			return out[i].ErrorPct > out[j].ErrorPct
		}
		return out[i].Route < out[j].Route
	})
	if len(out) > topNErrorRoutes {
		out = out[:topNErrorRoutes]
	}
	return out, nil
}

func (s *HTTPMetricsService) GetRouteErrorTimeseries(ctx context.Context, teamID int64, startMs, endMs int64) ([]RouteTimeseriesPoint, error) {
	rows, err := s.repo.GetRouteErrorTimeseriesPivot(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	type key struct {
		ts    string
		route string
	}
	type agg struct {
		req  uint64
		errs uint64
	}
	byBucket := make(map[key]*agg, len(rows))
	for _, row := range rows {
		k := key{ts: row.Timestamp, route: row.Route}
		a, ok := byBucket[k]
		if !ok {
			a = &agg{}
			byBucket[k] = a
		}
		a.req += row.SampleCount
		if isErrorStatus(row.HasError, row.StatusCode) {
			a.errs += row.SampleCount
		}
	}
	out := make([]RouteTimeseriesPoint, 0, len(byBucket))
	for k, a := range byBucket {
		rate := 0.0
		if a.req > 0 {
			rate = float64(a.errs) * 100.0 / float64(a.req)
		}
		out = append(out, RouteTimeseriesPoint{
			Timestamp:  k.ts,
			HttpRoute:  k.route,
			ReqCount:   a.req,
			ErrorCount: a.errs,
			ErrorRate:  rate,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Timestamp != out[j].Timestamp {
			return out[i].Timestamp < out[j].Timestamp
		}
		return out[i].ErrorCount > out[j].ErrorCount
	})
	return out, nil
}

func (s *HTTPMetricsService) GetStatusDistribution(ctx context.Context, teamID int64, startMs, endMs int64) ([]StatusGroupBucket, error) {
	rows, err := s.repo.GetStatusCodeDistribution(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	byGroup := make(map[string]uint64, 5)
	for _, row := range rows {
		byGroup[statusGroupFor(row.StatusCode)] += row.Count
	}
	out := make([]StatusGroupBucket, 0, len(byGroup))
	for group, c := range byGroup {
		out = append(out, StatusGroupBucket{StatusGroup: group, Count: c})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StatusGroup < out[j].StatusGroup })
	return out, nil
}

func (s *HTTPMetricsService) GetErrorTimeseries(ctx context.Context, teamID int64, startMs, endMs int64) ([]ErrorTimeseriesPoint, error) {
	rows, err := s.repo.GetErrorTimeseriesPivot(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	type agg struct {
		req  uint64
		errs uint64
	}
	byBucket := make(map[string]*agg)
	for _, row := range rows {
		a, ok := byBucket[row.Timestamp]
		if !ok {
			a = &agg{}
			byBucket[row.Timestamp] = a
		}
		a.req += row.SampleCount
		if isErrorStatus(row.HasError, row.StatusCode) {
			a.errs += row.SampleCount
		}
	}
	out := make([]ErrorTimeseriesPoint, 0, len(byBucket))
	for ts, a := range byBucket {
		rate := 0.0
		if a.req > 0 {
			rate = float64(a.errs) * 100.0 / float64(a.req)
		}
		out = append(out, ErrorTimeseriesPoint{
			Timestamp:  ts,
			ReqCount:   a.req,
			ErrorCount: a.errs,
			ErrorRate:  rate,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Timestamp < out[j].Timestamp })
	return out, nil
}

func (s *HTTPMetricsService) GetTopExternalHosts(ctx context.Context, teamID int64, startMs, endMs int64) ([]ExternalHostMetric, error) {
	rows, err := s.repo.GetTopExternalHosts(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	out := make([]ExternalHostMetric, len(rows))
	for i, row := range rows {
		out[i] = ExternalHostMetric{Host: row.Host, ReqCount: row.ReqCount}
	}
	return out, nil
}

func (s *HTTPMetricsService) GetExternalHostLatency(ctx context.Context, teamID int64, startMs, endMs int64) ([]ExternalHostMetric, error) {
	rows, err := s.repo.GetExternalHostLatency(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	out := make([]ExternalHostMetric, len(rows))
	for i, row := range rows {
		out[i] = ExternalHostMetric{Host: row.Host, ReqCount: row.ReqCount}
	}
	if s.sketchQ != nil && len(out) > 0 {
		hosts := make([]string, len(out))
		for i, row := range out {
			hosts[i] = row.Host
		}
		pcts, _ := s.sketchQ.PercentilesByDimSegment(ctx, sketch.HttpClientDuration, teamIDString(teamID), startMs, endMs, dimHttpClientHostSegment, hosts, 0.95)
		for i := range out {
			if v, ok := pcts[out[i].Host]; ok && len(v) == 1 {
				out[i].P95Ms = v[0]
			}
		}
	}
	return out, nil
}

func (s *HTTPMetricsService) GetExternalHostErrorRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]ExternalHostMetric, error) {
	rows, err := s.repo.GetExternalHostErrorPivot(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	type agg struct {
		req  uint64
		errs uint64
	}
	byHost := make(map[string]*agg, len(rows))
	for _, row := range rows {
		a, ok := byHost[row.Host]
		if !ok {
			a = &agg{}
			byHost[row.Host] = a
		}
		a.req += row.SampleCount
		if isErrorStatus(row.HasError, row.StatusCode) {
			a.errs += row.SampleCount
		}
	}
	out := make([]ExternalHostMetric, 0, len(byHost))
	for host, a := range byHost {
		pct := 0.0
		if a.req > 0 {
			pct = float64(a.errs) * 100.0 / float64(a.req)
		}
		out = append(out, ExternalHostMetric{Host: host, ReqCount: a.req, ErrorPct: pct})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ErrorPct != out[j].ErrorPct {
			return out[i].ErrorPct > out[j].ErrorPct
		}
		return out[i].Host < out[j].Host
	})
	if len(out) > topNErrorRoutes {
		out = out[:topNErrorRoutes]
	}
	return out, nil
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
	return HistogramSummary{Avg: avg}
}

func safeAvg(sum float64, count uint64) float64 {
	if count == 0 {
		return 0
	}
	return sum / float64(count)
}
