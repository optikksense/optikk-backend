package httpmetrics

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
)

const topNLimit = 20

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
	repo Repository
}

func NewService(repo Repository) Service {
	return &HTTPMetricsService{repo: repo}
}

func (s *HTTPMetricsService) GetRequestRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]StatusCodeBucket, error) {
	rows, err := s.repo.QueryServerRequestStatusSeries(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	out := make([]StatusCodeBucket, len(rows))
	for i, r := range rows {
		out[i] = StatusCodeBucket{
			Timestamp:  formatBucket(r.Timestamp),
			StatusCode: fmt.Sprintf("%d", r.StatusCode),
			Count:      int64(r.Count), //nolint:gosec
		}
	}
	return out, nil
}

func (s *HTTPMetricsService) GetRequestDuration(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	row, err := s.repo.QueryServerRequestDurationHistogram(ctx, teamID, startMs, endMs)
	if err != nil {
		return HistogramSummary{}, err
	}
	return histogramSummary(row), nil
}

func (s *HTTPMetricsService) GetActiveRequests(ctx context.Context, teamID int64, startMs, endMs int64) ([]TimeBucket, error) {
	rows, err := s.repo.QueryServerActiveRequestsSeries(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	out := make([]TimeBucket, len(rows))
	for i, r := range rows {
		v := r.Value
		out[i] = TimeBucket{Timestamp: formatBucket(r.Timestamp), Value: &v}
	}
	return out, nil
}

func (s *HTTPMetricsService) GetRequestBodySize(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	row, err := s.repo.QueryServerRequestBodySizeHistogram(ctx, teamID, startMs, endMs)
	if err != nil {
		return HistogramSummary{}, err
	}
	return histogramSummary(row), nil
}

func (s *HTTPMetricsService) GetResponseBodySize(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	row, err := s.repo.QueryServerResponseBodySizeHistogram(ctx, teamID, startMs, endMs)
	if err != nil {
		return HistogramSummary{}, err
	}
	return histogramSummary(row), nil
}

func (s *HTTPMetricsService) GetClientDuration(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	row, err := s.repo.QueryClientRequestDurationHistogram(ctx, teamID, startMs, endMs)
	if err != nil {
		return HistogramSummary{}, err
	}
	return histogramSummary(row), nil
}

func (s *HTTPMetricsService) GetDNSDuration(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	row, err := s.repo.QueryDNSLookupDurationHistogram(ctx, teamID, startMs, endMs)
	if err != nil {
		return HistogramSummary{}, err
	}
	return histogramSummary(row), nil
}

func (s *HTTPMetricsService) GetTLSDuration(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	row, err := s.repo.QueryTLSConnectDurationHistogram(ctx, teamID, startMs, endMs)
	if err != nil {
		return HistogramSummary{}, err
	}
	return histogramSummary(row), nil
}

func (s *HTTPMetricsService) GetTopRoutesByVolume(ctx context.Context, teamID int64, startMs, endMs int64) ([]RouteMetric, error) {
	rows, err := s.repo.QueryRouteAgg(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	slices.SortFunc(rows, func(a, b RouteAggRow) int { return cmp.Compare(b.Count, a.Count) })
	return mapRoutes(rows, topNLimit, false), nil
}

func (s *HTTPMetricsService) GetTopRoutesByLatency(ctx context.Context, teamID int64, startMs, endMs int64) ([]RouteMetric, error) {
	rows, err := s.repo.QueryRouteAgg(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	slices.SortFunc(rows, func(a, b RouteAggRow) int { return cmp.Compare(b.P95Ms, a.P95Ms) })
	return mapRoutes(rows, topNLimit, true), nil
}

func (s *HTTPMetricsService) GetRouteErrorRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]RouteMetric, error) {
	rows, err := s.repo.QueryRouteAgg(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	slices.SortFunc(rows, func(a, b RouteAggRow) int {
		return cmp.Compare(errPct(b.ErrCount, b.Count), errPct(a.ErrCount, a.Count))
	})
	if len(rows) > topNLimit {
		rows = rows[:topNLimit]
	}
	out := make([]RouteMetric, len(rows))
	for i, r := range rows {
		out[i] = RouteMetric{Route: r.Route, ReqCount: int64(r.Count), ErrorPct: errPct(r.ErrCount, r.Count)} //nolint:gosec
	}
	return out, nil
}

func (s *HTTPMetricsService) GetRouteErrorTimeseries(ctx context.Context, teamID int64, startMs, endMs int64) ([]RouteTimeseriesPoint, error) {
	rows, err := s.repo.QueryRouteErrorSeries(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	type k struct {
		ts    time.Time
		route string
	}
	type acc struct{ total, errs uint64 }
	windowMs := endMs - startMs
	agg := map[k]*acc{}
	for _, r := range rows {
		key := k{ts: timebucket.DisplayBucket(r.Timestamp.Unix(), windowMs), route: r.Route}
		x, ok := agg[key]
		if !ok {
			x = &acc{}
			agg[key] = x
		}
		x.total += r.Count
		x.errs += r.ErrCount
	}
	out := make([]RouteTimeseriesPoint, 0, len(agg))
	for kk, x := range agg {
		out = append(out, RouteTimeseriesPoint{
			Timestamp:  kk.ts.UTC().Format("2006-01-02 15:04:05"),
			HttpRoute:  kk.route,
			ReqCount:   int64(x.total), //nolint:gosec
			ErrorCount: int64(x.errs),  //nolint:gosec
			ErrorRate:  errPct(x.errs, x.total),
		})
	}
	slices.SortFunc(out, func(a, b RouteTimeseriesPoint) int {
		if c := cmp.Compare(a.Timestamp, b.Timestamp); c != 0 {
			return c
		}
		return cmp.Compare(a.HttpRoute, b.HttpRoute)
	})
	return out, nil
}

func (s *HTTPMetricsService) GetStatusDistribution(ctx context.Context, teamID int64, startMs, endMs int64) ([]StatusGroupBucket, error) {
	rows, err := s.repo.QueryServerRequestStatusSeries(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	totals := map[string]uint64{}
	for _, r := range rows {
		totals[statusGroup(r.StatusCode)] += r.Count
	}
	out := make([]StatusGroupBucket, 0, len(totals))
	for g, c := range totals {
		out = append(out, StatusGroupBucket{StatusGroup: g, Count: int64(c)}) //nolint:gosec
	}
	slices.SortFunc(out, func(a, b StatusGroupBucket) int { return cmp.Compare(a.StatusGroup, b.StatusGroup) })
	return out, nil
}

func (s *HTTPMetricsService) GetErrorTimeseries(ctx context.Context, teamID int64, startMs, endMs int64) ([]ErrorTimeseriesPoint, error) {
	rows, err := s.repo.QueryServerRequestStatusSeries(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	type acc struct{ total, errs uint64 }
	windowMs := endMs - startMs
	agg := map[time.Time]*acc{}
	for _, r := range rows {
		key := timebucket.DisplayBucket(r.Timestamp.Unix(), windowMs)
		x, ok := agg[key]
		if !ok {
			x = &acc{}
			agg[key] = x
		}
		x.total += r.Count
		if r.StatusCode >= 400 {
			x.errs += r.Count
		}
	}
	out := make([]ErrorTimeseriesPoint, 0, len(agg))
	for tt, x := range agg {
		out = append(out, ErrorTimeseriesPoint{
			Timestamp:  tt.UTC().Format("2006-01-02 15:04:05"),
			ReqCount:   int64(x.total), //nolint:gosec
			ErrorCount: int64(x.errs),  //nolint:gosec
			ErrorRate:  errPct(x.errs, x.total),
		})
	}
	slices.SortFunc(out, func(a, b ErrorTimeseriesPoint) int { return cmp.Compare(a.Timestamp, b.Timestamp) })
	return out, nil
}

func (s *HTTPMetricsService) GetTopExternalHosts(ctx context.Context, teamID int64, startMs, endMs int64) ([]ExternalHostMetric, error) {
	rows, err := s.repo.QueryExternalHostAgg(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	slices.SortFunc(rows, func(a, b HostAggRow) int { return cmp.Compare(b.Count, a.Count) })
	return mapHosts(rows, topNLimit, false), nil
}

func (s *HTTPMetricsService) GetExternalHostLatency(ctx context.Context, teamID int64, startMs, endMs int64) ([]ExternalHostMetric, error) {
	rows, err := s.repo.QueryExternalHostAgg(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	slices.SortFunc(rows, func(a, b HostAggRow) int { return cmp.Compare(b.P95Ms, a.P95Ms) })
	return mapHosts(rows, topNLimit, true), nil
}

func (s *HTTPMetricsService) GetExternalHostErrorRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]ExternalHostMetric, error) {
	rows, err := s.repo.QueryExternalHostAgg(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	slices.SortFunc(rows, func(a, b HostAggRow) int {
		return cmp.Compare(errPct(b.ErrCount, b.Count), errPct(a.ErrCount, a.Count))
	})
	if len(rows) > topNLimit {
		rows = rows[:topNLimit]
	}
	out := make([]ExternalHostMetric, len(rows))
	for i, r := range rows {
		out[i] = ExternalHostMetric{Host: r.Host, ReqCount: int64(r.Count), ErrorPct: errPct(r.ErrCount, r.Count)} //nolint:gosec
	}
	return out, nil
}

func histogramSummary(row HistogramAggRow) HistogramSummary {
	avg := 0.0
	if row.SumHistCount > 0 {
		avg = row.SumHistSum / float64(row.SumHistCount)
	}
	return HistogramSummary{
		Avg: avg,
		P50: row.P50,
		P95: row.P95,
		P99: row.P99,
	}
}

func mapRoutes(rows []RouteAggRow, n int, withP95 bool) []RouteMetric {
	if len(rows) > n {
		rows = rows[:n]
	}
	out := make([]RouteMetric, len(rows))
	for i, r := range rows {
		m := RouteMetric{Route: r.Route, ReqCount: int64(r.Count)} //nolint:gosec
		if withP95 {
			m.P95Ms = float64(r.P95Ms)
		}
		out[i] = m
	}
	return out
}

func mapHosts(rows []HostAggRow, n int, withP95 bool) []ExternalHostMetric {
	if len(rows) > n {
		rows = rows[:n]
	}
	out := make([]ExternalHostMetric, len(rows))
	for i, r := range rows {
		m := ExternalHostMetric{Host: r.Host, ReqCount: int64(r.Count)} //nolint:gosec
		if withP95 {
			m.P95Ms = float64(r.P95Ms)
		}
		out[i] = m
	}
	return out
}

// formatBucket renders a CH-emitted display-bucket DateTime as a stable
// UTC label. Repos already round to the display grain via
// timebucket.DisplayGrainSQL, so this is pure formatting.
func formatBucket(t time.Time) string {
	return t.UTC().Format("2006-01-02 15:04:05")
}

func errPct(errs, total uint64) float64 {
	if total == 0 {
		return 0
	}
	return float64(errs) * 100.0 / float64(total)
}

func statusGroup(code uint16) string {
	switch {
	case code >= 500:
		return "5xx"
	case code >= 400:
		return "4xx"
	case code >= 300:
		return "3xx"
	case code >= 200:
		return "2xx"
	default:
		return "other"
	}
}
