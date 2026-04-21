package overview

import (
	"github.com/Optikk-Org/optikk-backend/internal/infra/utils"
)

func mapRequestRateRows(rows []requestRateRow) []RequestRatePoint {
	points := make([]RequestRatePoint, len(rows))
	for i, row := range rows {
		points[i] = RequestRatePoint{
			Timestamp:    row.Timestamp,
			ServiceName:  row.ServiceName,
			RequestCount: int64(row.RequestCount),
		}
	}
	return points
}

func mapErrorRateRows(rows []errorRateRow) []ErrorRatePoint {
	points := make([]ErrorRatePoint, len(rows))
	for i, row := range rows {
		reqCount := int64(row.RequestCount)
		errCount := int64(row.ErrorCount)
		var rate float64
		if reqCount > 0 {
			rate = float64(errCount) * 100.0 / float64(reqCount)
		}
		points[i] = ErrorRatePoint{
			Timestamp:    row.Timestamp,
			ServiceName:  row.ServiceName,
			RequestCount: reqCount,
			ErrorCount:   errCount,
			ErrorRate:    rate,
		}
	}
	return points
}

func mapP95LatencyRows(rows []p95LatencyRow) []P95LatencyPoint {
	points := make([]P95LatencyPoint, len(rows))
	for i, row := range rows {
		points[i] = P95LatencyPoint{
			Timestamp:   row.Timestamp,
			ServiceName: row.ServiceName,
			P95:         utils.SanitizeFloat(row.P95),
		}
	}
	return points
}

func mapChartMetricsRows(rows []chartMetricsRow) []ChartMetricsPoint {
	points := make([]ChartMetricsPoint, len(rows))
	for i, row := range rows {
		reqCount := int64(row.RequestCount)
		errCount := int64(row.ErrorCount)
		var rate float64
		if reqCount > 0 {
			rate = float64(errCount) * 100.0 / float64(reqCount)
		}
		points[i] = ChartMetricsPoint{
			Timestamp:    row.Timestamp,
			ServiceName:  row.ServiceName,
			RequestCount: reqCount,
			ErrorCount:   errCount,
			ErrorRate:    rate,
			P95:          utils.SanitizeFloat(row.P95),
		}
	}
	return points
}

func mapServiceMetricRows(rows []serviceMetricRow) []ServiceMetric {
	services := make([]ServiceMetric, len(rows))
	for i, row := range rows {
		services[i] = ServiceMetric{
			ServiceName:  row.ServiceName,
			RequestCount: int64(row.RequestCount),
			ErrorCount:   int64(row.ErrorCount),
			AvgLatency:   utils.SanitizeFloat(avgLatency(row.DurationMsSum, row.RequestCount)),
			P50Latency:   utils.SanitizeFloat(row.P50Latency),
			P95Latency:   utils.SanitizeFloat(row.P95Latency),
			P99Latency:   utils.SanitizeFloat(row.P99Latency),
		}
	}
	return services
}

func mapEndpointMetricRows(rows []endpointMetricRow) []EndpointMetric {
	metrics := make([]EndpointMetric, len(rows))
	for i, row := range rows {
		metrics[i] = EndpointMetric{
			ServiceName:   row.ServiceName,
			OperationName: row.OperationName,
			EndpointName:  row.EndpointName,
			HTTPMethod:    row.HTTPMethod,
			RequestCount:  int64(row.RequestCount),
			ErrorCount:    int64(row.ErrorCount),
			AvgLatency:    utils.SanitizeFloat(avgLatency(row.DurationMsSum, row.RequestCount)),
			P50Latency:    utils.SanitizeFloat(row.P50Latency),
			P95Latency:    utils.SanitizeFloat(row.P95Latency),
			P99Latency:    utils.SanitizeFloat(row.P99Latency),
		}
	}
	return metrics
}

func mapGlobalSummaryRow(row serviceMetricRow) GlobalSummary {
	return GlobalSummary{
		TotalRequests: int64(row.RequestCount),
		ErrorCount:    int64(row.ErrorCount),
		AvgLatency:    utils.SanitizeFloat(avgLatency(row.DurationMsSum, row.RequestCount)),
		P50Latency:    utils.SanitizeFloat(row.P50Latency),
		P95Latency:    utils.SanitizeFloat(row.P95Latency),
		P99Latency:    utils.SanitizeFloat(row.P99Latency),
	}
}

// avgLatency derives mean latency from the rollup's duration-sum + request-
// count state — replaces the pre-Phase-5 `avg(duration_nano / 1e6)` aggregate.
func avgLatency(durationMsSum float64, requestCount uint64) float64 {
	if requestCount == 0 {
		return 0
	}
	return durationMsSum / float64(requestCount)
}
