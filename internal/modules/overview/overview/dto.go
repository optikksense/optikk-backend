package overview

// All computed fields (ErrorRate, AvgLatency) are derived in this mapping
// layer from the raw aggregates in the rollup-backed scan struct. The
// repository query returns pure sums/percentiles; Go does the ratio and
// average divisions so the SELECT stays free of conditional math.

func mapRequestRateRows(rows []requestRateRow) []RequestRatePoint {
	points := make([]RequestRatePoint, len(rows))
	for i, row := range rows {
		points[i] = RequestRatePoint{
			Timestamp:    row.Timestamp,
			ServiceName:  row.ServiceName,
			RequestCount: int64(row.RequestCount), //nolint:gosec // domain-bounded
		}
	}
	return points
}

func mapErrorRateRows(rows []errorRateRow) []ErrorRatePoint {
	points := make([]ErrorRatePoint, len(rows))
	for i, row := range rows {
		reqCount := int64(row.RequestCount) //nolint:gosec // domain-bounded
		errCount := int64(row.ErrorCount)   //nolint:gosec // domain-bounded
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
		points[i] = P95LatencyPoint(row)
	}
	return points
}

func mapServiceMetricRows(rows []serviceMetricRow) []ServiceMetric {
	services := make([]ServiceMetric, len(rows))
	for i, row := range rows {
		services[i] = ServiceMetric{
			ServiceName:  row.ServiceName,
			RequestCount: int64(row.RequestCount), //nolint:gosec // domain-bounded
			ErrorCount:   int64(row.ErrorCount),   //nolint:gosec // domain-bounded
			AvgLatency:   avgLatency(row.DurationMsSum, row.RequestCount),
			P50Latency:   row.P50Latency,
			P95Latency:   row.P95Latency,
			P99Latency:   row.P99Latency,
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
			RequestCount:  int64(row.RequestCount), //nolint:gosec // domain-bounded
			ErrorCount:    int64(row.ErrorCount),   //nolint:gosec // domain-bounded
			AvgLatency:    avgLatency(row.DurationMsSum, row.RequestCount),
			P50Latency:    row.P50Latency,
			P95Latency:    row.P95Latency,
			P99Latency:    row.P99Latency,
		}
	}
	return metrics
}

func mapGlobalSummaryRow(row serviceMetricRow) GlobalSummary {
	return GlobalSummary{
		TotalRequests: int64(row.RequestCount), //nolint:gosec // domain-bounded
		ErrorCount:    int64(row.ErrorCount),   //nolint:gosec // domain-bounded
		AvgLatency:    avgLatency(row.DurationMsSum, row.RequestCount),
		P50Latency:    row.P50Latency,
		P95Latency:    row.P95Latency,
		P99Latency:    row.P99Latency,
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
