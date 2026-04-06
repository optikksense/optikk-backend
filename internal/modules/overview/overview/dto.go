package overview

func mapRequestRateRows(rows []requestRateRow) []RequestRatePoint {
	points := make([]RequestRatePoint, len(rows))
	for i, row := range rows {
		points[i] = RequestRatePoint(row)
	}
	return points
}

func mapErrorRateRows(rows []errorRateRow) []ErrorRatePoint {
	points := make([]ErrorRatePoint, len(rows))
	for i, row := range rows {
		points[i] = ErrorRatePoint(row)
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
		services[i] = ServiceMetric(row)
	}
	return services
}

func mapEndpointMetricRows(rows []endpointMetricRow) []EndpointMetric {
	metrics := make([]EndpointMetric, len(rows))
	for i, row := range rows {
		metrics[i] = EndpointMetric(row)
	}
	return metrics
}

func mapTimeSeriesRows(rows []timeSeriesRow) []TimeSeriesPoint {
	points := make([]TimeSeriesPoint, len(rows))
	for i, row := range rows {
		points[i] = TimeSeriesPoint(row)
	}
	return points
}

func mapGlobalSummaryRow(row serviceMetricRow) GlobalSummary {
	return GlobalSummary{
		TotalRequests: row.RequestCount,
		ErrorCount:    row.ErrorCount,
		AvgLatency:    row.AvgLatency,
		P50Latency:    row.P50Latency,
		P95Latency:    row.P95Latency,
		P99Latency:    row.P99Latency,
	}
}
