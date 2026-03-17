package overview

func mapRequestRateRows(rows []requestRateRow) []RequestRatePoint {
	points := make([]RequestRatePoint, len(rows))
	for i, row := range rows {
		points[i] = RequestRatePoint{
			Timestamp:    row.TimeBucket,
			ServiceName:  row.ServiceName,
			RequestCount: row.RequestCount,
		}
	}
	return points
}

func mapErrorRateRows(rows []errorRateRow) []ErrorRatePoint {
	points := make([]ErrorRatePoint, len(rows))
	for i, row := range rows {
		points[i] = ErrorRatePoint{
			Timestamp:    row.TimeBucket,
			ServiceName:  row.ServiceName,
			RequestCount: row.RequestCount,
			ErrorCount:   row.ErrorCount,
			ErrorRate:    row.ErrorRate,
		}
	}
	return points
}

func mapP95LatencyRows(rows []p95LatencyRow) []P95LatencyPoint {
	points := make([]P95LatencyPoint, len(rows))
	for i, row := range rows {
		points[i] = P95LatencyPoint{
			Timestamp:   row.TimeBucket,
			ServiceName: row.ServiceName,
			P95:         row.P95,
		}
	}
	return points
}

func mapServiceMetricRows(rows []serviceMetricRow) []ServiceMetric {
	services := make([]ServiceMetric, len(rows))
	for i, row := range rows {
		services[i] = ServiceMetric{
			ServiceName:  row.ServiceName,
			RequestCount: row.RequestCount,
			ErrorCount:   row.ErrorCount,
			AvgLatency:   row.AvgLatency,
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
			HTTPMethod:    row.HTTPMethod,
			RequestCount:  row.RequestCount,
			ErrorCount:    row.ErrorCount,
			AvgLatency:    row.AvgLatency,
			P50Latency:    row.P50Latency,
			P95Latency:    row.P95Latency,
			P99Latency:    row.P99Latency,
		}
	}
	return metrics
}

func mapTimeSeriesRows(rows []timeSeriesRow) []TimeSeriesPoint {
	points := make([]TimeSeriesPoint, len(rows))
	for i, row := range rows {
		points[i] = TimeSeriesPoint{
			Timestamp:     row.TimeBucket,
			ServiceName:   row.ServiceName,
			OperationName: row.OperationName,
			HTTPMethod:    row.HTTPMethod,
			RequestCount:  row.RequestCount,
			ErrorCount:    row.ErrorCount,
			AvgLatency:    row.AvgLatency,
			P50:           row.P50,
			P95:           row.P95,
			P99:           row.P99,
		}
	}
	return points
}
