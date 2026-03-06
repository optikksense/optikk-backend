package overview

import (
	dbutil "github.com/observability/observability-backend-go/internal/database"
)

type TimeSeriesAggregator struct{}

func NewTimeSeriesAggregator() *TimeSeriesAggregator {
	return &TimeSeriesAggregator{}
}

func (a *TimeSeriesAggregator) Aggregate(rows []map[string]any) (any, error) {
	points := make([]TimeSeriesPoint, len(rows))
	for i, row := range rows {
		points[i] = TimeSeriesPoint{
			Timestamp:     dbutil.TimeFromAny(row["time_bucket"]),
			ServiceName:   dbutil.StringFromAny(row["service.name"]),
			OperationName: dbutil.StringFromAny(row["operation.name"]),
			HTTPMethod:    dbutil.StringFromAny(row["http.request.method"]),
			RequestCount:  dbutil.Int64FromAny(row["request_count"]),
			ErrorCount:    dbutil.Int64FromAny(row["error_count"]),
			AvgLatency:    dbutil.Float64FromAny(row["http.server.request.duration"]),
			P50:           dbutil.Float64FromAny(row["p50_latency"]),
			P95:           dbutil.Float64FromAny(row["p95_latency"]),
			P99:           dbutil.Float64FromAny(row["p99_latency"]),
		}
	}
	return points, nil
}

type ServiceAggregator struct{}

func NewServiceAggregator() *ServiceAggregator {
	return &ServiceAggregator{}
}

func (a *ServiceAggregator) Aggregate(rows []map[string]any) (any, error) {
	services := make([]ServiceMetric, len(rows))
	for i, row := range rows {
		services[i] = ServiceMetric{
			ServiceName:  dbutil.StringFromAny(row["service.name"]),
			RequestCount: dbutil.Int64FromAny(row["request_count"]),
			ErrorCount:   dbutil.Int64FromAny(row["error_count"]),
			AvgLatency:   dbutil.Float64FromAny(row["http.server.request.duration"]),
			P50Latency:   dbutil.Float64FromAny(row["p50_latency"]),
			P95Latency:   dbutil.Float64FromAny(row["p95_latency"]),
			P99Latency:   dbutil.Float64FromAny(row["p99_latency"]),
		}
	}
	return services, nil
}

type EndpointAggregator struct{}

func NewEndpointAggregator() *EndpointAggregator {
	return &EndpointAggregator{}
}

func (a *EndpointAggregator) Aggregate(rows []map[string]any) (any, error) {
	metrics := make([]EndpointMetric, len(rows))
	for i, row := range rows {
		metrics[i] = EndpointMetric{
			ServiceName:   dbutil.StringFromAny(row["service.name"]),
			OperationName: dbutil.StringFromAny(row["operation.name"]),
			HTTPMethod:    dbutil.StringFromAny(row["http.request.method"]),
			RequestCount:  dbutil.Int64FromAny(row["request_count"]),
			ErrorCount:    dbutil.Int64FromAny(row["error_count"]),
			AvgLatency:    dbutil.Float64FromAny(row["http.server.request.duration"]),
			P50Latency:    dbutil.Float64FromAny(row["p50_latency"]),
			P95Latency:    dbutil.Float64FromAny(row["p95_latency"]),
			P99Latency:    dbutil.Float64FromAny(row["p99_latency"]),
		}
	}
	return metrics, nil
}
