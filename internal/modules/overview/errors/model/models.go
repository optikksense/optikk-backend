package model

import "time"

// ErrorGroup represents an aggregation of similar errors.
type ErrorGroup struct {
	ServiceName     string    `json:"service_name"`
	OperationName   string    `json:"operation_name"`
	StatusMessage   string    `json:"status_message"`
	HTTPStatusCode  int       `json:"http_status_code"`
	ErrorCount      int64     `json:"error_count"`
	LastOccurrence  time.Time `json:"last_occurrence"`
	FirstOccurrence time.Time `json:"first_occurrence"`
	SampleTraceID   string    `json:"sample_trace_id"`
}

// TimeSeriesPoint represents service-level request and error buckets for the errors dashboard.
type TimeSeriesPoint struct {
	ServiceName  string    `json:"service_name"`
	Timestamp    time.Time `json:"timestamp"`
	RequestCount int64     `json:"request_count"`
	ErrorCount   int64     `json:"error_count"`
	ErrorRate    float64   `json:"error_rate"`
	AvgLatency   float64   `json:"avg_latency"`
}
