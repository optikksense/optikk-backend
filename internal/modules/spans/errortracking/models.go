package errortracking

import "time"

type ExceptionRatePoint struct {
	Timestamp     time.Time `json:"timestamp"      ch:"time_bucket"`
	ExceptionType string    `json:"exception_type" ch:"exception_type"`
	Count         int64     `json:"count"          ch:"event_count"`
}

type ErrorHotspotCell struct {
	ServiceName   string  `json:"service_name"   ch:"service_name"`
	OperationName string  `json:"operation_name" ch:"operation_name"`
	ErrorRate     float64 `json:"error_rate"     ch:"error_rate"`
	ErrorCount    int64   `json:"error_count"    ch:"error_count"`
	TotalCount    int64   `json:"total_count"    ch:"total_count"`
}

type HTTP5xxByRoute struct {
	HTTPRoute   string `json:"http_route"   ch:"http_route"`
	ServiceName string `json:"service_name" ch:"service_name"`
	Count       int64  `json:"count"        ch:"count_5xx"`
}
