package errortracking

import "time"

type ExceptionRatePoint struct {
	Timestamp     time.Time `json:"timestamp"`
	ExceptionType string    `json:"exception_type"`
	Count         int64     `json:"count"`
}

type ErrorHotspotCell struct {
	ServiceName   string  `json:"service_name"`
	OperationName string  `json:"operation_name"`
	ErrorRate     float64 `json:"error_rate"`
	ErrorCount    int64   `json:"error_count"`
	TotalCount    int64   `json:"total_count"`
}

type HTTP5xxByRoute struct {
	HTTPRoute   string `json:"http_route"`
	ServiceName string `json:"service_name"`
	Count       int64  `json:"count"`
}
