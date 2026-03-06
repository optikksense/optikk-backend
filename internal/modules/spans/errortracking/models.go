package errortracking

import "time"

// ExceptionRatePoint is a time-series point of exception count per type.
type ExceptionRatePoint struct {
	Timestamp     time.Time `json:"timestamp"`
	ExceptionType string    `json:"exceptionType"`
	Count         int64     `json:"count"`
}

// ErrorHotspotCell is a single (service × operation) cell in the error heatmap.
type ErrorHotspotCell struct {
	ServiceName   string  `json:"serviceName"`
	OperationName string  `json:"operationName"`
	ErrorRate     float64 `json:"errorRate"`
	ErrorCount    int64   `json:"errorCount"`
	TotalCount    int64   `json:"totalCount"`
}

// HTTP5xxByRoute is a count of 5xx responses per HTTP route.
type HTTP5xxByRoute struct {
	HTTPRoute   string `json:"httpRoute"`
	ServiceName string `json:"serviceName"`
	Count       int64  `json:"count"`
}
