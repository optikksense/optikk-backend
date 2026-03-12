package errortracking

import "time"

type ExceptionRatePoint struct {
	Timestamp     time.Time `json:"timestamp"`
	ExceptionType string    `json:"exceptionType"`
	Count         int64     `json:"count"`
}

type ErrorHotspotCell struct {
	ServiceName   string  `json:"serviceName"`
	OperationName string  `json:"operationName"`
	ErrorRate     float64 `json:"errorRate"`
	ErrorCount    int64   `json:"errorCount"`
	TotalCount    int64   `json:"totalCount"`
}

type HTTP5xxByRoute struct {
	HTTPRoute   string `json:"httpRoute"`
	ServiceName string `json:"serviceName"`
	Count       int64  `json:"count"`
}
