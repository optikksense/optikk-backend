package errorfingerprint

import "time"

// ErrorFingerprint represents a group of similar errors identified by a hash
// of (service, operation, exception_type, status_message).
type ErrorFingerprint struct {
	Fingerprint   string    `json:"fingerprint"`
	ServiceName   string    `json:"serviceName"`
	OperationName string    `json:"operationName"`
	ExceptionType string    `json:"exceptionType"`
	StatusMessage string    `json:"statusMessage"`
	FirstSeen     time.Time `json:"firstSeen"`
	LastSeen      time.Time `json:"lastSeen"`
	Count         int64     `json:"count"`
	SampleTraceID string    `json:"sampleTraceId"`
}

// FingerprintTrendPoint is a time-series point for a single fingerprint.
type FingerprintTrendPoint struct {
	Timestamp time.Time `json:"timestamp"`
	Count     int64     `json:"count"`
}
