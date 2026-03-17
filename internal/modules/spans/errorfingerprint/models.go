package errorfingerprint

import "time"

// ErrorFingerprint represents a group of similar errors identified by a hash
// of (service, operation, exception_type, status_message).
type ErrorFingerprint struct {
	Fingerprint   string    `json:"fingerprint"  ch:"fingerprint"`
	ServiceName   string    `json:"serviceName"  ch:"service_name"`
	OperationName string    `json:"operationName" ch:"operation_name"`
	ExceptionType string    `json:"exceptionType" ch:"exception_type"`
	StatusMessage string    `json:"statusMessage" ch:"status_message"`
	FirstSeen     time.Time `json:"firstSeen"    ch:"first_seen"`
	LastSeen      time.Time `json:"lastSeen"     ch:"last_seen"`
	Count         int64     `json:"count"        ch:"cnt"`
	SampleTraceID string    `json:"sampleTraceId" ch:"sample_trace_id"`
}

// FingerprintTrendPoint is a time-series point for a single fingerprint.
type FingerprintTrendPoint struct {
	Timestamp time.Time `json:"timestamp" ch:"ts"`
	Count     int64     `json:"count"     ch:"cnt"`
}
