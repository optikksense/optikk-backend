package telemetry

import "time"

// ---------------------------------------------------------------------------
// Domain records written to ClickHouse
// ---------------------------------------------------------------------------

type SpanRecord struct {
	TeamUUID          string
	TraceID           string
	SpanID            string
	ParentSpanID      string
	ParentServiceName string // denormalized from batch lookup; empty for cross-batch parents
	IsRoot            int    // 1 or 0
	OperationName     string
	ServiceName       string
	SpanKind          string
	StartTime         time.Time
	EndTime           time.Time
	DurationMs        int64
	Status            string
	StatusMessage     string
	HTTPMethod        string
	HTTPURL           string
	HTTPStatusCode    int
	Host              string
	Pod               string
	Container         string
	Attributes        string // JSON
}

type MetricRecord struct {
	TeamUUID       string
	MetricName     string
	MetricType     string
	MetricCategory string
	ServiceName    string
	OperationName  string
	Timestamp      time.Time
	Value          float64
	Count          int64
	Sum            float64
	Min            float64
	Max            float64
	Avg            float64
	P50            float64
	P95            float64
	P99            float64
	HTTPMethod     string
	HTTPStatusCode int
	Status         string
	Host           string
	Pod            string
	Container      string
	Attributes     string // JSON
}

type LogRecord struct {
	TeamUUID   string
	Timestamp  time.Time
	Level      string
	Service    string
	Logger     string
	Message    string
	TraceID    string
	SpanID     string
	Host       string
	Pod        string
	Container  string
	Thread     string
	Exception  string
	Attributes string // JSON
}
