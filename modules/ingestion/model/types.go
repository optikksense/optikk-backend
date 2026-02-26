package model

import "time"

// ---------------------------------------------------------------------------
// OTLP JSON payload structs (OTLP/HTTP JSON format)
// ---------------------------------------------------------------------------

type OTLPTracesPayload struct {
	ResourceSpans []OTLPResourceSpans `json:"resourceSpans"`
}

type OTLPResourceSpans struct {
	Resource   OTLPResource     `json:"resource"`
	ScopeSpans []OTLPScopeSpans `json:"scopeSpans"`
}

type OTLPScopeSpans struct {
	Scope OTLPScope  `json:"scope"`
	Spans []OTLPSpan `json:"spans"`
}

type OTLPSpan struct {
	TraceID           string          `json:"traceId"`
	SpanID            string          `json:"spanId"`
	ParentSpanID      string          `json:"parentSpanId,omitempty"`
	Name              string          `json:"name"`
	Kind              int             `json:"kind"` // 0=UNSPECIFIED,1=INTERNAL,2=SERVER,3=CLIENT,4=PRODUCER,5=CONSUMER
	StartTimeUnixNano string          `json:"startTimeUnixNano"`
	EndTimeUnixNano   string          `json:"endTimeUnixNano"`
	Attributes        []OTLPAttribute `json:"attributes"`
	Status            *OTLPStatus     `json:"status,omitempty"`
}

type OTLPStatus struct {
	Code    int    `json:"code"` // 0=UNSET, 1=OK, 2=ERROR
	Message string `json:"message,omitempty"`
}

type OTLPMetricsPayload struct {
	ResourceMetrics []OTLPResourceMetrics `json:"resourceMetrics"`
}

type OTLPLogsPayload struct {
	ResourceLogs []OTLPResourceLogs `json:"resourceLogs"`
}

type OTLPResourceMetrics struct {
	Resource     OTLPResource       `json:"resource"`
	ScopeMetrics []OTLPScopeMetrics `json:"scopeMetrics"`
}

type OTLPResourceLogs struct {
	Resource  OTLPResource    `json:"resource"`
	ScopeLogs []OTLPScopeLogs `json:"scopeLogs"`
}

type OTLPResource struct {
	Attributes []OTLPAttribute `json:"attributes"`
}

type OTLPScopeMetrics struct {
	Scope   OTLPScope    `json:"scope"`
	Metrics []OTLPMetric `json:"metrics"`
}

type OTLPScopeLogs struct {
	Scope      OTLPScope       `json:"scope"`
	LogRecords []OTLPLogRecord `json:"logRecords"`
}

type OTLPScope struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type OTLPMetric struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Unit        string         `json:"unit"`
	Gauge       *OTLPGauge     `json:"gauge,omitempty"`
	Sum         *OTLPSum       `json:"sum,omitempty"`
	Histogram   *OTLPHistogram `json:"histogram,omitempty"`
}

type OTLPGauge struct {
	DataPoints []OTLPNumberDataPoint `json:"dataPoints"`
}

type OTLPSum struct {
	DataPoints             []OTLPNumberDataPoint `json:"dataPoints"`
	AggregationTemporality int                   `json:"aggregationTemporality"`
	IsMonotonic            bool                  `json:"isMonotonic"`
}

type OTLPHistogram struct {
	DataPoints             []OTLPHistogramDataPoint `json:"dataPoints"`
	AggregationTemporality int                      `json:"aggregationTemporality"`
}

type OTLPNumberDataPoint struct {
	Attributes        []OTLPAttribute `json:"attributes"`
	StartTimeUnixNano string          `json:"startTimeUnixNano"`
	TimeUnixNano      string          `json:"timeUnixNano"`
	AsDouble          *float64        `json:"asDouble,omitempty"`
	AsInt             *string         `json:"asInt,omitempty"`
}

type OTLPHistogramDataPoint struct {
	Attributes        []OTLPAttribute `json:"attributes"`
	StartTimeUnixNano string          `json:"startTimeUnixNano"`
	TimeUnixNano      string          `json:"timeUnixNano"`
	Count             string          `json:"count"`
	Sum               *float64        `json:"sum,omitempty"`
	Min               *float64        `json:"min,omitempty"`
	Max               *float64        `json:"max,omitempty"`
	BucketCounts      []string        `json:"bucketCounts"`
	ExplicitBounds    []float64       `json:"explicitBounds"`
}

type OTLPLogRecord struct {
	TimeUnixNano         string          `json:"timeUnixNano"`
	ObservedTimeUnixNano string          `json:"observedTimeUnixNano,omitempty"`
	SeverityNumber       int             `json:"severityNumber,omitempty"`
	SeverityText         string          `json:"severityText,omitempty"`
	Body                 OTLPAnyValue    `json:"body"`
	Attributes           []OTLPAttribute `json:"attributes"`
	TraceID              string          `json:"traceId,omitempty"`
	SpanID               string          `json:"spanId,omitempty"`
}

type OTLPAttribute struct {
	Key   string       `json:"key"`
	Value OTLPAnyValue `json:"value"`
}

type OTLPAnyValue struct {
	StringValue *string  `json:"stringValue,omitempty"`
	IntValue    *string  `json:"intValue,omitempty"`
	DoubleValue *float64 `json:"doubleValue,omitempty"`
	BoolValue   *bool    `json:"boolValue,omitempty"`
}

// ---------------------------------------------------------------------------
// Domain Records for Repository layer
// ---------------------------------------------------------------------------

type SpanRecord struct {
	TeamUUID       string
	TraceID        string
	SpanID         string
	ParentSpanID   string
	IsRoot         int // 1 or 0
	OperationName  string
	ServiceName    string
	SpanKind       string
	StartTime      time.Time
	EndTime        time.Time
	DurationMs     int64
	Status         string
	StatusMessage  string
	HTTPMethod     string
	HTTPURL        string
	HTTPStatusCode int
	Host           string
	Pod            string
	Container      string
	Attributes     string // Pre-serialized JSON
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
	Attributes     string // Pre-serialized JSON
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
	Attributes string // Pre-serialized JSON
}
