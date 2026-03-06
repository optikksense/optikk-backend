// Package otlp provides an OTLP/HTTP JSON receive endpoint.
// Structures are based on the OpenTelemetry Protocol specification:
// https://opentelemetry.io/docs/specs/otlp/#otlphttp
package otlp

// ── Shared ────────────────────────────────────────────────────────────────────

// AnyValue holds a single OTel attribute value in JSON form.
// Only string, int, float, bool, and bytes values are common in practice.
type AnyValue struct {
	StringValue *string  `json:"stringValue,omitempty"`
	IntValue    *int64   `json:"intValue,omitempty,string"`
	DoubleValue *float64 `json:"doubleValue,omitempty"`
	BoolValue   *bool    `json:"boolValue,omitempty"`
	BytesValue  *string  `json:"bytesValue,omitempty"` // base64-encoded
}

// KeyValue is an OTel attribute key-value pair.
type KeyValue struct {
	Key   string   `json:"key"`
	Value AnyValue `json:"value"`
}

// Resource represents the entity producing telemetry (service, pod, etc.).
type Resource struct {
	Attributes []KeyValue `json:"attributes"`
}

// InstrumentationScope identifies the instrumentation library.
type InstrumentationScope struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// ── Traces ────────────────────────────────────────────────────────────────────

// ExportTraceServiceRequest is the top-level OTLP/HTTP JSON body for traces.
type ExportTraceServiceRequest struct {
	ResourceSpans []ResourceSpans `json:"resourceSpans"`
}

type ResourceSpans struct {
	Resource   Resource     `json:"resource"`
	ScopeSpans []ScopeSpans `json:"scopeSpans"`
}

type ScopeSpans struct {
	Scope InstrumentationScope `json:"scope"`
	Spans []OTLPSpan           `json:"spans"`
}

// OTLPSpan mirrors the OTel Span proto fields surfaced in OTLP/JSON.
type OTLPSpan struct {
	TraceID           string      `json:"traceId"`
	SpanID            string      `json:"spanId"`
	ParentSpanID      string      `json:"parentSpanId"`
	Name              string      `json:"name"`
	Kind              int         `json:"kind"` // SpanKind enum 0-5
	StartTimeUnixNano string      `json:"startTimeUnixNano"`
	EndTimeUnixNano   string      `json:"endTimeUnixNano"`
	Attributes        []KeyValue  `json:"attributes"`
	Status            SpanStatus  `json:"status"`
	Events            []SpanEvent `json:"events,omitempty"`
	Links             []SpanLink  `json:"links,omitempty"`
}

// SpanEvent represents an event within a span.
type SpanEvent struct {
	TimeUnixNano string     `json:"timeUnixNano"`
	Name         string     `json:"name"`
	Attributes   []KeyValue `json:"attributes,omitempty"`
}

// SpanLink represents a link to another span.
type SpanLink struct {
	TraceID    string     `json:"traceId"`
	SpanID     string     `json:"spanId"`
	Attributes []KeyValue `json:"attributes,omitempty"`
}

// SpanStatus mirrors the OTel Status proto (code 0=UNSET,1=OK,2=ERROR).
type SpanStatus struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// ── Logs ──────────────────────────────────────────────────────────────────────

// ExportLogsServiceRequest is the top-level OTLP/HTTP JSON body for logs.
type ExportLogsServiceRequest struct {
	ResourceLogs []ResourceLogs `json:"resourceLogs"`
}

type ResourceLogs struct {
	Resource  Resource    `json:"resource"`
	ScopeLogs []ScopeLogs `json:"scopeLogs"`
}

type ScopeLogs struct {
	Scope      InstrumentationScope `json:"scope"`
	LogRecords []LogRecord          `json:"logRecords"`
}

// LogRecord mirrors the OTel LogRecord proto fields surfaced in OTLP/JSON.
type LogRecord struct {
	TimeUnixNano         string     `json:"timeUnixNano"`
	ObservedTimeUnixNano string     `json:"observedTimeUnixNano"`
	SeverityNumber       int        `json:"severityNumber"`
	SeverityText         string     `json:"severityText"`
	Body                 AnyValue   `json:"body"`
	Attributes           []KeyValue `json:"attributes"`
	TraceID              string     `json:"traceId"`
	SpanID               string     `json:"spanId"`
	Flags                uint32     `json:"flags"`
}

// ── Metrics ───────────────────────────────────────────────────────────────────

// ExportMetricsServiceRequest is the top-level OTLP/HTTP JSON body for metrics.
type ExportMetricsServiceRequest struct {
	ResourceMetrics []ResourceMetrics `json:"resourceMetrics"`
}

type ResourceMetrics struct {
	Resource     Resource       `json:"resource"`
	ScopeMetrics []ScopeMetrics `json:"scopeMetrics"`
}

type ScopeMetrics struct {
	Scope   InstrumentationScope `json:"scope"`
	Metrics []OTLPMetric         `json:"metrics"`
}

// OTLPMetric covers gauge, sum, and histogram metric types.
type OTLPMetric struct {
	Name        string           `json:"name"`
	Description string           `json:"description"`
	Unit        string           `json:"unit"`
	Gauge       *MetricGauge     `json:"gauge,omitempty"`
	Sum         *MetricSum       `json:"sum,omitempty"`
	Histogram   *MetricHistogram `json:"histogram,omitempty"`
}

type MetricGauge struct {
	DataPoints []NumberDataPoint `json:"dataPoints"`
}

type MetricSum struct {
	AggregationTemporality any               `json:"aggregationTemporality,omitempty"`
	IsMonotonic            bool              `json:"isMonotonic,omitempty"`
	DataPoints             []NumberDataPoint `json:"dataPoints"`
}

type MetricHistogram struct {
	AggregationTemporality any                  `json:"aggregationTemporality,omitempty"`
	DataPoints             []HistogramDataPoint `json:"dataPoints"`
}

// NumberDataPoint is a single gauge or sum data point.
type NumberDataPoint struct {
	Attributes   []KeyValue `json:"attributes"`
	TimeUnixNano string     `json:"timeUnixNano"`
	AsDouble     *float64   `json:"asDouble,omitempty"`
	AsInt        *int64     `json:"asInt,omitempty,string"`
}

// HistogramDataPoint is a single histogram data point.
type HistogramDataPoint struct {
	Attributes     []KeyValue `json:"attributes"`
	TimeUnixNano   string     `json:"timeUnixNano"`
	Count          uint64     `json:"count"`
	Sum            *float64   `json:"sum,omitempty"`
	Min            *float64   `json:"min,omitempty"`
	Max            *float64   `json:"max,omitempty"`
	ExplicitBounds []float64  `json:"explicitBounds"`
	BucketCounts   []uint64   `json:"bucketCounts"`
}
