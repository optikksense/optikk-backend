package model

import "time"

// ---------------------------------------------------------------------------
// OTLP Metrics payload structs
// ---------------------------------------------------------------------------

type OTLPMetricsPayload struct {
	ResourceMetrics []OTLPResourceMetrics `json:"resourceMetrics"`
}

type OTLPResourceMetrics struct {
	Resource     OTLPResource       `json:"resource"`
	ScopeMetrics []OTLPScopeMetrics `json:"scopeMetrics"`
}

type OTLPScopeMetrics struct {
	Scope   OTLPScope    `json:"scope"`
	Metrics []OTLPMetric `json:"metrics"`
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

// ---------------------------------------------------------------------------
// Domain Record for metrics
// ---------------------------------------------------------------------------

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
