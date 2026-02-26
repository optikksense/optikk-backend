package model

import "time"

// ---------------------------------------------------------------------------
// OTLP Traces payload structs
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

// ---------------------------------------------------------------------------
// Domain Record for spans
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
