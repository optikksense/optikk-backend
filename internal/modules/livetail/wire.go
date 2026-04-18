package livetail

import "time"

// WireLog is the JSON shape the live-tail WebSocket emits for each log event.
// Kept in the livetail package (not the ingestion package) so both the
// publisher (ingestion/logs/livetail.go) and the consumer
// (livetail/handler.go) depend on this single definition without forming an
// import cycle.
type WireLog struct {
	Timestamp         uint64             `json:"timestamp"`
	ObservedTimestamp uint64             `json:"observed_timestamp"`
	SeverityText      string             `json:"severity_text"`
	SeverityNumber    uint8              `json:"severity_number"`
	Body              string             `json:"body"`
	TraceID           string             `json:"trace_id"`
	SpanID            string             `json:"span_id"`
	TraceFlags        uint32             `json:"trace_flags"`
	ServiceName       string             `json:"service_name"`
	Host              string             `json:"host"`
	Pod               string             `json:"pod"`
	Container         string             `json:"container"`
	Environment       string             `json:"environment"`
	AttributesString  map[string]string  `json:"attributes_string,omitempty"`
	AttributesNumber  map[string]float64 `json:"attributes_number,omitempty"`
	AttributesBool    map[string]bool    `json:"attributes_bool,omitempty"`
	ScopeName         string             `json:"scope_name"`
	ScopeVersion      string             `json:"scope_version"`
	Level             string             `json:"level"`
	Message           string             `json:"message"`
	Service           string             `json:"service"`
	EmitMs            int64              `json:"emit_ms"`
}

// WireSpan is the JSON shape the live-tail WebSocket emits for each span event.
type WireSpan struct {
	SpanID        string    `json:"spanId"`
	TraceID       string    `json:"traceId"`
	ServiceName   string    `json:"serviceName"`
	OperationName string    `json:"operationName"`
	DurationMs    float64   `json:"durationMs"`
	Status        string    `json:"status"`
	Host          string    `json:"host,omitempty"`
	HTTPMethod    string    `json:"httpMethod,omitempty"`
	HTTPStatus    string    `json:"httpStatusCode,omitempty"`
	SpanKind      string    `json:"spanKind,omitempty"`
	HasError      bool      `json:"hasError"`
	Timestamp     time.Time `json:"timestamp"`
	EmitMs        int64     `json:"emit_ms"`
}
