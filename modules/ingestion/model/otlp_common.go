package model

// ---------------------------------------------------------------------------
// Shared OTLP types used across all signal types (logs, metrics, traces)
// ---------------------------------------------------------------------------

type OTLPResource struct {
	Attributes []OTLPAttribute `json:"attributes"`
}

type OTLPScope struct {
	Name    string `json:"name"`
	Version string `json:"version"`
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
