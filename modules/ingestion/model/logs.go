package model

import "time"

// ---------------------------------------------------------------------------
// OTLP Logs payload structs
// ---------------------------------------------------------------------------

type OTLPLogsPayload struct {
	ResourceLogs []OTLPResourceLogs `json:"resourceLogs"`
}

type OTLPResourceLogs struct {
	Resource  OTLPResource    `json:"resource"`
	ScopeLogs []OTLPScopeLogs `json:"scopeLogs"`
}

type OTLPScopeLogs struct {
	Scope      OTLPScope       `json:"scope"`
	LogRecords []OTLPLogRecord `json:"logRecords"`
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

// ---------------------------------------------------------------------------
// Domain Record for logs
// ---------------------------------------------------------------------------

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
