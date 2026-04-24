package mapper

import (
	trace "go.opentelemetry.io/proto/otlp/trace/v1"
)

// spanStatus extracts message and code, defaulting to UNSET when unset.
func spanStatus(s *trace.Span) (string, trace.Status_StatusCode) {
	if s.Status != nil {
		return s.Status.GetMessage(), s.Status.GetCode()
	}
	return "", trace.Status_STATUS_CODE_UNSET
}

// spanKindString renders the enum as the short token observability queries use
// (SERVER, CLIENT, INTERNAL, PRODUCER, CONSUMER, UNSPECIFIED).
func spanKindString(kind trace.Span_SpanKind) string {
	switch kind {
	case trace.Span_SPAN_KIND_INTERNAL:
		return "INTERNAL"
	case trace.Span_SPAN_KIND_SERVER:
		return "SERVER"
	case trace.Span_SPAN_KIND_CLIENT:
		return "CLIENT"
	case trace.Span_SPAN_KIND_PRODUCER:
		return "PRODUCER"
	case trace.Span_SPAN_KIND_CONSUMER:
		return "CONSUMER"
	default:
		return "UNSPECIFIED"
	}
}

// statusCodeString renders the status code enum as a short token.
func statusCodeString(code trace.Status_StatusCode) string {
	switch code {
	case trace.Status_STATUS_CODE_OK:
		return "OK"
	case trace.Status_STATUS_CODE_ERROR:
		return "ERROR"
	default:
		return "UNSET"
	}
}
