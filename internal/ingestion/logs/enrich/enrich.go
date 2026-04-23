// Package enrich holds the pure normalization pass that sits between the OTLP
// mapper and the Kafka producer. Normalization is intentionally free of I/O so
// the same function can run at ingest time AND during reprocessing / backfill
// tools without dragging in Kafka or ClickHouse plumbing.
//
// The helpers here operate on plain strings / maps to avoid an import cycle
// with the outer `logs` package (which owns the Row type and calls these
// helpers from its mapper). The rules:
//   - severity_text is upper-cased and trimmed. When blank, severity_number is
//     rendered back out to a canonical token (TRACE/DEBUG/INFO/WARN/ERROR/FATAL).
//   - trace_id / span_id that are all zeroes are zeroed to "" so the CH bloom
//     filter on trace_id is not polluted.
//   - resource fields get fallbacks pulled from attributes when missing
//     (service.name, host.name, k8s.pod.name, deployment.environment) so the
//     CH MATERIALIZED columns always find something.
package enrich

import "strings"

const (
	zero16 = "00000000000000000000000000000000"
	zero8  = "0000000000000000"
)

// NormalizeSeverityText upper-cases and trims; on empty, derives a canonical
// token from severity_number per OTLP severity-number ranges.
func NormalizeSeverityText(text string, num uint32) string {
	t := strings.ToUpper(strings.TrimSpace(text))
	if t != "" {
		return t
	}
	switch {
	case num == 0:
		return "UNSET"
	case num <= 4:
		return "TRACE"
	case num <= 8:
		return "DEBUG"
	case num <= 12:
		return "INFO"
	case num <= 16:
		return "WARN"
	case num <= 20:
		return "ERROR"
	default:
		return "FATAL"
	}
}

// ZeroTraceID collapses an all-zero 32-hex-char trace id to empty.
func ZeroTraceID(id string) string {
	if id == zero16 {
		return ""
	}
	return id
}

// ZeroSpanID collapses an all-zero 16-hex-char span id to empty.
func ZeroSpanID(id string) string {
	if id == zero8 {
		return ""
	}
	return id
}

// FillResourceFallbacks pulls a handful of well-known attribute keys into the
// resource map so the CH MATERIALIZED columns (service, host, pod,
// environment) always find a value. Existing resource entries always win.
func FillResourceFallbacks(resource, attrs map[string]string) map[string]string {
	if resource == nil {
		resource = map[string]string{}
	}
	for _, k := range []string{"service.name", "host.name", "k8s.pod.name", "deployment.environment"} {
		if resource[k] != "" {
			continue
		}
		if v := attrs[k]; v != "" {
			resource[k] = v
		}
	}
	return resource
}
