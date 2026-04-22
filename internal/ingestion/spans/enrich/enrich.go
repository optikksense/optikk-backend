// Package enrich holds the pure normalization pass between the OTLP span
// mapper and the Kafka producer. Keeps I/O out so the same function runs at
// ingest time and during reprocessing / backfill tools.
//
// Rules:
//   - service.name, host.name, k8s.pod.name, deployment.environment get
//     resource fallbacks pulled from attributes when missing (parity with the
//     CH MATERIALIZED columns on observability.spans_v2).
//   - zero-valued trace_id / span_id / parent_span_id get collapsed to ""
//     so the CH bloom filter on these columns is not polluted.
//   - exception.* attrs on an error span are promoted to first-class
//     columns (exception_type / message / stacktrace) for the write path.
package enrich

import "strings"

const (
	zero16 = "00000000000000000000000000000000"
	zero8  = "0000000000000000"
)

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
// resource map so CH MATERIALIZED columns always find a value.
func FillResourceFallbacks(resource, attrs map[string]string) map[string]string {
	if resource == nil {
		resource = map[string]string{}
	}
	for _, k := range []string{"service.name", "host.name", "k8s.pod.name", "k8s.namespace.name", "deployment.environment", "service.version"} {
		if resource[k] != "" {
			continue
		}
		if v := attrs[k]; v != "" {
			resource[k] = v
		}
	}
	return resource
}

// FirstNonEmpty returns the first non-empty attribute value for the listed
// keys, or "". Useful for deriving an endpoint token from http.route,
// url.path, http.target — same order used by the CH materialized columns.
func FirstNonEmpty(m map[string]string, keys ...string) string {
	for _, k := range keys {
		if v := m[k]; v != "" {
			return v
		}
	}
	return ""
}

// KindString normalizes a protobuf span kind int (0..5) to the canonical
// upper-case token the CH `kind_string` column stores.
func KindString(kind int32) string {
	switch kind {
	case 1:
		return "INTERNAL"
	case 2:
		return "SERVER"
	case 3:
		return "CLIENT"
	case 4:
		return "PRODUCER"
	case 5:
		return "CONSUMER"
	default:
		return "UNSPECIFIED"
	}
}

// StatusCodeString maps the protobuf status_code int (0..2) to a canonical
// token. CH stores this in `status_code_string`.
func StatusCodeString(code int32) string {
	switch code {
	case 1:
		return "OK"
	case 2:
		return "ERROR"
	default:
		return "UNSET"
	}
}

// ExceptionFromEvents walks the events column (already JSON-serialised) for
// an exception event and pulls out type/message/stacktrace. Returns empty
// strings when no exception event exists. Used by the mapper to populate
// the first-class exception_* columns without re-parsing on query time.
func ExceptionFromEvents(events []string) (exType, exMsg, exStack string) {
	// Events are stored as JSON strings in the proto; the mapper parses the
	// canonical ones (name == "exception") into the dedicated columns. This
	// helper exists so the indexer + backfill tools share one extraction rule.
	for _, ev := range events {
		if !strings.Contains(ev, `"exception`) {
			continue
		}
		if exType == "" {
			exType = between(ev, `"exception.type":"`, `"`)
		}
		if exMsg == "" {
			exMsg = between(ev, `"exception.message":"`, `"`)
		}
		if exStack == "" {
			exStack = between(ev, `"exception.stacktrace":"`, `"`)
		}
	}
	return exType, exMsg, exStack
}

func between(s, start, end string) string {
	i := strings.Index(s, start)
	if i < 0 {
		return ""
	}
	s = s[i+len(start):]
	j := strings.Index(s, end)
	if j < 0 {
		return ""
	}
	return s[:j]
}
