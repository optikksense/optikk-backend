package spans

import (
	"log/slog"

	"github.com/Optikk-Org/optikk-backend/internal/infra/otlp"
	trace "go.opentelemetry.io/proto/otlp/trace/v1"
)

const maxSpanAttributes = 128

type httpFields struct {
	url, method, host, statusCode string
	externalURL, externalMethod   string
}

type exceptionFields struct {
	typ, message, stacktrace string
	escaped                  bool
}

// mergeAndCapAttrs merges resource + span attrs (span keys win), and truncates
// the total to maxSpanAttributes. Truncation is iteration-order dependent but
// that is acceptable — over-limit attribute sets are treated as a bug in the
// emitter, and the cap is a safety net, not a selection policy.
func mergeAndCapAttrs(teamID int64, spanID []byte, resMap, spanMap map[string]string) map[string]string {
	merged := make(map[string]string, len(resMap)+len(spanMap))
	for k, v := range resMap {
		merged[k] = v
	}
	for k, v := range spanMap {
		merged[k] = v
	}
	if len(merged) <= maxSpanAttributes {
		return merged
	}
	slog.Warn("ingest: span attributes truncated",
		slog.Int("from", len(merged)), slog.Int("to", maxSpanAttributes),
		slog.Int64("team_id", teamID), slog.String("span_id", otlp.BytesToHex(spanID)))
	trimmed := make(map[string]string, maxSpanAttributes)
	i := 0
	for k, v := range merged {
		trimmed[k] = v
		i++
		if i == maxSpanAttributes {
			break
		}
	}
	return trimmed
}

// extractHTTPFields pulls HTTP semantic-convention attrs; externalURL/Method are
// populated only for CLIENT-kind spans (consumer of the remote HTTP call).
func extractHTTPFields(spanMap map[string]string, kind trace.Span_SpanKind) httpFields {
	h := httpFields{
		method:     mapGet(spanMap, "http.method", "http.request.method"),
		url:        mapGet(spanMap, "http.url", "url.full"),
		host:       mapGet(spanMap, "http.host", "net.host.name"),
		statusCode: mapGet(spanMap, "http.status_code", "http.response.status_code"),
	}
	if kind == trace.Span_SPAN_KIND_CLIENT {
		h.externalURL = h.url
		h.externalMethod = h.method
	}
	return h
}

// extractExceptionFields pulls OTel exception-event attrs for fast ERROR search.
func extractExceptionFields(spanMap map[string]string) exceptionFields {
	return exceptionFields{
		typ:        spanMap["exception.type"],
		message:    spanMap["exception.message"],
		stacktrace: spanMap["exception.stacktrace"],
		escaped:    spanMap["exception.escaped"] == "true",
	}
}

// mapGet returns the first non-empty value among keys.
func mapGet(m map[string]string, keys ...string) string {
	for _, k := range keys {
		if v := m[k]; v != "" {
			return v
		}
	}
	return ""
}
