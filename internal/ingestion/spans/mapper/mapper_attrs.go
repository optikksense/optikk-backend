package mapper

import (
	"strconv"

	obsmetrics "github.com/Optikk-Org/optikk-backend/internal/infra/metrics"
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

// mergeAndCapAttrs merges resource + span attrs (span keys win) and trims to maxSpanAttributes.
func mergeAndCapAttrs(resMap, spanMap map[string]string) map[string]string {
	merged := make(map[string]string, len(resMap)+len(spanMap))
	for k, v := range resMap {
		merged[k] = v
	}
	for k, v := range spanMap {
		merged[k] = v
	}
	if dropped := otlp.CapStringMap(merged, maxSpanAttributes); dropped > 0 {
		obsmetrics.MapperAttrsDropped.WithLabelValues("spans").Add(float64(dropped))
	}
	return merged
}

// extractHTTPFields pulls HTTP semantic-convention attrs; externalURL/Method only for CLIENT-kind spans.
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

// promotedKeys are OTLP attribute keys mapped to dedicated top-level CH columns and stripped from the attribute map post-extraction.
var promotedKeys = []string{
	"http.method", "http.request.method",
	"http.url", "url.full",
	"http.host", "net.host.name",
	"http.status_code", "http.response.status_code",
	"exception.type", "exception.message", "exception.stacktrace", "exception.escaped",
	"service.name",
	"host.name",
	"k8s.pod.name",
	"service.version",
	"deployment.environment",
	"peer.service",
	"db.system",
	"db.name",
	"db.statement",
	"http.route",
}

func stripPromotedKeys(m map[string]string) {
	for _, k := range promotedKeys {
		delete(m, k)
	}
}

// httpStatusBucket bins an HTTP response status into the same coarse classes the schema used to compute MATERIALIZED-side: 5xx / 4xx / 3xx / 2xx, then "err" if the span carries an error, else "other".
func httpStatusBucket(statusCode string, hasError bool) string {
	if statusCode != "" {
		if n, err := strconv.ParseUint(statusCode, 10, 16); err == nil {
			switch {
			case n >= 500:
				return "5xx"
			case n >= 400:
				return "4xx"
			case n >= 300:
				return "3xx"
			case n >= 200:
				return "2xx"
			}
		}
	}
	if hasError {
		return "err"
	}
	return "other"
}
