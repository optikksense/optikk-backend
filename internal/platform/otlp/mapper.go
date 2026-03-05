package otlp

import (
	"encoding/json"
	"hash/fnv"
	"strconv"
	"time"

	"github.com/observability/observability-backend-go/internal/platform/ingest"
)

// spanKindString converts the OTel SpanKind enum to a human-readable string.
func spanKindString(kind int) string {
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

// statusCodeString converts the OTel span status code to a string.
func statusCodeString(code int) string {
	switch code {
	case 1:
		return "OK"
	case 2:
		return "ERROR"
	default:
		return "UNSET"
	}
}

// severityNumberToLevel converts an OTel severity number to a log level string.
func severityNumberToLevel(n int) string {
	switch {
	case n <= 0:
		return "UNSET"
	case n <= 4:
		return "TRACE"
	case n <= 8:
		return "DEBUG"
	case n <= 12:
		return "INFO"
	case n <= 16:
		return "WARN"
	case n <= 20:
		return "ERROR"
	default:
		return "FATAL"
	}
}

// nanoToTime converts a Unix nanosecond string to time.Time.
func nanoToTime(nanoStr string) time.Time {
	ns, err := strconv.ParseInt(nanoStr, 10, 64)
	if err != nil || ns == 0 {
		return time.Now()
	}
	return time.Unix(0, ns)
}

// attrsToMap converts a []KeyValue slice into a map[string]string.
func attrsToMap(kvs []KeyValue) map[string]string {
	m := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		m[kv.Key] = anyValueString(kv.Value)
	}
	return m
}

// anyValueString extracts the best string representation from an AnyValue.
func anyValueString(v AnyValue) string {
	if v.StringValue != nil {
		return *v.StringValue
	}
	if v.IntValue != nil {
		return strconv.FormatInt(*v.IntValue, 10)
	}
	if v.DoubleValue != nil {
		return strconv.FormatFloat(*v.DoubleValue, 'f', -1, 64)
	}
	if v.BoolValue != nil {
		if *v.BoolValue {
			return "true"
		}
		return "false"
	}
	return ""
}

// attrsToJSON serializes a []KeyValue to a compact JSON string for the
// `attributes` column (queried at read time via JSONExtractString).
func attrsToJSON(kvs []KeyValue) string {
	if len(kvs) == 0 {
		return "{}"
	}
	m := attrsToMap(kvs)
	b, err := json.Marshal(m)
	if err != nil {
		return "{}"
	}
	return string(b)
}

// resourceFingerprint hashes the resource attributes to a single stable UInt64.
func resourceFingerprint(kvs []KeyValue) uint64 {
	h := fnv.New64a()
	for _, kv := range kvs {
		h.Write([]byte(kv.Key))
		h.Write([]byte(anyValueString(kv.Value)))
	}
	return h.Sum64()
}

// temporalityString parses OTLP aggregation temporality safely from JSON.
func temporalityString(val any) string {
	if val == nil {
		return "Unspecified"
	}
	switch v := val.(type) {
	case string:
		if v == "" || v == "AGGREGATION_TEMPORALITY_UNSPECIFIED" {
			return "Unspecified"
		}
		if v == "AGGREGATION_TEMPORALITY_DELTA" || v == "Delta" || v == "1" {
			return "Delta"
		}
		if v == "AGGREGATION_TEMPORALITY_CUMULATIVE" || v == "Cumulative" || v == "2" {
			return "Cumulative"
		}
		return v
	case float64:
		if v == 1 {
			return "Delta"
		}
		if v == 2 {
			return "Cumulative"
		}
	}
	return "Unspecified"
}

// lookupAttr returns the string value of a named attribute from a []KeyValue slice.
func lookupAttr(kvs []KeyValue, key string) string {
	for _, kv := range kvs {
		if kv.Key == key {
			return anyValueString(kv.Value)
		}
	}
	return ""
}

// lookupAttrInt returns the int64 value of a named attribute.
func lookupAttrInt(kvs []KeyValue, key string) int64 {
	s := lookupAttr(kvs, key)
	if s == "" {
		return 0
	}
	v, _ := strconv.ParseInt(s, 10, 64)
	return v
}

// ── Spans mapper ──────────────────────────────────────────────────────────────

// SpanColumns is the ordered column list for observability.spans inserts.
var SpanColumns = []string{
	"team_id", "trace_id", "span_id", "parent_span_id",
	"parent_service_name", "is_root",
	"operation_name", "service_name", "span_kind",
	"start_time", "end_time", "duration_ms",
	"status", "status_message",
	"http_method", "http_url", "http_status_code",
	"host", "pod", "container",
	"attributes", "tags",
}

// MapSpans converts an OTLP trace export request into ingest rows.
func MapSpans(teamID string, req ExportTraceServiceRequest) []ingest.Row {
	rows := make([]ingest.Row, 0, 64)

	for _, rs := range req.ResourceSpans {
		resAttrs := rs.Resource.Attributes
		serviceName := lookupAttr(resAttrs, "service.name")
		host := lookupAttr(resAttrs, "host.name")
		pod := lookupAttr(resAttrs, "k8s.pod.name")
		container := lookupAttr(resAttrs, "k8s.container.name")

		for _, ss := range rs.ScopeSpans {
			for _, s := range ss.Spans {
				start := nanoToTime(s.StartTimeUnixNano)
				end := nanoToTime(s.EndTimeUnixNano)
				durMs := uint64(end.Sub(start).Milliseconds())
				if durMs == 0 && !end.IsZero() && !start.IsZero() {
					durMs = 0
				}

				isRoot := uint8(0)
				if s.ParentSpanID == "" {
					isRoot = 1
				}

				spanAttrs := s.Attributes
				httpMethod := lookupAttr(spanAttrs, "http.method")
				if httpMethod == "" {
					httpMethod = lookupAttr(spanAttrs, "http.request.method")
				}
				httpURL := lookupAttr(spanAttrs, "http.url")
				if httpURL == "" {
					httpURL = lookupAttr(spanAttrs, "url.full")
				}
				httpStatus := int32(lookupAttrInt(spanAttrs, "http.status_code"))
				if httpStatus == 0 {
					httpStatus = int32(lookupAttrInt(spanAttrs, "http.response.status_code"))
				}

				// Span host/pod/container can come from span attrs too.
				if host == "" {
					host = lookupAttr(spanAttrs, "host.name")
				}
				if pod == "" {
					pod = lookupAttr(spanAttrs, "k8s.pod.name")
				}
				if container == "" {
					container = lookupAttr(spanAttrs, "k8s.container.name")
				}

				attrsJSON := attrsToJSON(spanAttrs)
				tags := attrsToMap(resAttrs)
				// Merge span attributes into tags map.
				for k, v := range attrsToMap(spanAttrs) {
					tags[k] = v
				}

				rows = append(rows, ingest.Row{Values: []any{
					teamID,
					s.TraceID,
					s.SpanID,
					s.ParentSpanID,
					"", // parent_service_name — not available at ingest without lookup
					isRoot,
					s.Name,
					serviceName,
					spanKindString(s.Kind),
					start,
					end,
					durMs,
					statusCodeString(s.Status.Code),
					s.Status.Message,
					httpMethod,
					httpURL,
					httpStatus,
					host,
					pod,
					container,
					attrsJSON,
					tags,
				}})
			}
		}
	}

	return rows
}

// ── Logs mapper ───────────────────────────────────────────────────────────────

// LogColumns is the ordered column list for observability.logs inserts.
var LogColumns = []string{
	"team_id", "timestamp", "level", "service_name",
	"logger", "message", "trace_id", "span_id",
	"host", "pod", "container", "thread",
	"exception", "attributes", "tags",
}

// MapLogs converts an OTLP logs export request into ingest rows.
func MapLogs(teamID string, req ExportLogsServiceRequest) []ingest.Row {
	rows := make([]ingest.Row, 0, 64)

	for _, rl := range req.ResourceLogs {
		resAttrs := rl.Resource.Attributes
		serviceName := lookupAttr(resAttrs, "service.name")
		host := lookupAttr(resAttrs, "host.name")
		pod := lookupAttr(resAttrs, "k8s.pod.name")
		container := lookupAttr(resAttrs, "k8s.container.name")

		for _, sl := range rl.ScopeLogs {
			loggerName := sl.Scope.Name

			for _, lr := range sl.LogRecords {
				tsStr := lr.TimeUnixNano
				if tsStr == "" {
					tsStr = lr.ObservedTimeUnixNano
				}
				ts := nanoToTime(tsStr)

				level := lr.SeverityText
				if level == "" {
					level = severityNumberToLevel(lr.SeverityNumber)
				}

				body := ""
				if lr.Body.StringValue != nil {
					body = *lr.Body.StringValue
				}

				logAttrs := lr.Attributes
				thread := lookupAttr(logAttrs, "thread.name")
				exception := lookupAttr(logAttrs, "exception.message")

				attrsJSON := attrsToJSON(logAttrs)
				tags := attrsToMap(resAttrs)
				for k, v := range attrsToMap(logAttrs) {
					tags[k] = v
				}

				rows = append(rows, ingest.Row{Values: []any{
					teamID,
					ts,
					level,
					serviceName,
					loggerName,
					body,
					lr.TraceID,
					lr.SpanID,
					host,
					pod,
					container,
					thread,
					exception,
					attrsJSON,
					tags,
				}})
			}
		}
	}

	return rows
}

// ── Metrics mapper ────────────────────────────────────────────────────────────

// MetricColumns is the ordered column list for observability.metrics inserts.
var MetricColumns = []string{
	"team_id", "env", "metric_name", "metric_type", "temporality", "is_monotonic",
	"unit", "description", "resource_fingerprint", "timestamp", "value",
	"hist_sum", "hist_count", "hist_buckets", "hist_counts", "attributes",
}

// MapMetrics converts an OTLP metrics export request into ingest rows.
// Each data point emits one row in the metrics_v5 unified schema structure.
func MapMetrics(teamID string, req ExportMetricsServiceRequest) []ingest.Row {
	rows := make([]ingest.Row, 0, 128)

	for _, rm := range req.ResourceMetrics {
		resAttrs := rm.Resource.Attributes
		env := lookupAttr(resAttrs, "deployment.environment")
		if env == "" {
			env = "default"
		}
		fingerprint := resourceFingerprint(resAttrs)

		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {

				unit := m.Unit
				desc := m.Description

				// Gauge data points.
				if m.Gauge != nil {
					for _, dp := range m.Gauge.DataPoints {
						val := numberDataPointValue(dp)
						attrsJSON := attrsToJSON(append(resAttrs, dp.Attributes...))

						rows = append(rows, ingest.Row{Values: []any{
							teamID, env, m.Name, "Gauge", "Unspecified", false,
							unit, desc, fingerprint, nanoToTime(dp.TimeUnixNano), val,
							0.0, uint64(0), []float64{}, []uint64{}, attrsJSON,
						}})
					}
				}

				// Sum data points.
				if m.Sum != nil {
					temporality := temporalityString(m.Sum.AggregationTemporality)
					isMonotonic := m.Sum.IsMonotonic
					for _, dp := range m.Sum.DataPoints {
						val := numberDataPointValue(dp)
						attrsJSON := attrsToJSON(append(resAttrs, dp.Attributes...))

						rows = append(rows, ingest.Row{Values: []any{
							teamID, env, m.Name, "Sum", temporality, isMonotonic,
							unit, desc, fingerprint, nanoToTime(dp.TimeUnixNano), val,
							0.0, uint64(0), []float64{}, []uint64{}, attrsJSON,
						}})
					}
				}

				// Histogram data points.
				if m.Histogram != nil {
					temporality := temporalityString(m.Histogram.AggregationTemporality)
					for _, dp := range m.Histogram.DataPoints {
						count := dp.Count
						sum := 0.0
						if dp.Sum != nil {
							sum = *dp.Sum
						}

						avg := 0.0
						if count > 0 {
							avg = sum / float64(count)
						}

						bounds := dp.ExplicitBounds
						if bounds == nil {
							bounds = []float64{}
						}
						counts := dp.BucketCounts
						if counts == nil {
							counts = []uint64{}
						}

						attrsJSON := attrsToJSON(append(resAttrs, dp.Attributes...))

						rows = append(rows, ingest.Row{Values: []any{
							teamID, env, m.Name, "Histogram", temporality, false,
							unit, desc, fingerprint, nanoToTime(dp.TimeUnixNano), avg,
							sum, count, bounds, counts, attrsJSON,
						}})
					}
				}
			}
		}
	}

	return rows
}

// numberDataPointValue returns the numeric value of a NumberDataPoint.
func numberDataPointValue(dp NumberDataPoint) float64 {
	if dp.AsDouble != nil {
		return *dp.AsDouble
	}
	if dp.AsInt != nil {
		return float64(*dp.AsInt)
	}
	return 0
}

// histogramP95 approximates the p95 value from a histogram data point by finding
// the bucket that contains the 95th percentile cumulative count.
func histogramP95(dp HistogramDataPoint) float64 {
	if dp.Count == 0 || len(dp.BucketCounts) == 0 {
		return 0
	}
	target := uint64(float64(dp.Count) * 0.95)
	var cumulative uint64
	for i, c := range dp.BucketCounts {
		cumulative += c
		if cumulative >= target {
			if i < len(dp.ExplicitBounds) {
				return dp.ExplicitBounds[i]
			}
			// Last bucket has no upper bound — use max if available.
			if dp.Max != nil {
				return *dp.Max
			}
			return 0
		}
	}
	return 0
}
