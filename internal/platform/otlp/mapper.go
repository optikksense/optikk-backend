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

// parseNano converts a Unix nanosecond string to int64.
func parseNano(nanoStr string) int64 {
	ns, err := strconv.ParseInt(nanoStr, 10, 64)
	if err != nil {
		return 0
	}
	return ns
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

// TraceIngestRows groups span and resource rows emitted from a trace export.
type TraceIngestRows struct {
	Spans     []ingest.Row
	Resources []ingest.Row
}

// SpanColumns is the ordered column list for observability.spans inserts (optimized schema).
var SpanColumns = []string{
	"ts_bucket_start", "resource_fingerprint", "team_id",
	"timestamp", "trace_id", "span_id", "parent_span_id", "trace_state", "flags",
	"name", "kind", "kind_string", "duration_nano", "has_error", "is_remote",
	"status_code", "status_code_string", "status_message",
	"http_url", "http_method", "http_host", "external_http_url", "external_http_method",
	"response_status_code",
	"attributes", "events", "links",
	"exception_type", "exception_message", "exception_stacktrace", "exception_escaped",
}

// ResourceColumns is the ordered column list for observability.resources inserts.
var ResourceColumns = []string{
	"fingerprint", "team_id", "service_name", "host_name", "k8s_pod_name", "labels",
}

func resourceRow(teamID, fingerprint string, attrs []KeyValue) ingest.Row {
	return ingest.Row{Values: []any{
		fingerprint,
		teamID,
		lookupAttr(attrs, "service.name"),
		lookupAttr(attrs, "host.name"),
		lookupAttr(attrs, "k8s.pod.name"),
		attrsToJSON(attrs),
	}}
}

// MapTraceRows converts an OTLP trace export request into span and resource ingest rows.
func MapTraceRows(teamID string, req ExportTraceServiceRequest) TraceIngestRows {
	result := TraceIngestRows{
		Spans:     make([]ingest.Row, 0, 64),
		Resources: make([]ingest.Row, 0, len(req.ResourceSpans)),
	}
	resourceIndexes := make(map[string]int, len(req.ResourceSpans))

	for _, rs := range req.ResourceSpans {
		resAttrs := rs.Resource.Attributes
		resFp := strconv.FormatUint(resourceFingerprint(resAttrs), 16)
		hasSpan := false

		for _, ss := range rs.ScopeSpans {
			for _, s := range ss.Spans {
				timestamp := nanoToTime(s.StartTimeUnixNano)
				startNano := parseNano(s.StartTimeUnixNano)
				endNano := parseNano(s.EndTimeUnixNano)
				durNano := uint64(0)
				if endNano > startNano {
					durNano = uint64(endNano - startNano)
				}

				tsBucket := uint64(timestamp.Unix() / 300 * 300)
				hasSpan = true

				// Determine has_error from status code
				hasError := s.Status.Code == 2 // ERROR = 2

				// Extract HTTP attributes
				spanAttrs := s.Attributes
				httpMethod := lookupAttr(spanAttrs, "http.method")
				if httpMethod == "" {
					httpMethod = lookupAttr(spanAttrs, "http.request.method")
				}
				httpURL := lookupAttr(spanAttrs, "http.url")
				if httpURL == "" {
					httpURL = lookupAttr(spanAttrs, "url.full")
				}
				httpHost := lookupAttr(spanAttrs, "http.host")
				if httpHost == "" {
					httpHost = lookupAttr(spanAttrs, "net.host.name")
				}
				httpStatusCode := lookupAttr(spanAttrs, "http.status_code")
				if httpStatusCode == "" {
					httpStatusCode = lookupAttr(spanAttrs, "http.response.status_code")
				}

				// External HTTP (for CLIENT spans)
				externalHTTPURL := ""
				externalHTTPMethod := ""
				if s.Kind == 3 { // CLIENT
					externalHTTPURL = lookupAttr(spanAttrs, "http.url")
					if externalHTTPURL == "" {
						externalHTTPURL = lookupAttr(spanAttrs, "url.full")
					}
					externalHTTPMethod = httpMethod
				}

				// DB attributes
				dbName := lookupAttr(spanAttrs, "db.name")
				dbOperation := lookupAttr(spanAttrs, "db.operation")

				// Exception attributes
				exceptionType := lookupAttr(spanAttrs, "exception.type")
				exceptionMessage := lookupAttr(spanAttrs, "exception.message")
				exceptionStacktrace := lookupAttr(spanAttrs, "exception.stacktrace")
				exceptionEscaped := lookupAttr(spanAttrs, "exception.escaped") == "true"

				allAttrs := make([]KeyValue, 0, len(resAttrs)+len(spanAttrs))
				allAttrs = append(allAttrs, resAttrs...)
				allAttrs = append(allAttrs, spanAttrs...)
				attrsJSON := attrsToJSON(allAttrs)

				// Events column is Array(String), so pass native []string values.
				eventNames := make([]string, 0, len(s.Events))
				for _, e := range s.Events {
					eventNames = append(eventNames, e.Name)
				}

				// Links (simplified - store as JSON string)
				linksJSON := "[]"

				result.Spans = append(result.Spans, ingest.Row{Values: []any{
					tsBucket,
					resFp,
					teamID,
					timestamp,
					s.TraceID,
					s.SpanID,
					s.ParentSpanID,
					"",        // trace_state
					uint32(0), // flags
					s.Name,
					int8(s.Kind),
					spanKindString(s.Kind),
					durNano,
					hasError,
					"", // is_remote
					int16(s.Status.Code),
					statusCodeString(s.Status.Code),
					s.Status.Message,
					httpURL,
					httpMethod,
					httpHost,
					externalHTTPURL,
					externalHTTPMethod,
					httpStatusCode,
					dbName,
					dbOperation,
					attrsJSON,
					eventNames,
					linksJSON,
					exceptionType,
					exceptionMessage,
					exceptionStacktrace,
					exceptionEscaped,
				}})
			}
		}

		if len(resAttrs) == 0 || !hasSpan {
			continue
		}

		if _, ok := resourceIndexes[resFp]; ok {
			continue
		}

		resourceIndexes[resFp] = len(result.Resources)
		result.Resources = append(result.Resources, resourceRow(teamID, resFp, resAttrs))
	}

	return result
}

// MapSpans converts an OTLP trace export request into span ingest rows.
func MapSpans(teamID string, req ExportTraceServiceRequest) []ingest.Row {
	return MapTraceRows(teamID, req).Spans
}

// ── Logs mapper ───────────────────────────────────────────────────────────────

// LogColumns is the ordered column list for observability.logs inserts.
var LogColumns = []string{
	"team_id", "ts_bucket_start", "timestamp", "observed_timestamp",
	"id", "trace_id", "span_id", "trace_flags",
	"severity_text", "severity_number", "body",
	"attributes_string", "attributes_number", "attributes_bool",
	"resource", "resource_fingerprint",
	"scope_name", "scope_version", "scope_string",
}

// attrsToTypedMaps splits a []KeyValue into typed maps by AnyValue variant.
func attrsToTypedMaps(kvs []KeyValue) (map[string]string, map[string]float64, map[string]bool) {
	sm := make(map[string]string, len(kvs))
	nm := make(map[string]float64)
	bm := make(map[string]bool)
	for _, kv := range kvs {
		switch {
		case kv.Value.StringValue != nil:
			sm[kv.Key] = *kv.Value.StringValue
		case kv.Value.IntValue != nil:
			nm[kv.Key] = float64(*kv.Value.IntValue)
		case kv.Value.DoubleValue != nil:
			nm[kv.Key] = *kv.Value.DoubleValue
		case kv.Value.BoolValue != nil:
			bm[kv.Key] = *kv.Value.BoolValue
		case kv.Value.BytesValue != nil:
			sm[kv.Key] = *kv.Value.BytesValue
		}
	}
	return sm, nm, bm
}

// nanoStrToUint64 parses a Unix nanosecond string to uint64, falling back to now.
func nanoStrToUint64(s string) uint64 {
	ns, err := strconv.ParseUint(s, 10, 64)
	if err != nil || ns == 0 {
		return uint64(time.Now().UnixNano())
	}
	return ns
}

// logID generates a stable FNV-64a ID for a log record.
func logID(teamID string, tsNano uint64, lr LogRecord) string {
	h := fnv.New64a()
	h.Write([]byte(teamID))
	h.Write([]byte{0})
	b := strconv.AppendUint(nil, tsNano, 10)
	h.Write(b)
	h.Write([]byte{0})
	h.Write([]byte(lr.TraceID))
	h.Write([]byte{0})
	h.Write([]byte(lr.SpanID))
	h.Write([]byte{0})
	if lr.Body.StringValue != nil {
		h.Write([]byte(*lr.Body.StringValue))
	}
	return strconv.FormatUint(h.Sum64(), 16)
}

// MapLogs converts an OTLP logs export request into ingest rows.
func MapLogs(teamID string, req ExportLogsServiceRequest) []ingest.Row {
	rows := make([]ingest.Row, 0, 64)

	for _, rl := range req.ResourceLogs {
		resAttrs := rl.Resource.Attributes
		resourceJSON := attrsToJSON(resAttrs)
		fingerprint := strconv.FormatUint(resourceFingerprint(resAttrs), 16)

		for _, sl := range rl.ScopeLogs {
			scopeName := sl.Scope.Name
			scopeVersion := sl.Scope.Version
			scopeAttrs := map[string]string{}
			if scopeName != "" {
				scopeAttrs["name"] = scopeName
			}

			for _, lr := range sl.LogRecords {
				tsStr := lr.TimeUnixNano
				if tsStr == "" {
					tsStr = lr.ObservedTimeUnixNano
				}
				tsNano := nanoStrToUint64(tsStr)
				observedNano := nanoStrToUint64(lr.ObservedTimeUnixNano)

				// ts_bucket_start: truncate to day boundary (seconds)
				tsSec := tsNano / 1_000_000_000
				tsBucket := uint32(tsSec - tsSec%86400)

				severityText := lr.SeverityText
				if severityText == "" {
					severityText = severityNumberToLevel(lr.SeverityNumber)
				}

				body := ""
				if lr.Body.StringValue != nil {
					body = *lr.Body.StringValue
				}

				attrStr, attrNum, attrBool := attrsToTypedMaps(lr.Attributes)
				id := logID(teamID, tsNano, lr)

				rows = append(rows, ingest.Row{Values: []any{
					teamID,
					tsBucket,
					tsNano,
					observedNano,
					id,
					lr.TraceID,
					lr.SpanID,
					lr.Flags,
					severityText,
					uint8(lr.SeverityNumber),
					body,
					attrStr,
					attrNum,
					attrBool,
					resourceJSON,
					fingerprint,
					scopeName,
					scopeVersion,
					scopeAttrs,
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
