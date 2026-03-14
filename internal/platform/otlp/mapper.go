package otlp

import (
	"encoding/json"
	"hash/fnv"
	"strconv"
	"time"

	"github.com/observability/observability-backend-go/internal/platform/ingest"
	"github.com/observability/observability-backend-go/internal/platform/timebucket"
)

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

func nanoToTime(nanoStr string) time.Time {
	ns, err := strconv.ParseInt(nanoStr, 10, 64)
	if err != nil || ns == 0 {
		return time.Now()
	}
	return time.Unix(0, ns)
}

func parseNano(nanoStr string) int64 {
	ns, err := strconv.ParseInt(nanoStr, 10, 64)
	if err != nil {
		return 0
	}
	return ns
}

func attrsToMap(kvs []KeyValue) map[string]string {
	m := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		m[kv.Key] = anyValueString(kv.Value)
	}
	return m
}

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
	return mapToJSON(m)
}

// mapToJSON serializes a map[string]string to a compact JSON string.
func mapToJSON(m map[string]string) string {
	if len(m) == 0 {
		return "{}"
	}
	b, err := json.Marshal(m)
	if err != nil {
		return "{}"
	}
	return string(b)
}

// mergeAttrsJSON merges a pre-built resource map with datapoint KeyValue attrs
// and serializes to JSON. Avoids the temporary slice allocation from append().
func mergeAttrsJSON(resMap map[string]string, dpAttrs []KeyValue) string {
	if len(dpAttrs) == 0 {
		return mapToJSON(resMap)
	}
	merged := make(map[string]string, len(resMap)+len(dpAttrs))
	for k, v := range resMap {
		merged[k] = v
	}
	for _, kv := range dpAttrs {
		merged[kv.Key] = anyValueString(kv.Value)
	}
	return mapToJSON(merged)
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

func lookupAttr(kvs []KeyValue, key string) string {
	for _, kv := range kvs {
		if kv.Key == key {
			return anyValueString(kv.Value)
		}
	}
	return ""
}

func lookupAttrInt(kvs []KeyValue, key string) int64 {
	s := lookupAttr(kvs, key)
	if s == "" {
		return 0
	}
	v, _ := strconv.ParseInt(s, 10, 64)
	return v
}


var SpanColumns = []string{
	"ts_bucket_start", "team_id",
	"timestamp", "trace_id", "span_id", "parent_span_id", "trace_state", "flags",
	"name", "kind", "kind_string", "duration_nano", "has_error", "is_remote",
	"status_code", "status_code_string", "status_message",
	"http_url", "http_method", "http_host", "external_http_url", "external_http_method",
	"response_status_code",
	"attributes", "events", "links",
	"exception_type", "exception_message", "exception_stacktrace", "exception_escaped",
}

func MapSpans(teamID int64, req ExportTraceServiceRequest) []ingest.Row {
	result := make([]ingest.Row, 0, 64)

	for _, rs := range req.ResourceSpans {
		resAttrs := rs.Resource.Attributes
		// Build resource attribute map once per resource (shared across all spans).
		resMap := attrsToMap(resAttrs)

		for _, ss := range rs.ScopeSpans {
			for _, s := range ss.Spans {
				timestamp := nanoToTime(s.StartTimeUnixNano)
				startNano := parseNano(s.StartTimeUnixNano)
				endNano := parseNano(s.EndTimeUnixNano)
				durNano := uint64(0)
				if endNano > startNano {
					durNano = uint64(endNano - startNano)
				}

				tsBucket := timebucket.SpansBucketStart(timestamp.Unix())

				// Determine has_error from status code
				hasError := s.Status.Code == 2 // ERROR = 2

				// Build a merged attribute map: resource + span attrs.
				// This single map is used for all O(1) lookups AND JSON serialization.
				spanMap := attrsToMap(s.Attributes)
				mergedMap := make(map[string]string, len(resMap)+len(spanMap))
				for k, v := range resMap {
					mergedMap[k] = v
				}
				for k, v := range spanMap {
					mergedMap[k] = v // span attrs override resource attrs
				}

				// O(1) lookups from the merged map
				httpMethod := mapGet(spanMap, "http.method", "http.request.method")
				httpURL := mapGet(spanMap, "http.url", "url.full")
				httpHost := mapGet(spanMap, "http.host", "net.host.name")
				httpStatusCode := mapGet(spanMap, "http.status_code", "http.response.status_code")

				// External HTTP (for CLIENT spans)
				externalHTTPURL := ""
				externalHTTPMethod := ""
				if s.Kind == 3 { // CLIENT
					externalHTTPURL = httpURL
					externalHTTPMethod = httpMethod
				}

				// Exception attributes
				exceptionType := spanMap["exception.type"]
				exceptionMessage := spanMap["exception.message"]
				exceptionStacktrace := spanMap["exception.stacktrace"]
				exceptionEscaped := spanMap["exception.escaped"] == "true"

				// Serialize merged map to JSON (single pass, no redundant attrsToMap call).
				attrsJSON := mapToJSON(mergedMap)

				// Events column is Array(String). Each element is a JSON object
			// containing the event name, timestamp, and attributes.
			eventJSONs := make([]string, 0, len(s.Events))
			for _, e := range s.Events {
				ev := map[string]any{"name": e.Name}
				if e.TimeUnixNano != "" {
					ev["timeUnixNano"] = e.TimeUnixNano
				}
				if len(e.Attributes) > 0 {
					ev["attributes"] = attrsToMap(e.Attributes)
				}
				b, err := json.Marshal(ev)
				if err == nil {
					eventJSONs = append(eventJSONs, string(b))
				}
			}

				// Links (simplified - store as JSON string)
				linksJSON := "[]"

				result = append(result, ingest.Row{Values: []any{
					tsBucket,
					uint32(teamID),
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
					false, // is_remote
					int16(s.Status.Code),
					statusCodeString(s.Status.Code),
					s.Status.Message,
					httpURL,
					httpMethod,
					httpHost,
					externalHTTPURL,
					externalHTTPMethod,
					httpStatusCode,
					attrsJSON,
					eventJSONs,
					linksJSON,
					exceptionType,
					exceptionMessage,
					exceptionStacktrace,
					exceptionEscaped,
				}})
			}
		}
	}

	return result
}

// mapGet returns the value of the first non-empty key from the map (fallback chain).
func mapGet(m map[string]string, keys ...string) string {
	for _, k := range keys {
		if v := m[k]; v != "" {
			return v
		}
	}
	return ""
}


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

func nanoStrToUint64(s string) uint64 {
	ns, err := strconv.ParseUint(s, 10, 64)
	if err != nil || ns == 0 {
		return uint64(time.Now().UnixNano())
	}
	return ns
}

func logID(teamID int64, tsNano uint64, lr LogRecord) string {
	h := fnv.New64a()
	h.Write([]byte(strconv.FormatInt(teamID, 10)))
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

func MapLogs(teamID int64, req ExportLogsServiceRequest) []ingest.Row {
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
				tsBucket := timebucket.LogsBucketStart(int64(tsNano / 1_000_000_000))

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
					uint32(teamID),
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


var MetricColumns = []string{
	"team_id", "env", "metric_name", "metric_type", "temporality", "is_monotonic",
	"unit", "description", "resource_fingerprint", "timestamp", "value",
	"hist_sum", "hist_count", "hist_buckets", "hist_counts", "attributes",
}

func MapMetrics(teamID int64, req ExportMetricsServiceRequest) []ingest.Row {
	rows := make([]ingest.Row, 0, 128)

	for _, rm := range req.ResourceMetrics {
		resAttrs := rm.Resource.Attributes
		resMap := attrsToMap(resAttrs)
		env := resMap["deployment.environment"]
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
						attrsJSON := mergeAttrsJSON(resMap, dp.Attributes)

						rows = append(rows, ingest.Row{Values: []any{
							uint32(teamID), env, m.Name, "Gauge", "Unspecified", false,
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
						attrsJSON := mergeAttrsJSON(resMap, dp.Attributes)

						rows = append(rows, ingest.Row{Values: []any{
							uint32(teamID), env, m.Name, "Sum", temporality, isMonotonic,
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

						attrsJSON := mergeAttrsJSON(resMap, dp.Attributes)

						rows = append(rows, ingest.Row{Values: []any{
							uint32(teamID), env, m.Name, "Histogram", temporality, false,
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
