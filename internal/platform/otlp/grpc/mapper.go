package grpc

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"strconv"
	"time"

	"github.com/observability/observability-backend-go/internal/platform/ingest"
	"github.com/observability/observability-backend-go/internal/platform/timebucket"
	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	log "go.opentelemetry.io/proto/otlp/logs/v1"
	metricsdatapb "go.opentelemetry.io/proto/otlp/metrics/v1"
	trace "go.opentelemetry.io/proto/otlp/trace/v1"
)

func nanoToTime(ns uint64) time.Time {
	if ns == 0 {
		return time.Now()
	}
	return time.Unix(0, int64(ns))
}

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

func severityNumberToLevel(n log.SeverityNumber) string {
	v := int(n)
	switch {
	case v <= 0:
		return "UNSET"
	case v <= 4:
		return "TRACE"
	case v <= 8:
		return "DEBUG"
	case v <= 12:
		return "INFO"
	case v <= 16:
		return "WARN"
	case v <= 20:
		return "ERROR"
	default:
		return "FATAL"
	}
}

func anyValueString(v *commonpb.AnyValue) string {
	if v == nil {
		return ""
	}
	switch val := v.Value.(type) {
	case *commonpb.AnyValue_StringValue:
		return val.StringValue
	case *commonpb.AnyValue_IntValue:
		return strconv.FormatInt(val.IntValue, 10)
	case *commonpb.AnyValue_DoubleValue:
		return strconv.FormatFloat(val.DoubleValue, 'f', -1, 64)
	case *commonpb.AnyValue_BoolValue:
		if val.BoolValue {
			return "true"
		}
		return "false"
	case *commonpb.AnyValue_BytesValue:
		return string(val.BytesValue) // Not ideal, but base64 or raw string is best effort
	default:
		return ""
	}
}

func attrsToMap(kvs []*commonpb.KeyValue) map[string]string {
	m := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		m[kv.Key] = anyValueString(kv.Value)
	}
	return m
}

// attrsToJSON serializes protobuf attributes into a JSON string map.
func attrsToJSON(kvs []*commonpb.KeyValue) string {
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

// mergeAttrsJSON merges a pre-built resource map with datapoint protobuf attrs
// and serializes to JSON. Avoids temporary slice allocation from append().
func mergeAttrsJSON(resMap map[string]string, dpAttrs []*commonpb.KeyValue) string {
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

// mapGet returns the value of the first non-empty key from the map (fallback chain).
func mapGet(m map[string]string, keys ...string) string {
	for _, k := range keys {
		if v := m[k]; v != "" {
			return v
		}
	}
	return ""
}

// resourceFingerprint hashes the resource attributes to a single stable UInt64.
func resourceFingerprint(kvs []*commonpb.KeyValue) uint64 {
	h := fnv.New64a()
	for _, kv := range kvs {
		h.Write([]byte(kv.Key))
		h.Write([]byte(anyValueString(kv.Value)))
	}
	return h.Sum64()
}

func lookupAttr(kvs []*commonpb.KeyValue, key string) string {
	for _, kv := range kvs {
		if kv.Key == key {
			return anyValueString(kv.Value)
		}
	}
	return ""
}

func lookupAttrInt(kvs []*commonpb.KeyValue, key string) int64 {
	s := lookupAttr(kvs, key)
	if s == "" {
		return 0
	}
	v, _ := strconv.ParseInt(s, 10, 64)
	return v
}

func bytesToHex(b []byte) string {
	if len(b) > 0 {
		return fmt.Sprintf("%x", b)
	}
	return ""
}

func MapSpans(teamID int64, req *tracepb.ExportTraceServiceRequest) []ingest.Row {
	result := make([]ingest.Row, 0, 64)

	for _, rs := range req.ResourceSpans {
		var resAttrs []*commonpb.KeyValue
		if rs.Resource != nil {
			resAttrs = rs.Resource.Attributes
		}
		// Build resource attribute map once per resource (shared across all spans).
		resMap := attrsToMap(resAttrs)

		for _, ss := range rs.ScopeSpans {
			for _, s := range ss.Spans {
				timestamp := nanoToTime(s.StartTimeUnixNano)
				durNano := uint64(0)
				if s.EndTimeUnixNano > s.StartTimeUnixNano {
					durNano = s.EndTimeUnixNano - s.StartTimeUnixNano
				}

				tsBucket := timebucket.SpansBucketStart(timestamp.Unix())

				// Determine has_error from status code
				statusMsg, statusCode := "", trace.Status_STATUS_CODE_UNSET
				if s.Status != nil {
					statusMsg = s.Status.Message
					statusCode = s.Status.Code
				}
				hasError := statusCode == trace.Status_STATUS_CODE_ERROR

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

				// O(1) lookups from the span map
				httpMethod := mapGet(spanMap, "http.method", "http.request.method")
				httpURL := mapGet(spanMap, "http.url", "url.full")
				httpHost := mapGet(spanMap, "http.host", "net.host.name")
				httpStatusCode := mapGet(spanMap, "http.status_code", "http.response.status_code")

				// External HTTP (for CLIENT spans)
				externalHTTPURL := ""
				externalHTTPMethod := ""
				if s.Kind == trace.Span_SPAN_KIND_CLIENT {
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

				// Events column is Array(String), so pass native []string values.
				eventNames := make([]string, 0, len(s.Events))
				for _, e := range s.Events {
					eventNames = append(eventNames, e.Name)
				}

				// Links (simplified - store as JSON string)
				linksJSON := "[]"

				result = append(result, ingest.Row{Values: []any{
					tsBucket,
					uint32(teamID),
					timestamp,
					bytesToHex(s.TraceId),
					bytesToHex(s.SpanId),
					bytesToHex(s.ParentSpanId),
					s.TraceState,
					s.Flags,
					s.Name,
					int8(s.Kind),
					spanKindString(s.Kind),
					durNano,
					hasError,
					false, // is_remote
					int16(statusCode),
					statusCodeString(statusCode),
					statusMsg,
					httpURL,
					httpMethod,
					httpHost,
					externalHTTPURL,
					externalHTTPMethod,
					httpStatusCode,
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
	}

	return result
}

// protoAttrsToTypedMaps splits protobuf KeyValue slice into typed maps.
func protoAttrsToTypedMaps(kvs []*commonpb.KeyValue) (map[string]string, map[string]float64, map[string]bool) {
	sm := make(map[string]string, len(kvs))
	nm := make(map[string]float64)
	bm := make(map[string]bool)
	for _, kv := range kvs {
		if kv.Value == nil {
			continue
		}
		switch val := kv.Value.Value.(type) {
		case *commonpb.AnyValue_StringValue:
			sm[kv.Key] = val.StringValue
		case *commonpb.AnyValue_IntValue:
			nm[kv.Key] = float64(val.IntValue)
		case *commonpb.AnyValue_DoubleValue:
			nm[kv.Key] = val.DoubleValue
		case *commonpb.AnyValue_BoolValue:
			bm[kv.Key] = val.BoolValue
		case *commonpb.AnyValue_BytesValue:
			sm[kv.Key] = string(val.BytesValue)
		}
	}
	return sm, nm, bm
}

func protoLogID(teamID int64, tsNano uint64, traceID, spanID []byte, body string) string {
	h := fnv.New64a()
	h.Write([]byte(strconv.FormatInt(teamID, 10)))
	h.Write([]byte{0})
	b := strconv.AppendUint(nil, tsNano, 10)
	h.Write(b)
	h.Write([]byte{0})
	h.Write(traceID)
	h.Write([]byte{0})
	h.Write(spanID)
	h.Write([]byte{0})
	h.Write([]byte(body))
	return strconv.FormatUint(h.Sum64(), 16)
}

func MapLogs(teamID int64, req *logspb.ExportLogsServiceRequest) []ingest.Row {
	var rows []ingest.Row
	for _, rl := range req.ResourceLogs {
		var resAttrs []*commonpb.KeyValue
		if rl.Resource != nil {
			resAttrs = rl.Resource.Attributes
		}
		resourceJSON := attrsToJSON(resAttrs)
		fingerprint := strconv.FormatUint(resourceFingerprint(resAttrs), 16)

		for _, sl := range rl.ScopeLogs {
			scopeName := ""
			scopeVersion := ""
			if sl.Scope != nil {
				scopeName = sl.Scope.Name
				scopeVersion = sl.Scope.Version
			}
			scopeAttrs := map[string]string{}
			if scopeName != "" {
				scopeAttrs["name"] = scopeName
			}

			for _, lr := range sl.LogRecords {
				tsNs := lr.TimeUnixNano
				if tsNs == 0 {
					tsNs = lr.ObservedTimeUnixNano
				}
				if tsNs == 0 {
					tsNs = uint64(time.Now().UnixNano())
				}
				observedNs := lr.ObservedTimeUnixNano
				if observedNs == 0 {
					observedNs = uint64(time.Now().UnixNano())
				}

				// ts_bucket_start: truncate to day boundary (seconds)
				tsBucket := timebucket.LogsBucketStart(int64(tsNs / 1_000_000_000))

				severityText := lr.SeverityText
				if severityText == "" {
					severityText = severityNumberToLevel(lr.SeverityNumber)
				}

				body := anyValueString(lr.Body)
				attrStr, attrNum, attrBool := protoAttrsToTypedMaps(lr.Attributes)
				id := protoLogID(teamID, tsNs, lr.TraceId, lr.SpanId, body)

				rows = append(rows, ingest.Row{Values: []any{
					uint32(teamID),
					tsBucket,
					tsNs,
					observedNs,
					id,
					bytesToHex(lr.TraceId),
					bytesToHex(lr.SpanId),
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

func MapMetrics(teamID int64, req *metricspb.ExportMetricsServiceRequest) []ingest.Row {
	var rows []ingest.Row
	for _, rm := range req.ResourceMetrics {
		var resAttrs []*commonpb.KeyValue
		if rm.Resource != nil {
			resAttrs = rm.Resource.Attributes
		}

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

				switch data := m.Data.(type) {
				case *metricsdatapb.Metric_Gauge:
					for _, dp := range data.Gauge.DataPoints {
						val := numberDataPointValue(dp)
						attrsJSON := mergeAttrsJSON(resMap, dp.Attributes)

						rows = append(rows, ingest.Row{Values: []any{
							uint32(teamID), env, m.Name, "Gauge", "Unspecified", false,
							unit, desc, fingerprint, nanoToTime(dp.TimeUnixNano), val,
							0.0, uint64(0), []float64{}, []uint64{}, attrsJSON,
						}})
					}
				case *metricsdatapb.Metric_Sum:
					temporality := "Unspecified"
					switch data.Sum.AggregationTemporality {
					case metricsdatapb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA:
						temporality = "Delta"
					case metricsdatapb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE:
						temporality = "Cumulative"
					}
					isMonotonic := data.Sum.IsMonotonic

					for _, dp := range data.Sum.DataPoints {
						val := numberDataPointValue(dp)
						attrsJSON := mergeAttrsJSON(resMap, dp.Attributes)

						rows = append(rows, ingest.Row{Values: []any{
							uint32(teamID), env, m.Name, "Sum", temporality, isMonotonic,
							unit, desc, fingerprint, nanoToTime(dp.TimeUnixNano), val,
							0.0, uint64(0), []float64{}, []uint64{}, attrsJSON,
						}})
					}
				case *metricsdatapb.Metric_Histogram:
					temporality := "Unspecified"
					switch data.Histogram.AggregationTemporality {
					case metricsdatapb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA:
						temporality = "Delta"
					case metricsdatapb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE:
						temporality = "Cumulative"
					}

					for _, dp := range data.Histogram.DataPoints {
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

func numberDataPointValue(dp *metricsdatapb.NumberDataPoint) float64 {
	switch v := dp.Value.(type) {
	case *metricsdatapb.NumberDataPoint_AsDouble:
		return v.AsDouble
	case *metricsdatapb.NumberDataPoint_AsInt:
		return float64(v.AsInt)
	}
	return 0
}
