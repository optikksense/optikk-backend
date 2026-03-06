package grpc

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"strconv"
	"time"

	"github.com/observability/observability-backend-go/internal/platform/ingest"
	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	log "go.opentelemetry.io/proto/otlp/logs/v1"
	metricsdatapb "go.opentelemetry.io/proto/otlp/metrics/v1"
	trace "go.opentelemetry.io/proto/otlp/trace/v1"
)

// nanoToTime converts a Unix nanosecond uint64 to time.Time.
func nanoToTime(ns uint64) time.Time {
	if ns == 0 {
		return time.Now()
	}
	return time.Unix(0, int64(ns))
}

// spanKindString converts the OTel SpanKind enum to a human-readable string.
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

// statusCodeString converts the OTel span status code to a string.
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

// severityNumberToLevel converts an OTel severity number to a log level string.
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

// anyValueString extracts the string representation from a protobuf AnyValue.
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

// attrsToMap converts a slice of protobuf KeyValue into map[string]string.
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
	b, err := json.Marshal(m)
	if err != nil {
		return "{}"
	}
	return string(b)
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

// lookupAttr returns the string value of a named attribute.
func lookupAttr(kvs []*commonpb.KeyValue, key string) string {
	for _, kv := range kvs {
		if kv.Key == key {
			return anyValueString(kv.Value)
		}
	}
	return ""
}

// lookupAttrInt returns the int64 value of a named attribute.
func lookupAttrInt(kvs []*commonpb.KeyValue, key string) int64 {
	s := lookupAttr(kvs, key)
	if s == "" {
		return 0
	}
	v, _ := strconv.ParseInt(s, 10, 64)
	return v
}

// bytesToHex is a helper to stringify Trace/Span IDs.
func bytesToHex(b []byte) string {
	if len(b) > 0 {
		return fmt.Sprintf("%x", b)
	}
	return ""
}

// MapSpans maps gRPC Traces Request to ingest.Row.
func MapSpans(teamID string, req *tracepb.ExportTraceServiceRequest) []ingest.Row {
	var rows []ingest.Row
	for _, rs := range req.ResourceSpans {
		var resAttrs []*commonpb.KeyValue
		if rs.Resource != nil {
			resAttrs = rs.Resource.Attributes
		}
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

				parentSpanID := bytesToHex(s.ParentSpanId)
				isRoot := uint8(0)
				if parentSpanID == "" {
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
				for k, v := range attrsToMap(spanAttrs) {
					tags[k] = v
				}

				statusMsg, statusCode := "", trace.Status_STATUS_CODE_UNSET
				if s.Status != nil {
					statusMsg = s.Status.Message
					statusCode = s.Status.Code
				}

				rows = append(rows, ingest.Row{Values: []any{
					teamID,
					bytesToHex(s.TraceId),
					bytesToHex(s.SpanId),
					parentSpanID,
					"", // parent_service_name
					isRoot,
					s.Name,
					serviceName,
					spanKindString(s.Kind),
					start,
					end,
					durMs,
					statusCodeString(statusCode),
					statusMsg,
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

// protoLogID generates a stable FNV-64a ID for a gRPC log record.
func protoLogID(teamID string, tsNano uint64, traceID, spanID []byte, body string) string {
	h := fnv.New64a()
	h.Write([]byte(teamID))
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

// MapLogs maps gRPC Logs Request to ingest.Row.
func MapLogs(teamID string, req *logspb.ExportLogsServiceRequest) []ingest.Row {
	var rows []ingest.Row
	for _, rl := range req.ResourceLogs {
		var resAttrs []*commonpb.KeyValue
		if rl.Resource != nil {
			resAttrs = rl.Resource.Attributes
		}
		resourceMap := attrsToMap(resAttrs)
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
				tsSec := tsNs / 1_000_000_000
				tsBucket := uint32(tsSec - tsSec%86400)

				severityText := lr.SeverityText
				if severityText == "" {
					severityText = severityNumberToLevel(lr.SeverityNumber)
				}

				body := anyValueString(lr.Body)
				attrStr, attrNum, attrBool := protoAttrsToTypedMaps(lr.Attributes)
				id := protoLogID(teamID, tsNs, lr.TraceId, lr.SpanId, body)

				rows = append(rows, ingest.Row{Values: []any{
					teamID,
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
					resourceMap,
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

// MapMetrics maps gRPC Metrics Request to ingest.Row.
func MapMetrics(teamID string, req *metricspb.ExportMetricsServiceRequest) []ingest.Row {
	var rows []ingest.Row
	for _, rm := range req.ResourceMetrics {
		var resAttrs []*commonpb.KeyValue
		if rm.Resource != nil {
			resAttrs = rm.Resource.Attributes
		}

		env := lookupAttr(resAttrs, "deployment.environment")
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
						attrsJSON := attrsToJSON(append(resAttrs, dp.Attributes...))

						rows = append(rows, ingest.Row{Values: []any{
							teamID, env, m.Name, "Gauge", "Unspecified", false,
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
						attrsJSON := attrsToJSON(append(resAttrs, dp.Attributes...))

						rows = append(rows, ingest.Row{Values: []any{
							teamID, env, m.Name, "Sum", temporality, isMonotonic,
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

func numberDataPointValue(dp *metricsdatapb.NumberDataPoint) float64 {
	switch v := dp.Value.(type) {
	case *metricsdatapb.NumberDataPoint_AsDouble:
		return v.AsDouble
	case *metricsdatapb.NumberDataPoint_AsInt:
		return float64(v.AsInt)
	}
	return 0
}
