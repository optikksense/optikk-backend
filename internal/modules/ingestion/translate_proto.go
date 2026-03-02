package telemetry

import (
	"encoding/hex"
	"math"
	"strconv"
	"strings"
	"time"

	dbutil "github.com/observability/observability-backend-go/internal/database"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

// ---------------------------------------------------------------------------
// Proto → Domain record translation (shared by gRPC and HTTP paths)
// ---------------------------------------------------------------------------

// TranslateProtoSpans converts an OTLP ExportTraceServiceRequest directly into
// SpanRecord slices, bypassing any intermediate model structs. Both the gRPC
// handler and the HTTP handler call this after deserialization.
func TranslateProtoSpans(teamUUID string, req *coltracepb.ExportTraceServiceRequest, cache *SpanCache) []SpanRecord {
	if req == nil {
		return nil
	}

	// Pass 1: build spanID → serviceName map for batch-local parent resolution.
	spanService := make(map[string]string)
	for _, rs := range req.GetResourceSpans() {
		rc := protoResourceCtx(rs.GetResource())
		for _, ss := range rs.GetScopeSpans() {
			for _, span := range ss.GetSpans() {
				spanAttrs := protoAttrMap(span.GetAttributes())
				sid := hexEncode(span.GetSpanId())
				spanService[sid] = firstNonEmpty(spanAttrs["service.name"], rc.serviceName, "unknown")
			}
		}
	}

	// Pass 2: build domain records.
	var spans []SpanRecord
	for _, rs := range req.GetResourceSpans() {
		rc := protoResourceCtx(rs.GetResource())
		for _, ss := range rs.GetScopeSpans() {
			for _, span := range ss.GetSpans() {
				spanAttrs := protoAttrMap(span.GetAttributes())
				allAttrs := mergeStringMaps(rc.attrs, spanAttrs)

				startTime := unixNanoToTime(span.GetStartTimeUnixNano())
				endTime := unixNanoToTime(span.GetEndTimeUnixNano())
				durationMs := endTime.Sub(startTime).Milliseconds()
				if durationMs < 0 {
					durationMs = 0
				}

				status := "OK"
				statusMessage := ""
				if st := span.GetStatus(); st != nil {
					if st.GetCode() == 2 {
						status = "ERROR"
					}
					statusMessage = st.GetMessage()
				}

				httpMethod := firstNonEmpty(
					spanAttrs["http.request.method"],
					spanAttrs["http.method"],
					spanAttrs["method"],
					spanAttrs["request.method"],
				)
				httpURL := firstNonEmpty(
					spanAttrs["url.full"],
					spanAttrs["http.url"],
					spanAttrs["http.target"],
					spanAttrs["http.route"],
					spanAttrs["uri"],
					spanAttrs["path"],
				)
				httpStatusCode := parseHTTPStatusCode(spanAttrs)
				if status != "ERROR" && httpStatusCode >= 400 {
					status = "ERROR"
					if statusMessage == "" {
						statusMessage = "HTTP " + strconv.Itoa(httpStatusCode)
					}
				}

				operationName := firstNonEmpty(span.GetName(), spanAttrs["http.route"], spanAttrs["rpc.method"], spanAttrs["db.operation"], "unknown")
				serviceName := firstNonEmpty(spanAttrs["service.name"], rc.serviceName, "unknown")
				infra := extractInfraLabelsFromMap(spanAttrs, rc.attrs)

				traceID := hexEncode(span.GetTraceId())
				spanID := hexEncode(span.GetSpanId())
				parentSpanID := hexEncode(span.GetParentSpanId())

				isRoot := 0
				if parentSpanID == "" {
					isRoot = 1
				}

				// Resolve parent service: batch-local first, then cross-batch cache.
				parentServiceName := spanService[parentSpanID]
				if parentServiceName == "" && parentSpanID != "" && cache != nil {
					parentServiceName, _ = cache.Get(parentSpanID)
				}

				spans = append(spans, SpanRecord{
					TeamUUID:          teamUUID,
					TraceID:           traceID,
					SpanID:            spanID,
					ParentSpanID:      parentSpanID,
					ParentServiceName: parentServiceName,
					IsRoot:            isRoot,
					OperationName:     operationName,
					ServiceName:       serviceName,
					SpanKind:          spanKindString(int(span.GetKind())),
					StartTime:         startTime,
					EndTime:           endTime,
					DurationMs:        durationMs,
					Status:            status,
					StatusMessage:     statusMessage,
					HTTPMethod:        httpMethod,
					HTTPURL:           httpURL,
					HTTPStatusCode:    httpStatusCode,
					Host:              infra.host,
					Pod:               infra.pod,
					Container:         infra.container,
					Attributes:        dbutil.JSONString(toAnyMap(allAttrs)),
				})
			}
		}
	}

	// Populate cross-batch cache.
	if cache != nil {
		for spanID, svcName := range spanService {
			cache.Put(spanID, svcName)
		}
	}

	return spans
}

// TranslateProtoMetrics converts an OTLP ExportMetricsServiceRequest directly
// into MetricRecord slices.
func TranslateProtoMetrics(teamUUID string, req *colmetricspb.ExportMetricsServiceRequest) []MetricRecord {
	if req == nil {
		return nil
	}
	var metrics []MetricRecord
	for _, rm := range req.GetResourceMetrics() {
		rc := protoResourceCtx(rm.GetResource())
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				category := metricCategory(m.GetName())
				switch d := m.GetData().(type) {
				case *metricspb.Metric_Gauge:
					for _, dp := range d.Gauge.GetDataPoints() {
						metrics = append(metrics, buildProtoNumberMetric(teamUUID, rc, m.GetName(), "gauge", category, dp))
					}
				case *metricspb.Metric_Sum:
					for _, dp := range d.Sum.GetDataPoints() {
						metrics = append(metrics, buildProtoNumberMetric(teamUUID, rc, m.GetName(), "sum", category, dp))
					}
				case *metricspb.Metric_Histogram:
					for _, dp := range d.Histogram.GetDataPoints() {
						metrics = append(metrics, buildProtoHistogramMetric(teamUUID, rc, m.GetName(), category, dp))
					}
				}
			}
		}
	}
	return metrics
}

// TranslateProtoLogs converts an OTLP ExportLogsServiceRequest directly into
// LogRecord slices.
func TranslateProtoLogs(teamUUID string, req *collogspb.ExportLogsServiceRequest) []LogRecord {
	if req == nil {
		return nil
	}
	var logs []LogRecord
	for _, rl := range req.GetResourceLogs() {
		rc := protoResourceCtx(rl.GetResource())
		for _, sl := range rl.GetScopeLogs() {
			scopeName := ""
			if sl.GetScope() != nil {
				scopeName = sl.GetScope().GetName()
			}
			for _, lr := range sl.GetLogRecords() {
				logAttrs := protoAttrMap(lr.GetAttributes())
				allAttrs := mergeStringMaps(rc.attrs, logAttrs)

				ts := unixNanoToTime(lr.GetTimeUnixNano())
				if lr.GetTimeUnixNano() == 0 {
					ts = unixNanoToTime(lr.GetObservedTimeUnixNano())
				}

				level := strings.TrimSpace(lr.GetSeverityText())
				if level == "" {
					level = severityTextFromNumber(int(lr.GetSeverityNumber()))
				}

				message := strings.TrimSpace(protoAnyValueStr(lr.GetBody()))
				if message == "" {
					message = strings.TrimSpace(logAttrs["message"])
				}

				infra := extractInfraLabelsFromMap(logAttrs, rc.attrs)

				traceID := hexEncode(lr.GetTraceId())
				if traceID == "" {
					traceID = firstNonEmpty(
						logAttrs["trace.id"],
						logAttrs["trace_id"],
						logAttrs["traceId"],
						logAttrs["otel.trace_id"],
						logAttrs["log.mdc.trace.id"],
						logAttrs["log.mdc.trace_id"],
						logAttrs["log.mdc.traceId"],
					)
				}
				spanID := hexEncode(lr.GetSpanId())
				if spanID == "" {
					spanID = firstNonEmpty(
						logAttrs["span.id"],
						logAttrs["span_id"],
						logAttrs["spanId"],
						logAttrs["otel.span_id"],
						logAttrs["log.mdc.span.id"],
						logAttrs["log.mdc.span_id"],
						logAttrs["log.mdc.spanId"],
					)
				}

				logs = append(logs, LogRecord{
					TeamUUID:   teamUUID,
					Timestamp:  ts,
					Level:      level,
					Service:    firstNonEmpty(logAttrs["service.name"], rc.serviceName),
					Logger:     firstNonEmpty(logAttrs["logger.name"], scopeName),
					Message:    message,
					TraceID:    traceID,
					SpanID:     spanID,
					Host:       infra.host,
					Pod:        infra.pod,
					Container:  infra.container,
					Thread:     firstNonEmpty(logAttrs["thread.name"], logAttrs["thread.id"]),
					Exception:  firstNonEmpty(logAttrs["exception.message"], logAttrs["exception.type"]),
					Attributes: dbutil.JSONString(toAnyMap(allAttrs)),
				})
			}
		}
	}
	return logs
}

// ---------------------------------------------------------------------------
// Proto metric builders
// ---------------------------------------------------------------------------

func buildProtoNumberMetric(teamUUID string, rc protoResourceContext, name, metricType, category string, dp *metricspb.NumberDataPoint) MetricRecord {
	dpAttrs := protoAttrMap(dp.GetAttributes())
	labels := extractDPLabelsFromMap(dpAttrs, rc.attrs)
	v := sanitizeFloat(protoNumberDPValue(dp))
	return MetricRecord{
		TeamUUID:       teamUUID,
		MetricName:     name,
		MetricType:     metricType,
		MetricCategory: category,
		ServiceName:    rc.serviceName,
		Timestamp:      unixNanoToTime(dp.GetTimeUnixNano()),
		Value:          v,
		Count:          1,
		Sum:            v,
		Min:            v,
		Max:            v,
		Avg:            v,
		HTTPMethod:     labels.httpMethod,
		HTTPStatusCode: labels.httpStatusCode,
		Status:         labels.status,
		Host:           labels.host,
		Pod:            labels.pod,
		Container:      labels.container,
		Attributes:     dbutil.JSONString(toAnyMap(mergeStringMaps(rc.attrs, dpAttrs))),
	}
}

func buildProtoHistogramMetric(teamUUID string, rc protoResourceContext, name, category string, dp *metricspb.HistogramDataPoint) MetricRecord {
	dpAttrs := protoAttrMap(dp.GetAttributes())
	labels := extractDPLabelsFromMap(dpAttrs, rc.attrs)
	ts := unixNanoToTime(dp.GetTimeUnixNano())

	count := int64(dp.GetCount())
	sumVal := dp.GetSum()
	minVal := dp.GetMin()
	maxVal := dp.GetMax()
	avgVal := 0.0
	if count > 0 {
		avgVal = sumVal / float64(count)
	}
	p50Val, p95Val, p99Val := minVal, minVal+(maxVal-minVal)*0.85, maxVal
	if minVal == 0 && maxVal == 0 {
		p50Val, p95Val, p99Val = avgVal, avgVal, avgVal
	}

	allAttrs := mergeStringMaps(rc.attrs, dpAttrs)

	return MetricRecord{
		TeamUUID:       teamUUID,
		MetricName:     name,
		MetricType:     "histogram",
		MetricCategory: category,
		ServiceName:    rc.serviceName,
		Timestamp:      ts,
		Value:          sanitizeFloat(avgVal),
		Count:          count,
		Sum:            sanitizeFloat(sumVal),
		Min:            sanitizeFloat(minVal),
		Max:            sanitizeFloat(maxVal),
		Avg:            sanitizeFloat(avgVal),
		P50:            sanitizeFloat(p50Val),
		P95:            sanitizeFloat(p95Val),
		P99:            sanitizeFloat(p99Val),
		HTTPMethod:     labels.httpMethod,
		HTTPStatusCode: labels.httpStatusCode,
		Status:         labels.status,
		Host:           labels.host,
		Pod:            labels.pod,
		Container:      labels.container,
		Attributes:     dbutil.JSONString(toAnyMap(allAttrs)),
	}
}

// ---------------------------------------------------------------------------
// Proto helpers
// ---------------------------------------------------------------------------

type protoResourceContext struct {
	attrs       map[string]string
	serviceName string
}

func protoResourceCtx(r *resourcepb.Resource) protoResourceContext {
	if r == nil {
		return protoResourceContext{attrs: map[string]string{}, serviceName: "unknown"}
	}
	attrs := protoAttrMap(r.GetAttributes())
	svc := attrs["service.name"]
	if svc == "" {
		svc = "unknown"
	}
	return protoResourceContext{attrs: attrs, serviceName: svc}
}

// protoAttrMap converts proto KeyValue attributes to a flat string map.
func protoAttrMap(attrs []*commonpb.KeyValue) map[string]string {
	m := make(map[string]string, len(attrs))
	for _, kv := range attrs {
		if v := protoAnyValueStr(kv.GetValue()); v != "" {
			m[kv.GetKey()] = v
		}
	}
	return m
}

// protoAnyValueStr extracts a string representation from a proto AnyValue.
func protoAnyValueStr(v *commonpb.AnyValue) string {
	if v == nil {
		return ""
	}
	switch val := v.GetValue().(type) {
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
	default:
		return ""
	}
}

// hexEncode converts raw bytes (trace ID, span ID) to lowercase hex string.
func hexEncode(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	// Skip all-zero IDs (proto default).
	allZero := true
	for _, v := range b {
		if v != 0 {
			allZero = false
			break
		}
	}
	if allZero {
		return ""
	}
	return hex.EncodeToString(b)
}

// unixNanoToTime converts uint64 nanoseconds to time.Time.
func unixNanoToTime(ns uint64) time.Time {
	if ns == 0 {
		return time.Now().UTC()
	}
	return time.Unix(0, int64(ns)).UTC()
}

func protoNumberDPValue(dp *metricspb.NumberDataPoint) float64 {
	switch v := dp.GetValue().(type) {
	case *metricspb.NumberDataPoint_AsDouble:
		return v.AsDouble
	case *metricspb.NumberDataPoint_AsInt:
		return float64(v.AsInt)
	default:
		return 0
	}
}

// mergeStringMaps merges two string maps with attribute limits.
func mergeStringMaps(resource, dp map[string]string) map[string]string {
	out := make(map[string]string, len(resource)+len(dp))
	count := 0
	for k, v := range resource {
		if count >= maxAttributeCount {
			break
		}
		k, v = truncateAttr(k, v)
		out[k] = v
		count++
	}
	for k, v := range dp {
		if count >= maxAttributeCount {
			break
		}
		k, v = truncateAttr(k, v)
		out[k] = v
		count++
	}
	return out
}

// toAnyMap converts map[string]string to map[string]any for JSON serialization.
func toAnyMap(m map[string]string) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// extractInfraLabelsFromMap extracts host/pod/container from string maps.
func extractInfraLabelsFromMap(dp, resource map[string]string) infraLabels {
	return infraLabels{
		host: firstNonEmpty(
			dp["server.address"], dp["server.socket.address"], dp["host.name"], dp["net.host.name"],
			resource["host.name"], resource["host.id"], resource["k8s.node.name"], resource["service.instance.id"],
		),
		pod:       firstNonEmpty(dp["k8s.pod.name"], resource["k8s.pod.name"], dp["pod.name"], resource["pod.name"]),
		container: firstNonEmpty(dp["k8s.container.name"], resource["k8s.container.name"], dp["container.name"], resource["container.name"]),
	}
}

// extractDPLabelsFromMap extracts data point labels from string maps.
func extractDPLabelsFromMap(dp, resource map[string]string) dpLabels {
	httpMethod := firstNonEmpty(dp["http.request.method"], dp["http.method"], dp["method"])
	httpStatusCode := parseHTTPStatusCode(dp)
	status := "OK"
	if httpStatusCode >= 400 {
		status = "ERROR"
	}
	return dpLabels{
		httpMethod:     httpMethod,
		httpStatusCode: httpStatusCode,
		infraLabels:    extractInfraLabelsFromMap(dp, resource),
		status:         status,
	}
}

// sanitizeFloat converts NaN/Inf to 0, preventing ClickHouse insert failures.
// Re-exported here so both old and new translation paths can use it.
func sanitizeProtoFloat(f float64) float64 {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return 0
	}
	return f
}
