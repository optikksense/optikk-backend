package telemetry

import (
	"math"
	"strconv"
	"strings"
	"time"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

// TranslateSpans converts an OTLP traces payload into SpanRecord slices.
// ParentServiceName is populated via a batch-local spanID->serviceName lookup,
// then falls back to the cross-batch SpanCache for parents that arrived in
// earlier HTTP requests. After translation, every span in the batch is written
// to the cache so future batches can resolve them.
func TranslateSpans(teamUUID string, payload OTLPTracesPayload, cache *SpanCache) []SpanRecord {
	// Pass 1: build spanID -> serviceName map for the entire batch.
	spanService := make(map[string]string)
	for _, rs := range payload.ResourceSpans {
		rc := newResourceContext(rs.Resource.Attributes)
		for _, ss := range rs.ScopeSpans {
			for _, span := range ss.Spans {
				spanAttrs := otlpAttrMap(span.Attributes)
				spanService[span.SpanID] = firstNonEmpty(spanAttrs["service.name"], rc.serviceName, "unknown")
			}
		}
	}

	// Pass 2: build records with ParentServiceName resolved from the map.
	var spans []SpanRecord
	for _, rs := range payload.ResourceSpans {
		rc := newResourceContext(rs.Resource.Attributes)
		for _, ss := range rs.ScopeSpans {
			for _, span := range ss.Spans {
				spanAttrs := otlpAttrMap(span.Attributes)
				allAttrs := mergeOTLPAttrs(rc.attrs, spanAttrs)

				startTime := nanosToTime(span.StartTimeUnixNano)
				endTime := nanosToTime(span.EndTimeUnixNano)
				durationMs := endTime.Sub(startTime).Milliseconds()
				if durationMs < 0 {
					durationMs = 0
				}

				status := "OK"
				statusMessage := ""
				if span.Status != nil {
					if span.Status.Code == 2 {
						status = "ERROR"
					}
					statusMessage = span.Status.Message
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

				operationName := firstNonEmpty(span.Name, spanAttrs["http.route"], spanAttrs["rpc.method"], spanAttrs["db.operation"], "unknown")
				serviceName := firstNonEmpty(spanAttrs["service.name"], rc.serviceName, "unknown")
				infra := extractInfraLabels(spanAttrs, rc.attrs)

				isRoot := 0
				if span.ParentSpanID == "" {
					isRoot = 1
				}

				// Resolve parent service name: batch-local first, then cross-batch cache.
				parentServiceName := spanService[span.ParentSpanID]
				if parentServiceName == "" && span.ParentSpanID != "" && cache != nil {
					parentServiceName, _ = cache.Get(span.ParentSpanID)
				}

				spans = append(spans, SpanRecord{
					TeamUUID:          teamUUID,
					TraceID:           span.TraceID,
					SpanID:            span.SpanID,
					ParentSpanID:      span.ParentSpanID,
					ParentServiceName: parentServiceName,
					IsRoot:            isRoot,
					OperationName:     operationName,
					ServiceName:       serviceName,
					SpanKind:          spanKindString(span.Kind),
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
					Attributes:        dbutil.JSONString(allAttrs),
				})
			}
		}
	}

	// Populate the cross-batch cache with every span from this batch so that
	// future batches can resolve parent service names across HTTP requests.
	if cache != nil {
		for spanID, svcName := range spanService {
			cache.Put(spanID, svcName)
		}
	}

	return spans
}

// TranslateMetrics converts an OTLP metrics payload into MetricRecord slices.
// NaN/Inf float values are sanitized to 0.
func TranslateMetrics(teamUUID string, payload OTLPMetricsPayload) []MetricRecord {
	var metrics []MetricRecord
	for _, rm := range payload.ResourceMetrics {
		rc := newResourceContext(rm.Resource.Attributes)
		for _, sm := range rm.ScopeMetrics {
			for _, metric := range sm.Metrics {
				category := metricCategory(metric.Name)
				switch {
				case metric.Gauge != nil:
					for _, dp := range metric.Gauge.DataPoints {
						metrics = append(metrics, buildNumberMetricRecord(teamUUID, rc, metric.Name, "gauge", category, dp))
					}
				case metric.Sum != nil:
					for _, dp := range metric.Sum.DataPoints {
						metrics = append(metrics, buildNumberMetricRecord(teamUUID, rc, metric.Name, "sum", category, dp))
					}
				case metric.Histogram != nil:
					for _, dp := range metric.Histogram.DataPoints {
						metrics = append(metrics, buildHistogramMetricRecord(teamUUID, rc, metric.Name, category, dp))
					}
				}
			}
		}
	}
	return metrics
}

// TranslateLogs converts an OTLP logs payload into LogRecord slices.
func TranslateLogs(teamUUID string, payload OTLPLogsPayload) []LogRecord {
	var logs []LogRecord
	for _, rl := range payload.ResourceLogs {
		rc := newResourceContext(rl.Resource.Attributes)
		for _, sl := range rl.ScopeLogs {
			for _, record := range sl.LogRecords {
				logAttrs := otlpAttrMap(record.Attributes)
				allAttrs := mergeOTLPAttrs(rc.attrs, logAttrs)

				ts := nanosToTime(record.TimeUnixNano)
				if strings.TrimSpace(record.TimeUnixNano) == "" {
					ts = nanosToTime(record.ObservedTimeUnixNano)
				}

				level := strings.TrimSpace(record.SeverityText)
				if level == "" {
					level = severityTextFromNumber(record.SeverityNumber)
				}

				message := strings.TrimSpace(otlpAttrString(record.Body))
				if message == "" {
					message = strings.TrimSpace(logAttrs["message"])
				}

				infra := extractInfraLabels(logAttrs, rc.attrs)
				traceID := strings.TrimSpace(record.TraceID)
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
				spanID := strings.TrimSpace(record.SpanID)
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
					Logger:     firstNonEmpty(logAttrs["logger.name"], sl.Scope.Name),
					Message:    message,
					TraceID:    traceID,
					SpanID:     spanID,
					Host:       infra.host,
					Pod:        infra.pod,
					Container:  infra.container,
					Thread:     firstNonEmpty(logAttrs["thread.name"], logAttrs["thread.id"]),
					Exception:  firstNonEmpty(logAttrs["exception.message"], logAttrs["exception.type"]),
					Attributes: dbutil.JSONString(allAttrs),
				})
			}
		}
	}
	return logs
}

// ---------------------------------------------------------------------------
// Metric builders
// ---------------------------------------------------------------------------

func buildNumberMetricRecord(teamUUID string, rc resourceContext, name, metricType, category string, dp OTLPNumberDataPoint) MetricRecord {
	dpAttrs := otlpAttrMap(dp.Attributes)
	labels := extractDPLabels(dpAttrs, rc.attrs)
	v := sanitizeFloat(numberDPValue(dp))
	return MetricRecord{
		TeamUUID:       teamUUID,
		MetricName:     name,
		MetricType:     metricType,
		MetricCategory: category,
		ServiceName:    rc.serviceName,
		Timestamp:      nanosToTime(dp.TimeUnixNano),
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
		Attributes:     dbutil.JSONString(mergeOTLPAttrs(rc.attrs, dpAttrs)),
	}
}

func buildHistogramMetricRecord(teamUUID string, rc resourceContext, name, category string, dp OTLPHistogramDataPoint) MetricRecord {
	dpAttrs := otlpAttrMap(dp.Attributes)
	labels := extractDPLabels(dpAttrs, rc.attrs)
	ts := nanosToTime(dp.TimeUnixNano)

	count, _ := strconv.ParseInt(dp.Count, 10, 64)
	sumVal := 0.0
	if dp.Sum != nil {
		sumVal = *dp.Sum
	}
	minVal, maxVal := 0.0, 0.0
	if dp.Min != nil {
		minVal = *dp.Min
	}
	if dp.Max != nil {
		maxVal = *dp.Max
	}
	avgVal := 0.0
	if count > 0 {
		avgVal = sumVal / float64(count)
	}
	p50Val, p95Val, p99Val := minVal, minVal+(maxVal-minVal)*0.85, maxVal
	if minVal == 0 && maxVal == 0 {
		p50Val, p95Val, p99Val = avgVal, avgVal, avgVal
	}

	allAttrs := mergeOTLPAttrs(rc.attrs, dpAttrs)
	if len(dp.BucketCounts) > 0 {
		allAttrs["_bucketCounts"] = dp.BucketCounts
		allAttrs["_explicitBounds"] = dp.ExplicitBounds
	}

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
		Attributes:     dbutil.JSONString(allAttrs),
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func otlpAttrMap(attrs []OTLPAttribute) map[string]string {
	m := make(map[string]string, len(attrs))
	for _, a := range attrs {
		if v := otlpAttrString(a.Value); v != "" {
			m[a.Key] = v
		}
	}
	return m
}

func otlpAttrString(v OTLPAnyValue) string {
	switch {
	case v.StringValue != nil:
		return *v.StringValue
	case v.IntValue != nil:
		return *v.IntValue
	case v.DoubleValue != nil:
		return strconv.FormatFloat(*v.DoubleValue, 'f', -1, 64)
	case v.BoolValue != nil:
		if *v.BoolValue {
			return "true"
		}
		return "false"
	}
	return ""
}

func nanosToTime(s string) time.Time {
	if s == "" {
		return time.Now().UTC()
	}
	ns, err := strconv.ParseInt(s, 10, 64)
	if err != nil || ns == 0 {
		return time.Now().UTC()
	}
	return time.Unix(0, ns).UTC()
}

const (
	maxAttributeCount    = 128
	maxAttributeKeyLen   = 256
	maxAttributeValueLen = 4096
)

func mergeOTLPAttrs(resource, dp map[string]string) map[string]any {
	out := make(map[string]any, len(resource)+len(dp))
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

func truncateAttr(key, value string) (string, string) {
	if len(key) > maxAttributeKeyLen {
		key = key[:maxAttributeKeyLen]
	}
	if len(value) > maxAttributeValueLen {
		value = value[:maxAttributeValueLen]
	}
	return key, value
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		v = strings.TrimSpace(v)
		if v != "" {
			return v
		}
	}
	return ""
}

func severityTextFromNumber(n int) string {
	switch {
	case n >= 21:
		return "FATAL"
	case n >= 17:
		return "ERROR"
	case n >= 13:
		return "WARN"
	case n >= 9:
		return "INFO"
	case n >= 5:
		return "DEBUG"
	default:
		return "INFO"
	}
}

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
		return "INTERNAL"
	}
}

func metricCategory(name string) string {
	n := strings.ToLower(name)
	switch {
	case strings.HasPrefix(n, "http."):
		return "http"
	case strings.HasPrefix(n, "jvm."):
		return "jvm"
	case strings.HasPrefix(n, "db."):
		return "db"
	case strings.HasPrefix(n, "messaging."):
		return "messaging"
	case strings.HasPrefix(n, "system."):
		return "system"
	case strings.HasPrefix(n, "process."):
		return "process"
	case strings.HasPrefix(n, "runtime."):
		return "runtime"
	default:
		return "custom"
	}
}

func numberDPValue(dp OTLPNumberDataPoint) float64 {
	if dp.AsDouble != nil {
		return *dp.AsDouble
	}
	if dp.AsInt != nil {
		n, _ := strconv.ParseInt(*dp.AsInt, 10, 64)
		return float64(n)
	}
	return 0
}

// sanitizeFloat converts NaN/Inf to 0, preventing ClickHouse insert failures.
func sanitizeFloat(f float64) float64 {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return 0
	}
	return f
}

type resourceContext struct {
	attrs       map[string]string
	serviceName string
}

func newResourceContext(raw []OTLPAttribute) resourceContext {
	attrs := otlpAttrMap(raw)
	svc := attrs["service.name"]
	if svc == "" {
		svc = "unknown"
	}
	return resourceContext{attrs: attrs, serviceName: svc}
}

type infraLabels struct {
	host      string
	pod       string
	container string
}

func extractInfraLabels(dp, resource map[string]string) infraLabels {
	return infraLabels{
		host: firstNonEmpty(
			dp["server.address"], dp["server.socket.address"], dp["host.name"], dp["net.host.name"],
			resource["host.name"], resource["host.id"], resource["k8s.node.name"], resource["service.instance.id"],
		),
		pod:       firstNonEmpty(dp["k8s.pod.name"], resource["k8s.pod.name"], dp["pod.name"], resource["pod.name"]),
		container: firstNonEmpty(dp["k8s.container.name"], resource["k8s.container.name"], dp["container.name"], resource["container.name"]),
	}
}

type dpLabels struct {
	httpMethod     string
	httpStatusCode int
	infraLabels
	status string
}

func extractDPLabels(dp, resource map[string]string) dpLabels {
	httpMethod := firstNonEmpty(dp["http.request.method"], dp["http.method"], dp["method"])
	httpStatusCode := parseHTTPStatusCode(dp)
	status := "OK"
	if httpStatusCode >= 400 {
		status = "ERROR"
	}
	return dpLabels{
		httpMethod:     httpMethod,
		httpStatusCode: httpStatusCode,
		infraLabels:    extractInfraLabels(dp, resource),
		status:         status,
	}
}

func parseHTTPStatusCode(attrs map[string]string) int {
	for _, key := range []string{
		"http.response.status_code",
		"http.status_code",
		"status_code",
		"status",
	} {
		if value := strings.TrimSpace(attrs[key]); value != "" {
			if code, err := strconv.Atoi(value); err == nil {
				return code
			}
		}
	}
	return 0
}
