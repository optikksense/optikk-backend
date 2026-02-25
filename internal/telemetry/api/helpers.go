package api

import (
	"strconv"
	"strings"
	"time"

	"github.com/observability/observability-backend-go/internal/telemetry/model"
)

// otlpAttrMap converts an OTel attribute list into a string map.
func otlpAttrMap(attrs []model.OTLPAttribute) map[string]string {
	m := make(map[string]string, len(attrs))
	for _, a := range attrs {
		if v := otlpAttrString(a.Value); v != "" {
			m[a.Key] = v
		}
	}
	return m
}

// otlpAttrString returns the string representation of an OTLPAnyValue.
func otlpAttrString(v model.OTLPAnyValue) string {
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

// nanosToTime parses an OTel nanosecond Unix timestamp string into time.Time.
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

// mergeOTLPAttrs merges resource and datapoint attribute maps into a
// map[string]any suitable for JSON serialisation. Datapoint attrs win.
func mergeOTLPAttrs(resource, dp map[string]string) map[string]any {
	out := make(map[string]any, len(resource)+len(dp))
	for k, v := range resource {
		out[k] = v
	}
	for k, v := range dp {
		out[k] = v
	}
	return out
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

// metricCategory returns the category string for a given OTel metric name
// based on its dotted-namespace prefix.
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

// numberDPValue extracts the float64 value from a gauge/sum data point.
// OTel SDKs emit either asDouble or asInt, never both.
func numberDPValue(dp model.OTLPNumberDataPoint) float64 {
	if dp.AsDouble != nil {
		return *dp.AsDouble
	}
	if dp.AsInt != nil {
		n, _ := strconv.ParseInt(*dp.AsInt, 10, 64)
		return float64(n)
	}
	return 0
}

// resourceContext bundles the resource-level attributes and service name that
// every handler extracts identically from each OTLP resource.
type resourceContext struct {
	attrs       map[string]string
	serviceName string
}

// newResourceContext converts raw OTLP resource attributes into a
// resourceContext, defaulting serviceName to "unknown" when absent.
func newResourceContext(raw []model.OTLPAttribute) resourceContext {
	attrs := otlpAttrMap(raw)
	svc := attrs["service.name"]
	if svc == "" {
		svc = "unknown"
	}
	return resourceContext{attrs: attrs, serviceName: svc}
}

// infraLabels holds the common infrastructure fields extracted from OTel
// semantic conventions (host, pod, container).
type infraLabels struct {
	host      string
	pod       string
	container string
}

// extractInfraLabels resolves host/pod/container from datapoint-level and
// resource-level attribute maps. Datapoint attributes take precedence.
func extractInfraLabels(dp, resource map[string]string) infraLabels {
	return infraLabels{
		host:      firstNonEmpty(dp["server.address"], dp["net.host.name"], resource["host.name"]),
		pod:       firstNonEmpty(dp["k8s.pod.name"], resource["k8s.pod.name"]),
		container: firstNonEmpty(dp["k8s.container.name"], resource["k8s.container.name"]),
	}
}

// Label extraction (OTel semantic conventions -> dedicated columns)
type dpLabels struct {
	httpMethod     string
	httpStatusCode int
	infraLabels
	status string
}

// extractDPLabels maps OTel semantic-convention attribute keys to the column
// fields used by the metrics table. Datapoint attributes take precedence over
// resource attributes for host/pod/container.
func extractDPLabels(dp, resource map[string]string) dpLabels {
	httpMethod := firstNonEmpty(dp["http.request.method"], dp["http.method"])

	statusStr := firstNonEmpty(dp["http.response.status_code"], dp["http.status_code"])
	httpStatusCode, _ := strconv.Atoi(statusStr)

	status := "OK"
	if httpStatusCode >= 500 {
		status = "ERROR"
	}

	return dpLabels{
		httpMethod:     httpMethod,
		httpStatusCode: httpStatusCode,
		infraLabels:    extractInfraLabels(dp, resource),
		status:         status,
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
