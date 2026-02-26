package translate

import (
	"strconv"
	"strings"

	"github.com/observability/observability-backend-go/modules/ingestion/model"
)

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

// dpLabels holds the label columns extracted from OTel semantic conventions.
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
