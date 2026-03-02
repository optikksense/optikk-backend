package telemetry

import (
	"math"
	"strconv"
	"strings"
)

// ---------------------------------------------------------------------------
// Shared helpers used by translate_proto.go (and potentially other modules)
// ---------------------------------------------------------------------------

const (
	maxAttributeCount    = 128
	maxAttributeKeyLen   = 256
	maxAttributeValueLen = 4096
)

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

// sanitizeFloat converts NaN/Inf to 0, preventing ClickHouse insert failures.
func sanitizeFloat(f float64) float64 {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return 0
	}
	return f
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

// ---------------------------------------------------------------------------
// Shared data types used across translation and metrics
// ---------------------------------------------------------------------------

type infraLabels struct {
	host      string
	pod       string
	container string
}

type dpLabels struct {
	httpMethod     string
	httpStatusCode int
	infraLabels
	status string
}
