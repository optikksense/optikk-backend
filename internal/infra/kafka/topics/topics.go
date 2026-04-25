// Package topics holds the canonical ingest / DLQ topic names and consumer
// group id helpers shared by the producer, consumer, and DLQ wrappers.
package topics

import (
	"fmt"
	"strings"
)

// Signal enumerates the three OTLP signals we ingest. The naming here feeds
// both topic construction and consumer-group construction so the canonical
// wire names stay in one place.
const (
	SignalLogs    = "logs"
	SignalMetrics = "metrics"
	SignalSpans   = "spans"
)

// IngestTopics returns the three canonical ingest topic names for the given
// prefix. The order is fixed: logs, metrics, spans.
func IngestTopics(prefix string) []string {
	p := normalizeIngestPrefix(prefix)
	return []string{
		IngestTopic(p, SignalLogs),
		IngestTopic(p, SignalMetrics),
		IngestTopic(p, SignalSpans),
	}
}

// IngestTopic returns the full topic name for one signal. Callers must pass
// one of the Signal* constants; any other value returns a best-effort
// "<prefix>.<signal>" name so misuse still fails loudly at broker time.
func IngestTopic(prefix, signal string) string {
	return normalizeIngestPrefix(prefix) + "." + signal
}

// DLQTopic returns the canonical DLQ topic name for a signal, e.g. for
// prefix "optikk.ingest" + signal "logs" → "optikk.dlq.logs".
func DLQTopic(prefix, signal string) string {
	p := normalizeIngestPrefix(prefix)
	// Replace ".ingest" segment with ".dlq" so both halves derive from the
	// same operator-configured root.
	base := strings.TrimSuffix(p, ".ingest")
	if base == p {
		base = p + ".dlq"
	} else {
		base = base + ".dlq"
	}
	return base + "." + signal
}

// GroupID builds the canonical per-signal, per-role consumer group id.
func GroupID(base, signal, role string) string {
	b := strings.TrimSpace(base)
	if b == "" {
		b = "optikk-ingest"
	}
	return fmt.Sprintf("%s.%s.%s", b, signal, role)
}

func normalizeIngestPrefix(prefix string) string {
	p := strings.TrimSpace(prefix)
	if p == "" {
		p = "optikk.ingest"
	}
	return p
}
