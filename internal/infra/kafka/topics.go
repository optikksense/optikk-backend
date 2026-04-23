package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Signal enumerates the three OTLP signals we ingest. The naming here feeds
// both topic construction and consumer-group construction so the canonical
// wire names stay in one place.
const (
	SignalLogs    = "logs"
	SignalMetrics = "metrics"
	SignalSpans   = "spans"
)

// TopicSpec describes one topic to ensure with partition count + RF overrides.
// Passed to EnsureTopics so ingest and DLQ topics can have different shapes.
type TopicSpec struct {
	Name              string
	Partitions        int32
	ReplicationFactor int16
}

// EnsureTopics creates topics if they do not exist (idempotent). Each spec
// carries its own partition count + replication factor so callers can ensure
// ingest topics at 32 partitions and DLQ topics at a smaller size from the
// same call.
func EnsureTopics(brokers []string, specs []TopicSpec) error {
	if len(brokers) == 0 {
		return fmt.Errorf("kafka: no brokers configured")
	}
	if strings.TrimSpace(brokers[0]) == "" {
		return fmt.Errorf("kafka: empty broker address")
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return fmt.Errorf("kafka: new client: %w", err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, s := range specs {
		if err := createOne(ctx, adm, s); err != nil {
			return err
		}
	}
	return nil
}

func createOne(ctx context.Context, adm *kadm.Client, s TopicSpec) error {
	name := strings.TrimSpace(s.Name)
	if name == "" {
		return nil
	}
	parts := s.Partitions
	if parts <= 0 {
		parts = 1
	}
	rf := s.ReplicationFactor
	if rf <= 0 {
		rf = 1
	}
	_, err := adm.CreateTopic(ctx, parts, rf, nil, name)
	if err == nil || errors.Is(err, kerr.TopicAlreadyExists) {
		return nil
	}
	return fmt.Errorf("kafka: create topic %q: %w", name, err)
}

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
