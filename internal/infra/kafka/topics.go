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

// EnsureTopics creates topics if they do not exist (idempotent for existing topics).
func EnsureTopics(brokers []string, topics []string) error {
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

	for _, t := range topics {
		t = strings.TrimSpace(t)
		if t == "" {
			continue
		}
		_, err := adm.CreateTopic(ctx, 1, 1, nil, t)
		if err == nil {
			continue
		}
		if errors.Is(err, kerr.TopicAlreadyExists) {
			continue
		}
		return fmt.Errorf("kafka: create topic %q: %w", t, err)
	}
	return nil
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
