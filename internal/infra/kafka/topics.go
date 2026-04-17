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

// IngestTopicNames returns full topic names for logs, spans, and metrics under prefix,
// plus a per-signal DLQ sibling for batches that fail CH insert.
func IngestTopicNames(prefix string) []string {
	p := normalizeIngestPrefix(prefix)
	return []string{
		p + ".logs",
		p + ".spans",
		p + ".metrics",
		p + ".logs.dlq",
		p + ".spans.dlq",
		p + ".metrics.dlq",
	}
}

// DLQTopicFor returns the DLQ topic name paired with a primary ingest topic.
func DLQTopicFor(topic string) string {
	t := strings.TrimSpace(topic)
	if t == "" {
		return ""
	}
	return t + ".dlq"
}

func normalizeIngestPrefix(prefix string) string {
	p := strings.TrimSpace(prefix)
	if p == "" {
		p = "optikk.ingest"
	}
	return p
}
