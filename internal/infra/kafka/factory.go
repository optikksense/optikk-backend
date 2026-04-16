package kafka

import (
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// InitIngestClients handles the start-time initialization for a producer and two specialized consumers.
func InitIngestClients(brokers []string, groupBase, topic, signal string) (p, s, r *kgo.Client, err error) {
	if err := ValidateConfig(brokers, groupBase, topic, signal); err != nil {
		return nil, nil, nil, err
	}

	if p, err = initProducer(brokers, signal); err != nil {
		return nil, nil, nil, err
	}
	if s, err = initStreamConsumer(brokers, groupBase, topic, signal); err != nil {
		p.Close()
		return nil, nil, nil, err
	}
	if r, err = initPersistConsumer(brokers, groupBase, topic, signal); err != nil {
		p.Close()
		s.Close()
		return nil, nil, nil, err
	}
	return p, s, r, nil
}

// ValidateConfig performs basic validation on the Kafka configuration parameters.
func ValidateConfig(brokers []string, groupBase, topic, signal string) error {
	if len(brokers) == 0 {
		return fmt.Errorf("kafka: no brokers for %s", signal)
	}
	if strings.TrimSpace(topic) == "" {
		return fmt.Errorf("kafka: empty topic for %s", signal)
	}
	if strings.TrimSpace(groupBase) == "" {
		return fmt.Errorf("kafka: empty consumer group base")
	}
	return nil
}

func initProducer(brokers []string, signal string) (*kgo.Client, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, fmt.Errorf("kafka: producer client %s: %w", signal, err)
	}
	return cl, nil
}

func initStreamConsumer(brokers []string, groupBase, topic, signal string) (*kgo.Client, error) {
	cg := strings.TrimSpace(groupBase) + "-stream-" + signal
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(cg),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxWait(500*time.Millisecond),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka: stream consumer client %s: %w", signal, err)
	}
	return cl, nil
}

func initPersistConsumer(brokers []string, groupBase, topic, signal string) (*kgo.Client, error) {
	cg := strings.TrimSpace(groupBase) + "-persist-" + signal
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(cg),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxWait(5*time.Second),
		kgo.FetchMinBytes(1_000_000),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka: persist consumer client %s: %w", signal, err)
	}
	return cl, nil
}
