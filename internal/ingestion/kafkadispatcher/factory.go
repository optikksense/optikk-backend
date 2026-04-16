package kafkadispatcher

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/ingestion"
	"github.com/twmb/franz-go/pkg/kgo"
)

// New starts a stream consumer, a persist consumer, and a producer client.
func New[T any](brokers []string, groupBase, topic, signal string, handlers ingestion.Handlers[T]) (*KafkaDispatcher[T], error) {
	if err := validateConfig(brokers, groupBase, topic, signal); err != nil {
		return nil, err
	}
	topic = strings.TrimSpace(topic)

	producer, stream, persist, err := initAllClients(brokers, groupBase, topic, signal)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	d := &KafkaDispatcher[T]{
		signal:          signal,
		topic:           topic,
		producer:        producer,
		consumerStream:  stream,
		consumerPersist: persist,
		handlers:        handlers,
		cancel:          cancel,
	}
	d.startLoops(ctx)
	return d, nil
}

func initAllClients(brokers []string, groupBase, topic, signal string) (p, s, r *kgo.Client, err error) {
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

func validateConfig(brokers []string, groupBase, topic, signal string) error {
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
	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, fmt.Errorf("kafka: producer client %s: %w", signal, err)
	}
	return producer, nil
}

func initStreamConsumer(brokers []string, groupBase, topic, signal string) (*kgo.Client, error) {
	cg := strings.TrimSpace(groupBase) + "-stream-" + signal
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(cg),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxWait(500*time.Millisecond),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka: stream consumer client %s: %w", signal, err)
	}
	return consumer, nil
}

func initPersistConsumer(brokers []string, groupBase, topic, signal string) (*kgo.Client, error) {
	cg := strings.TrimSpace(groupBase) + "-persist-" + signal
	consumer, err := kgo.NewClient(
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
	return consumer, nil
}
