package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer is a thin wrapper around *kgo.Client that exposes only what the
// per-signal producer.go files need. All ingest producers share one underlying
// client (one TCP connection pool per broker) — see newProducerFor callers
// in server/infra.go.
type Producer struct {
	client *kgo.Client
}

func NewProducer(client *kgo.Client) *Producer {
	return &Producer{client: client}
}

// Produce publishes one record synchronously. Synchronous is deliberate:
// the handler that calls us is on the hot path of an OTLP Export RPC and must
// know whether the record was durably accepted before returning OK to the
// client. Batching still happens at the broker-protocol layer via linger.
func (p *Producer) Produce(ctx context.Context, topic string, key, value []byte) error {
	if p == nil || p.client == nil {
		return fmt.Errorf("kafka: nil producer")
	}
	res := p.client.ProduceSync(ctx, &kgo.Record{
		Topic: topic,
		Key:   key,
		Value: value,
	})
	return res.FirstErr()
}

// Flush waits for all buffered records to be acknowledged. Called on graceful
// shutdown so the process does not drop in-flight ingest on Ctrl-C.
func (p *Producer) Flush(ctx context.Context) error {
	if p == nil || p.client == nil {
		return nil
	}
	return p.client.Flush(ctx)
}

// Close releases the underlying connections. Safe to call more than once.
func (p *Producer) Close() {
	if p == nil || p.client == nil {
		return
	}
	p.client.Close()
}
