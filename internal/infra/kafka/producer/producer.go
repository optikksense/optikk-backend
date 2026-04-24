// Package producer wraps *kgo.Client with a narrow PublishBatch / Produce
// surface used by every ingest path.
package producer

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

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

// Produce publishes one record synchronously. Kept for non-ingest callers
// (e.g. DLQ writer uses PublishBatch). Prefer PublishBatch on ingest paths.
func (p *Producer) Produce(ctx context.Context, topic string, key, value []byte) error {
	if p == nil || p.client == nil {
		return fmt.Errorf("kafka: nil producer")
	}
	record := &kgo.Record{Topic: topic, Key: key, Value: value}
	res := p.client.ProduceSync(ctx, record)
	return res.FirstErr()
}

// PublishBatch asynchronously produces every record and waits for all promises
// to resolve before returning. The first error encountered is returned; other
// per-record errors are collapsed to the same error so the caller can retry
// the batch as a whole (at-least-once semantics — CH dedup handles replays).
//
// Records may name any topic; typically every record in one call is bound for
// the same topic but the API does not force it.
func (p *Producer) PublishBatch(ctx context.Context, records []*kgo.Record) error {
	if p == nil || p.client == nil {
		return fmt.Errorf("kafka: nil producer")
	}
	if len(records) == 0 {
		return nil
	}
	var (
		wg      sync.WaitGroup
		firstEr atomic.Value // error
	)
	wg.Add(len(records))
	for _, r := range records {
		p.client.Produce(ctx, r, func(_ *kgo.Record, err error) {
			defer wg.Done()
			if err != nil {
				firstEr.CompareAndSwap(nil, err)
			}
		})
	}
	wg.Wait()
	if v := firstEr.Load(); v != nil {
		if err, ok := v.(error); ok {
			return err
		}
	}
	return nil
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
