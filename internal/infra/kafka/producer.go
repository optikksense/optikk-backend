package kafka

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer is a thin wrapper around *kgo.Client. One instance is shared across
// all signal producers — kgo batches concurrent Produce calls on the same
// client into broker-level batches under linger / batch_max_bytes.
type Producer struct {
	client *kgo.Client
}

func NewProducer(client *kgo.Client) *Producer { return &Producer{client: client} }

// PublishBatch produces every record asynchronously and waits for all acks.
// First error wins and is returned to the caller. Callers retry the whole
// batch as a unit (CH dedup window absorbs replays).
func (p *Producer) PublishBatch(ctx context.Context, records []*kgo.Record) error {
	if len(records) == 0 {
		return nil
	}
	var (
		wg       sync.WaitGroup
		firstErr atomic.Value
	)
	wg.Add(len(records))
	for _, r := range records {
		p.client.Produce(ctx, r, func(_ *kgo.Record, err error) {
			defer wg.Done()
			if err != nil {
				firstErr.CompareAndSwap(nil, err)
			}
		})
	}
	wg.Wait()
	if v := firstErr.Load(); v != nil {
		if err, ok := v.(error); ok {
			return err
		}
	}
	return nil
}

// PublishSync produces one record and waits for the ack. Used by low-rate
// paths (DLQ) that publish individual records.
func (p *Producer) PublishSync(ctx context.Context, rec *kgo.Record) error {
	return p.client.ProduceSync(ctx, rec).FirstErr()
}

func (p *Producer) Flush(ctx context.Context) error { return p.client.Flush(ctx) }
func (p *Producer) Close()                          { p.client.Close() }
func (p *Producer) Client() *kgo.Client             { return p.client }
