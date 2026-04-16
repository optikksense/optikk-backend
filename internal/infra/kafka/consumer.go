package kafka

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
	googlepb "google.golang.org/protobuf/proto"
)

// ConsumerRunner manages a Kafka polling loop for a specific proto message type.
type ConsumerRunner[T googlepb.Message] struct {
	Signal string
	Client *kgo.Client
	NewMsg func() T
	OnRows func(ctx context.Context, rows []T) error

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

// Start launches the polling loop in a background goroutine.
func (c *ConsumerRunner[T]) Start(ctx context.Context) {
	runCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.run(runCtx)
	}()
}

// Stop shuts down the runner and waits for the loop to exit.
func (c *ConsumerRunner[T]) Stop() error {
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()
	if c.Client != nil {
		c.Client.Close()
	}
	return nil
}

func (c *ConsumerRunner[T]) run(ctx context.Context) {
	for {
		fetches := c.Client.PollFetches(ctx)
		if err := fetches.Err0(); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, kgo.ErrClientClosed) {
				return
			}
		}

		fetches.EachError(func(topic string, p int32, err error) {
			if err != nil {
				slog.Error("kafka: fetch error",
					slog.String("signal", c.Signal),
					slog.String("topic", topic),
					slog.Int("partition", int(p)),
					slog.Any("error", err))
			}
		})

		rows := c.extractRows(fetches)
		if len(rows) > 0 {
			if err := c.OnRows(ctx, rows); err != nil {
				slog.Error("kafka: process rows failed",
					slog.String("signal", c.Signal),
					slog.Any("error", err))
			}
		}

		// Commit after processing
		if err := c.Client.CommitUncommittedOffsets(ctx); err != nil {
			if !errors.Is(err, context.Canceled) && !errors.Is(err, kgo.ErrClientClosed) {
				slog.Warn("kafka: commit offsets failed",
					slog.String("signal", c.Signal),
					slog.Any("error", err))
			}
		}
	}
}

func (c *ConsumerRunner[T]) extractRows(fetches kgo.Fetches) []T {
	iter := fetches.RecordIter()
	var rows []T
	for !iter.Done() {
		r := iter.Next()
		msg := c.NewMsg()
		if err := googlepb.Unmarshal(r.Value, msg); err != nil {
			slog.Error("kafka: proto unmarshal failed",
				slog.String("signal", c.Signal),
				slog.Any("error", err))
			continue
		}
		rows = append(rows, msg)
	}
	return rows
}
