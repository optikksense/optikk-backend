package ingest

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// WriterConfig bounds retry behavior for the CH batch send.
type WriterConfig struct {
	MaxAttempts	int		// total tries (default 5)
	BaseBackoff	time.Duration	// first backoff (default 100ms)
	MaxBackoff	time.Duration	// cap (default 5s)
	Timeout		time.Duration	// per-attempt ctx timeout (default 30s)
}

// DefaultWriterConfig returns the plan-mandated retry schedule: 5 attempts,
// 100ms → 5s exponential backoff, 30s per-attempt timeout.
func DefaultWriterConfig() WriterConfig {
	return WriterConfig{
		MaxAttempts:	5,
		BaseBackoff:	100 * time.Millisecond,
		MaxBackoff:	5 * time.Second,
		Timeout:	30 * time.Second,
	}
}

// BatchSender is the signal-specific CH insert closure. Return nil on success.
// Callers are expected to construct a fresh CH batch inside the closure each
// call so retries are safe.
type BatchSender[T any] func(ctx context.Context, items []T) error

// DLQSink is called once when every retry fails. Implementation writes the
// items + reason to the signal's DLQ topic and must be non-blocking w.r.t. the
// Kafka client (a plain kgo.Produce is enough). Errors from the sink are logged
// but do not propagate — a dead DLQ cannot stop ingest.
type DLQSink[T any] func(ctx context.Context, items []T, reason error)

// Writer ties retry + DLQ around a BatchSender for a single worker.
type Writer[T any] struct {
	cfg	WriterConfig
	send	BatchSender[T]
	dlq	DLQSink[T]
	name	string	// "logs" | "spans" for logs
}

// NewWriter constructs a retrying writer. name labels log lines.
func NewWriter[T any](name string, cfg WriterConfig, send BatchSender[T], dlq DLQSink[T]) *Writer[T] {
	if cfg.MaxAttempts == 0 {
		cfg = DefaultWriterConfig()
	}
	return &Writer[T]{cfg: cfg, send: send, dlq: dlq, name: name}
}

// Write attempts the batch with exponential backoff; on exhaustion hands off
// to the DLQ and returns the last error so the caller can commit or abort.
// Returns nil on success OR on DLQ-handled failure (at-least-once: we do NOT
// want the batch retried forever by the consumer group).
func (w *Writer[T]) Write(ctx context.Context, items []T) error {
	if len(items) == 0 {
		return nil
	}
	var last error
	for attempt := 1; attempt <= w.cfg.MaxAttempts; attempt++ {
		if err := w.attempt(ctx, items); err == nil {
			return nil
		} else {
			last = err
			if errors.Is(err, context.Canceled) {
				return err
			}
			if attempt < w.cfg.MaxAttempts {
				sleepBackoff(ctx, w.backoff(attempt))
			}
		}
	}
	slog.ErrorContext(ctx, "ingest writer: exhausted retries; routing to DLQ",
		slog.String("signal", w.name),
		slog.Int("items", len(items)),
		slog.Any("error", last))
	if w.dlq != nil {
		w.dlq(ctx, items, last)
	}
	return nil
}

func (w *Writer[T]) attempt(ctx context.Context, items []T) error {
	attemptCtx, cancel := context.WithTimeout(ctx, w.cfg.Timeout)
	defer cancel()
	return w.send(attemptCtx, items)
}

// backoff returns the sleep for attempt N (1-based): base * 2^(N-1), capped.
func (w *Writer[T]) backoff(attempt int) time.Duration {
	d := w.cfg.BaseBackoff << (attempt - 1)
	if d <= 0 || d > w.cfg.MaxBackoff {
		return w.cfg.MaxBackoff
	}
	return d
}

func sleepBackoff(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
