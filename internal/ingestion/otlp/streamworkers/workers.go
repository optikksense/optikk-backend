package streamworkers

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp"
	otlplogs "github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/logs"
	otlpmetrics "github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/metrics"
	otlpspans "github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/spans"
	"github.com/Optikk-Org/optikk-backend/internal/modules/livetail"
)

// Workers runs in-memory queue consumers for OTLP ingest to ClickHouse
// and the real-time Live Tail Hub.
//
// Persistence contract (Phase 1):
//   - One AckableBatch from the dispatcher == one ClickHouse insert.
//   - Insert success → Ack(nil) commits the source Kafka offset.
//   - Insert failure → Ack(err) routes the record to the per-signal DLQ
//     topic and then commits the offset so the partition is not blocked.
//   - No in-process retry. The DLQ is the retry surface (operators replay).
//   - Each insert carries AckableBatch.DedupToken as `insert_deduplication_token`
//     so redelivered records (crash between flush and commit) collapse to a
//     single physical write.
type Workers struct {
	ch               clickhouse.Conn
	logDispatcher    ingestion.Dispatcher[*otlplogs.LogRow]
	spanDispatcher   ingestion.Dispatcher[*otlpspans.SpanRow]
	metricDispatcher ingestion.Dispatcher[*otlpmetrics.MetricRow]
	hub              livetail.Hub

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewWorkers constructs the persistence + streaming workers.
// The batchMaxRows / batchMaxWait parameters are accepted for backward
// compatibility with existing call sites but are unused — persistence is now
// per-record to preserve Kafka offset ack semantics.
func NewWorkers(
	ch clickhouse.Conn,
	ld ingestion.Dispatcher[*otlplogs.LogRow],
	sd ingestion.Dispatcher[*otlpspans.SpanRow],
	md ingestion.Dispatcher[*otlpmetrics.MetricRow],
	hub livetail.Hub,
	_ int,
	_ time.Duration,
) *Workers {
	return &Workers{
		ch:               ch,
		logDispatcher:    ld,
		spanDispatcher:   sd,
		metricDispatcher: md,
		hub:              hub,
	}
}

func (w *Workers) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel

	logFlusher := otlp.NewCHFlusher[*otlplogs.LogRow](w.ch, "observability.logs", otlplogs.LogColumns)
	metricFlusher := otlp.NewCHFlusher[*otlpmetrics.MetricRow](w.ch, "observability.metrics", otlpmetrics.MetricColumns)
	spanFlusher := otlp.NewCHFlusher[*otlpspans.SpanRow](w.ch, "observability.spans", otlpspans.SpanColumns)

	w.wg.Add(4)
	go func() { defer w.wg.Done(); RunPersistence(ctx, "logs", w.logDispatcher, logFlusher.Flush) }()
	go func() { defer w.wg.Done(); RunPersistence(ctx, "spans", w.spanDispatcher, spanFlusher.Flush) }()
	go func() { defer w.wg.Done(); RunPersistence(ctx, "metrics", w.metricDispatcher, metricFlusher.Flush) }()
	go func() { defer w.wg.Done(); w.runStreaming(ctx) }()
}

func (w *Workers) Stop() error {
	if w.cancel != nil {
		w.cancel()
	}
	w.wg.Wait()
	return nil
}

// FlushFunc is the narrow contract RunPersistence depends on. The concrete
// implementation is otlp.CHFlusher[T].Flush; tests can substitute a fake.
type FlushFunc[T any] func(rows []T, dedupToken string) error

// RunPersistence drains a single signal's persistence channel. Each
// AckableBatch is flushed via flush and acked with the flush result: Ack(nil)
// on success (Kafka offset commits), Ack(err) on failure (DLQ produce + commit).
// Exported for tests in tests/ingestion/.
func RunPersistence[T any](ctx context.Context, signal string, d ingestion.Dispatcher[T], flush FlushFunc[T]) {
	ch := d.Persistence()
	for {
		select {
		case <-ctx.Done():
			return
		case batch, ok := <-ch:
			if !ok {
				return
			}
			err := flush(batch.Batch.Rows, batch.DedupToken)
			if err != nil {
				slog.Error("ingest: flush failed; routing to DLQ",
					slog.String("signal", signal),
					slog.Int("rows", len(batch.Batch.Rows)),
					slog.Any("error", err))
			} else {
				slog.Info("ingest: flushed",
					slog.String("signal", signal),
					slog.Int("rows", len(batch.Batch.Rows)))
			}
			if batch.Ack != nil {
				batch.Ack(err)
			}
		}
	}
}

func (w *Workers) runStreaming(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case batch, ok := <-w.logDispatcher.Streaming():
			if !ok {
				return
			}
			for _, row := range batch.Rows {
				if payload, ok := otlplogs.LiveTailStreamPayload(row); ok && payload != nil {
					w.hub.Publish(batch.TeamID, payload)
				}
			}
		case batch, ok := <-w.spanDispatcher.Streaming():
			if !ok {
				return
			}
			for _, row := range batch.Rows {
				data, err := otlpspans.SpanLiveTailStreamPayload(row, time.Now().UnixMilli())
				if err == nil && data != nil {
					w.hub.Publish(batch.TeamID, data)
				}
			}
		}
	}
}
