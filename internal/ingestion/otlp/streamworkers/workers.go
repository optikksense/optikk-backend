package streamworkers

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp"
	otlplogs "github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/logs"
	otlpmetrics "github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/metrics"
	otlpspans "github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/spans"
	platformingestion "github.com/Optikk-Org/optikk-backend/internal/platform/ingestion"
	platformlivetail "github.com/Optikk-Org/optikk-backend/internal/platform/livetail"
)

const (
	batchMaxRows = 5000
	batchMaxWait = 5 * time.Second
)

// Workers runs in-memory queue consumers for OTLP ingest to ClickHouse
// and the real-time Live Tail Hub.
type Workers struct {
	ch               clickhouse.Conn
	logDispatcher    platformingestion.Dispatcher[*otlplogs.LogRow]
	spanDispatcher   platformingestion.Dispatcher[*otlpspans.SpanRow]
	metricDispatcher platformingestion.Dispatcher[*otlpmetrics.MetricRow]
	hub              platformlivetail.Hub

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewWorkers(ch clickhouse.Conn, ld platformingestion.Dispatcher[*otlplogs.LogRow], sd platformingestion.Dispatcher[*otlpspans.SpanRow], md platformingestion.Dispatcher[*otlpmetrics.MetricRow], hub platformlivetail.Hub) *Workers {
	return &Workers{
		ch:               ch,
		logDispatcher:    ld,
		spanDispatcher:   sd,
		metricDispatcher: md,
		hub:              hub,
	}
}

// Start launches background consumers.
func (w *Workers) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel

	w.wg.Add(2)
	go func() { defer w.wg.Done(); w.runPersistence(ctx) }()
	go func() { defer w.wg.Done(); w.runStreaming(ctx) }()
}

// Stop shuts down background consumers.
func (w *Workers) Stop() error {
	if w.cancel != nil {
		w.cancel()
	}
	w.wg.Wait()
	return nil
}

func (w *Workers) runPersistence(ctx context.Context) {
	logFlusher := otlp.NewCHFlusher[*otlplogs.LogRow](w.ch, "observability.logs", otlplogs.LogColumns)
	metricFlusher := otlp.NewCHFlusher[*otlpmetrics.MetricRow](w.ch, "observability.metrics", otlpmetrics.MetricColumns)
	spanFlusher := otlp.NewCHFlusher[*otlpspans.SpanRow](w.ch, "observability.spans", otlpspans.SpanColumns)

	logBuf := make([]*otlplogs.LogRow, 0, batchMaxRows)
	metricBuf := make([]*otlpmetrics.MetricRow, 0, batchMaxRows)
	spanBuf := make([]*otlpspans.SpanRow, 0, batchMaxRows)

	ticker := time.NewTicker(batchMaxWait)
	defer ticker.Stop()

	flush := func() {
		if len(logBuf) > 0 {
			if err := logFlusher.Flush(logBuf); err == nil {
				slog.Info("ingest: flushed logs", slog.Int("rows", len(logBuf)))
				logBuf = logBuf[:0]
			}
		}
		if len(metricBuf) > 0 {
			if err := metricFlusher.Flush(metricBuf); err == nil {
				slog.Info("ingest: flushed metrics", slog.Int("rows", len(metricBuf)))
				metricBuf = metricBuf[:0]
			}
		}
		if len(spanBuf) > 0 {
			if err := spanFlusher.Flush(spanBuf); err == nil {
				slog.Info("ingest: flushed spans", slog.Int("rows", len(spanBuf)))
				spanBuf = spanBuf[:0]
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case <-ticker.C:
			flush()
		case batch, ok := <-w.logDispatcher.Persistence():
			if !ok {
				flush()
				return
			}
			logBuf = append(logBuf, batch.Rows...)
			if len(logBuf) >= batchMaxRows {
				if err := logFlusher.Flush(logBuf); err == nil {
					logBuf = logBuf[:0]
				}
			}
		case batch, ok := <-w.spanDispatcher.Persistence():
			if !ok {
				flush()
				return
			}
			spanBuf = append(spanBuf, batch.Rows...)
			if len(spanBuf) >= batchMaxRows {
				if err := spanFlusher.Flush(spanBuf); err == nil {
					spanBuf = spanBuf[:0]
				}
			}
		case batch, ok := <-w.metricDispatcher.Persistence():
			if !ok {
				flush()
				return
			}
			metricBuf = append(metricBuf, batch.Rows...)
			if len(metricBuf) >= batchMaxRows {
				if err := metricFlusher.Flush(metricBuf); err == nil {
					metricBuf = metricBuf[:0]
				}
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
