package streamworkers

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/config"
	"github.com/Optikk-Org/optikk-backend/internal/infra/livetail"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/internal/ingest"
	otlplogs "github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/logs"
	otlpmetrics "github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/metrics"
	otlpspans "github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/spans"
)

const (
	// batchMaxRows is the maximum number of rows we aggregate before flushing to ClickHouse.
	batchMaxRows = 5000
	// batchMaxWait is the maximum time we wait before flushing an incomplete batch.
	batchMaxWait = 5 * time.Second
)

// Workers runs in-memory queue consumers for OTLP ingest to ClickHouse
// and the real-time Live Tail Hub.
type Workers struct {
	ch         clickhouse.Conn
	cfg        config.Config
	dispatcher *otlp.Dispatcher
	hub        *livetail.Hub

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewWorkers(ch clickhouse.Conn, d *otlp.Dispatcher, hub *livetail.Hub, cfg config.Config) *Workers {
	return &Workers{ch: ch, dispatcher: d, hub: hub, cfg: cfg}
}

// Start launches background consumers.
func (w *Workers) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel

	w.wg.Add(2)
	// 1. Persistence Worker (Aggregates and flushes to ClickHouse)
	go func() { defer w.wg.Done(); w.runPersistence(ctx) }()

	// 2. Streaming Worker (Fans out to the Live Tail Hub)
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
	logFlusher := otlp.NewCHFlusher(w.ch, "observability.logs", otlplogs.LogColumns)
	metricFlusher := otlp.NewCHFlusher(w.ch, "observability.metrics", otlpmetrics.MetricColumns)
	spanFlusher := otlp.NewCHFlusher(w.ch, "observability.spans", otlpspans.SpanColumns)

	// Local buffers for aggregation
	logBuf := make([]ingest.Row, 0, batchMaxRows)
	metricBuf := make([]ingest.Row, 0, batchMaxRows)
	spanBuf := make([]ingest.Row, 0, batchMaxRows)

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
			flush() // final flush on shutdown
			return
		case <-ticker.C:
			flush()
		case batch, ok := <-w.dispatcher.PersistenceChan:
			if !ok {
				flush()
				return
			}

			switch batch.Signal {
			case otlp.SignalLog:
				logBuf = append(logBuf, batch.Rows...)
				if len(logBuf) >= batchMaxRows {
					if err := logFlusher.Flush(logBuf); err == nil {
						slog.Info("ingest: flushed logs (max size)", slog.Int("rows", len(logBuf)))
						logBuf = logBuf[:0]
					}
				}
			case otlp.SignalMetric:
				metricBuf = append(metricBuf, batch.Rows...)
				if len(metricBuf) >= batchMaxRows {
					if err := metricFlusher.Flush(metricBuf); err == nil {
						slog.Info("ingest: flushed metrics (max size)", slog.Int("rows", len(metricBuf)))
						metricBuf = metricBuf[:0]
					}
				}
			case otlp.SignalSpan:
				spanBuf = append(spanBuf, batch.Rows...)
				if len(spanBuf) >= batchMaxRows {
					if err := spanFlusher.Flush(spanBuf); err == nil {
						slog.Info("ingest: flushed spans (max size)", slog.Int("rows", len(spanBuf)))
						spanBuf = spanBuf[:0]
					}
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
		case batch, ok := <-w.dispatcher.StreamingChan:
			if !ok {
				return
			}
			if len(batch.Rows) == 0 {
				continue
			}

			// Only logs and spans are currently streamed to Live Tail
			if batch.Signal == otlp.SignalLog || batch.Signal == otlp.SignalSpan {
				for _, row := range batch.Rows {
					var payload any
					var ok bool
					if batch.Signal == otlp.SignalLog {
						payload, ok = otlplogs.LiveTailStreamPayload(row)
					} else {
						// SpanLiveTailStreamJSON returns ([]byte, error)
						data, err := otlpspans.SpanLiveTailStreamPayload(row, time.Now().UnixMilli())
						if err == nil {
							payload = data
							ok = true
						}
					}

					if ok && payload != nil {
						w.hub.Publish(batch.TeamID, payload)
					}
				}
			}
		}
	}
}
