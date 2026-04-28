package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/config"
	"github.com/Optikk-Org/optikk-backend/internal/infra/kafka/ingest"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/logs/dlq"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/logs/schema"
)

// NewWriter builds the retrying CH writer for the logs signal. Wraps the
// shared ingest.Writer generic with a CH-specific BatchSender closure and
// routes exhausted batches to the DLQ topic via the injected producer.
// WriterConfig is derived from the per-signal IngestPipelineConfig so retry
// budget + timeout are YAML-tunable without a code change.
func NewWriter(ch clickhouse.Conn, dlqP *dlq.Producer, pc config.IngestPipelineConfig) *ingest.Writer[*schema.Row] {
	query := "INSERT INTO " + schema.CHTable + " (" + strings.Join(schema.Columns, ", ") + ")"
	chCtx := ingest.WithIngestSettings(pc)
	send := func(ctx context.Context, items []*schema.Row) error {
		return insertBatch(chCtx(ctx), ch, query, items)
	}
	var sink ingest.DLQSink[*schema.Row]
	if dlqP != nil {
		sink = func(ctx context.Context, items []*schema.Row, reason error) {
			dlqP.Publish(ctx, items, reason)
		}
	}
	return ingest.NewWriter[*schema.Row]("logs", ingest.WriterCfgFromPipeline(pc), send, sink)
}

// insertBatch prepares one CH batch, appends every row, and sends. A fresh
// batch is prepared each call so retries do not accumulate rows. The
// per-batch info log was removed in favor of the flush_duration histogram.
func insertBatch(ctx context.Context, ch clickhouse.Conn, query string, rows []*schema.Row) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := ch.PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("prepare: %w", err)
	}
	for i, row := range rows {
		if err := batch.Append(schema.ChValues(row)...); err != nil {
			return fmt.Errorf("append at index %d: %w", i, err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send: %w", err)
	}
	slog.DebugContext(ctx, "logs writer: batch sent successfully", slog.Int("row_count", len(rows)))
	return nil
}
