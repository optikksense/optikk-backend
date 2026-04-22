package logs

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/infra/kafka/ingest"
)

// NewWriter builds the retrying CH writer for the logs signal. Wraps the
// shared ingest.Writer generic with a CH-specific BatchSender closure and
// routes exhausted batches to the DLQ topic via the injected producer.
func NewWriter(ch clickhouse.Conn, dlq *DLQProducer) *ingest.Writer[*Row] {
	query := "INSERT INTO " + CHTable + " (" + strings.Join(Columns, ", ") + ")"
	send := func(ctx context.Context, items []*Row) error {
		return insertBatch(ctx, ch, query, items)
	}
	var sink ingest.DLQSink[*Row]
	if dlq != nil {
		sink = func(ctx context.Context, items []*Row, reason error) {
			if err := dlq.Publish(ctx, items, reason); err != nil {
				slog.Warn("logs writer: DLQ publish failed",
					slog.Int("items", len(items)), slog.Any("error", err))
			}
		}
	}
	return ingest.NewWriter[*Row]("logs", ingest.DefaultWriterConfig(), send, sink)
}

// insertBatch prepares one CH batch, appends every row, and sends. A fresh
// batch is prepared each call so retries do not accumulate rows.
func insertBatch(ctx context.Context, ch clickhouse.Conn, query string, rows []*Row) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := ch.PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("prepare: %w", err)
	}
	for _, row := range rows {
		if err := batch.Append(chValues(row)...); err != nil {
			return fmt.Errorf("append: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send: %w", err)
	}
	slog.Info("logs writer: flushed", slog.Int("rows", len(rows)))
	return nil
}
