package consumer

import (
	"context"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/config"
	"github.com/Optikk-Org/optikk-backend/internal/infra/kafka/ingest"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/spans/dlq"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/spans/schema"
)

// NewWriter builds the retrying CH writer for the spans signal. Wraps
// ingest.Writer with a CH-specific BatchSender + DLQ sink.
func NewWriter(ch clickhouse.Conn, dlqP *dlq.Producer, pc config.IngestPipelineConfig) *ingest.Writer[*schema.Row] {
	query := "INSERT INTO " + schema.CHTable + " (" + strings.Join(schema.Columns, ", ") + ")"
	chCtx := ingest.WithAsyncInsert(pc)
	send := func(ctx context.Context, items []*schema.Row) error {
		if err := insertBatch(chCtx(ctx), ch, query, items); err != nil {
			return err
		}
		return nil
	}
	var sink ingest.DLQSink[*schema.Row]
	if dlqP != nil {
		sink = func(ctx context.Context, items []*schema.Row, reason error) {
			dlqP.Publish(ctx, items, reason)
		}
	}
	return ingest.NewWriter[*schema.Row]("spans", ingest.WriterCfgFromPipeline(pc), send, sink)
}

func insertBatch(ctx context.Context, ch clickhouse.Conn, query string, rows []*schema.Row) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := ch.PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("prepare: %w", err)
	}
	for _, row := range rows {
		if err := batch.Append(schema.ChValues(row)...); err != nil {
			return fmt.Errorf("append: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send: %w", err)
	}
	return nil
}

