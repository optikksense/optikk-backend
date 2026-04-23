package spans

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/infra/kafka/ingest"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/spans/enrich"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/spans/indexer"
)

// NewWriter builds the retrying CH writer for the spans signal. Wraps
// ingest.Writer with a CH-specific BatchSender + DLQ sink. When an
// assembler is supplied, every successfully-persisted span is folded into
// the per-trace summary for traces_index.
func NewWriter(ch clickhouse.Conn, dlq *DLQProducer, asm *indexer.Assembler) *ingest.Writer[*Row] {
	query := "INSERT INTO " + CHTable + " (" + strings.Join(Columns, ", ") + ")"
	send := func(ctx context.Context, items []*Row) error {
		if err := insertBatch(ctx, ch, query, items); err != nil {
			return err
		}
		// Feed the indexer only after the CH write commits so a failed
		// insert doesn't produce a traces_index row for a span that
		// didn't persist.
		if asm != nil {
			for _, r := range items {
				asm.Observe(ctx, rowToSpan(r))
			}
		}
		return nil
	}
	var sink ingest.DLQSink[*Row]
	if dlq != nil {
		sink = func(ctx context.Context, items []*Row, reason error) {
			if err := dlq.Publish(ctx, items, reason); err != nil {
				slog.Warn("spans writer: DLQ publish failed",
					slog.Int("items", len(items)), slog.Any("error", err))
			}
		}
	}
	return ingest.NewWriter[*Row]("spans", ingest.DefaultWriterConfig(), send, sink)
}

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
	slog.Info("spans writer: flushed", slog.Int("rows", len(rows)))
	return nil
}

// rowToSpan projects the protobuf Row into the indexer's neutral Span shape
// so the indexer package stays import-free of the spans package.
func rowToSpan(r *Row) indexer.Span {
	attrs := r.GetAttributes()
	isRoot := enrich.ZeroSpanID(r.GetParentSpanId()) == ""
	startMs := r.GetTimestampNs() / 1_000_000
	endMs := startMs + int64(r.GetDurationNano()/1_000_000) //nolint:gosec // duration fits int64
	return indexer.Span{
		TeamID:        r.GetTeamId(),
		TraceID:       enrich.ZeroTraceID(r.GetTraceId()),
		SpanID:        enrich.ZeroSpanID(r.GetSpanId()),
		ParentSpanID:  enrich.ZeroSpanID(r.GetParentSpanId()),
		Service:       ServiceName(r),
		Name:          r.GetName(),
		StartMs:       startMs,
		EndMs:         endMs,
		IsRoot:        isRoot,
		IsError:       r.GetHasError(),
		HTTPMethod:    r.GetHttpMethod(),
		HTTPStatus:    r.GetResponseStatusCode(),
		StatusCode:    r.GetStatusCodeString(),
		PeerService:   attrs["peer.service"],
		ErrorFp:       r.GetExceptionType(),
		Environment:   attrs["deployment.environment"],
		TsBucketStart: r.GetTsBucketStart(),
	}
}
