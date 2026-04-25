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
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/spans/enrich"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/spans/indexer"
)

// NewWriter builds the retrying CH writer for the spans signal. Wraps
// ingest.Writer with a CH-specific BatchSender + DLQ sink. When an assembler
// is supplied, every successfully-persisted span is folded into the
// per-trace summary for traces_index.
func NewWriter(ch clickhouse.Conn, dlqP *dlq.Producer, asm *indexer.Assembler, pc config.IngestPipelineConfig) *ingest.Writer[*schema.Row] {
	query := "INSERT INTO " + schema.CHTable + " (" + strings.Join(schema.Columns, ", ") + ")"
	chCtx := ingest.WithAsyncInsert(pc)
	send := func(ctx context.Context, items []*schema.Row) error {
		if err := insertBatch(chCtx(ctx), ch, query, items); err != nil {
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

// rowToSpan projects the protobuf Row into the indexer's neutral Span shape
// so the indexer package stays import-free of the spans package.
func rowToSpan(r *schema.Row) indexer.Span {
	attrs := r.GetAttributes()
	isRoot := enrich.ZeroSpanID(r.GetParentSpanId()) == ""
	startMs := r.GetTimestampNs() / 1_000_000
	endMs := startMs + int64(r.GetDurationNano()/1_000_000) //nolint:gosec // duration fits int64
	return indexer.Span{
		TeamID:        r.GetTeamId(),
		TraceID:       enrich.ZeroTraceID(r.GetTraceId()),
		SpanID:        enrich.ZeroSpanID(r.GetSpanId()),
		ParentSpanID:  enrich.ZeroSpanID(r.GetParentSpanId()),
		Service:       schema.ServiceName(r),
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
