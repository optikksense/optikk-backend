package indexer

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// Emitter is the sink the Assembler writes completed TraceIndexRow entries
// into. The primary impl is CHEmitter (writes to observability.traces_index);
// tests can stub a channel-backed impl.
type Emitter interface {
	Emit(ctx context.Context, row TraceIndexRow) error
}

// CHTable is the ClickHouse destination table for traces_index summaries.
const CHTable = "observability.traces_index"

var chColumns = []string{
	"team_id", "ts_bucket_start", "trace_id",
	"start_ms", "end_ms", "duration_ns",
	"root_service", "root_operation", "root_status",
	"root_http_method", "root_http_status",
	"span_count", "has_error", "error_count",
	"service_set", "peer_service_set", "error_fingerprint",
	"truncated", "last_seen_ms",
}

// CHEmitter wraps a clickhouse.Conn and appends rows one-at-a-time. The
// assembler calls Emit per completed trace; bulk batching lives inside the
// clickhouse-go async insert path (enabled at client construction).
type CHEmitter struct {
	ch    clickhouse.Conn
	query string
}

func NewCHEmitter(ch clickhouse.Conn) *CHEmitter {
	query := "INSERT INTO " + CHTable + " ("
	for i, c := range chColumns {
		if i > 0 {
			query += ", "
		}
		query += c
	}
	query += ")"
	return &CHEmitter{ch: ch, query: query}
}

// Emit appends one row. clickhouse-go handles batching under the hood when
// the client is constructed with async inserts; otherwise the driver
// prepares a batch-of-one (slow, fine for low-volume).
func (e *CHEmitter) Emit(ctx context.Context, row TraceIndexRow) error {
	batch, err := e.ch.PrepareBatch(ctx, e.query)
	if err != nil {
		return fmt.Errorf("indexer CH prepare: %w", err)
	}
	if err := batch.Append(rowValues(row)...); err != nil {
		return fmt.Errorf("indexer CH append: %w", err)
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("indexer CH send: %w", err)
	}
	return nil
}

func rowValues(r TraceIndexRow) []any {
	return []any{
		r.TeamID, r.TsBucketStart, r.TraceID,
		r.StartMs, r.EndMs, r.DurationNs,
		r.RootService, r.RootOperation, r.RootStatus,
		r.RootHTTPMethod, r.RootHTTPStatus,
		r.SpanCount, r.HasError, r.ErrorCount,
		r.ServiceSet, r.PeerServiceSet, r.ErrorFp,
		r.Truncated, r.LastSeenMs,
	}
}

// LoggingEmitter is a no-op sink used when CH is unavailable (tests /
// offline reprocessing). Keeps the Assembler loop wired without writes.
type LoggingEmitter struct{}

func (LoggingEmitter) Emit(_ context.Context, row TraceIndexRow) error {
	slog.Debug("indexer: emit (logging only)",
		slog.String("trace_id", row.TraceID),
		slog.Uint64("span_count", uint64(row.SpanCount)),
		slog.Bool("has_error", row.HasError),
		slog.Bool("truncated", row.Truncated))
	return nil
}
