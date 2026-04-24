package indexer

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

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
	"environment", "truncated", "last_seen_ms",
}

// CHEmitter wraps a clickhouse.Conn and appends rows one-at-a-time. The
// assembler calls Emit per completed trace; bulk batching lives inside the
// clickhouse-go async insert path (enabled at client construction).
type CHEmitter struct {
	ch	clickhouse.Conn
	query	string
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

// EmitBatch appends multiple rows in a single PrepareBatch/Send round-trip.
// Used by the assembler drain path on shutdown to avoid per-row overhead.
func (e *CHEmitter) EmitBatch(ctx context.Context, rows []TraceIndexRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := e.ch.PrepareBatch(ctx, e.query)
	if err != nil {
		return fmt.Errorf("indexer CH prepare: %w", err)
	}
	for _, row := range rows {
		if err := batch.Append(rowValues(row)...); err != nil {
			return fmt.Errorf("indexer CH append: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("indexer CH send: %w", err)
	}
	return nil
}

// parseRootHTTPStatusUInt16 maps OTLP/HTTP textual status (e.g. "200", "404 Not Found") to
// traces_index.root_http_status (UInt16). Non-numeric / empty → 0.
func parseRootHTTPStatusUInt16(s string) uint16 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	i := 0
	for i < len(s) && (s[i] < '0' || s[i] > '9') {
		i++
	}
	s = s[i:]
	if s == "" {
		return 0
	}
	var n uint64
	for _, c := range s {
		if c < '0' || c > '9' {
			break
		}
		n = n*10 + uint64(c-'0')
		if n > 65535 {
			return 65535
		}
	}
	return uint16(n)
}

func rowValues(r TraceIndexRow) []any {
	return []any{
		r.TeamID, r.TsBucketStart, r.TraceID,
		r.StartMs, r.EndMs, r.DurationNs,
		r.RootService, r.RootOperation, r.RootStatus,
		r.RootHTTPMethod, parseRootHTTPStatusUInt16(r.RootHTTPStatus),
		r.SpanCount, r.HasError, r.ErrorCount,
		r.ServiceSet, r.PeerServiceSet, r.ErrorFp,
		r.Environment, r.Truncated, uint64(r.LastSeenMs.UnixMilli()),
	}
}

// LoggingEmitter is a no-op sink used when CH is unavailable (tests /
// offline reprocessing). Keeps the Assembler loop wired without writes.
type LoggingEmitter struct{}

func (LoggingEmitter) Emit(ctx context.Context, row TraceIndexRow) error {
	slog.DebugContext(ctx, "indexer: emit (logging only)",
		slog.String("trace_id", row.TraceID),
		slog.Uint64("span_count", uint64(row.SpanCount)),
		slog.Bool("has_error", row.HasError),
		slog.Bool("truncated", row.Truncated))
	return nil
}
