package otlp

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/internal/ingest"
)

// Task List:
// - `[x]` Add drop detection and logging in `dispatcher.go`
// - `[/]` Update `CHFlusher` to return errors for better diagnostics in `flusher.go`
// - `[ ]` Implement robust time-and-size-based batching in `workers.go`
// - `[ ]` Verify build and monitoring logs

type CHFlusher struct {
	conn        clickhouse.Conn
	queryPrefix string
	table       string
}

func NewCHFlusher(conn clickhouse.Conn, table string, columns []string) *CHFlusher {
	return &CHFlusher{
		conn:        conn,
		queryPrefix: "INSERT INTO " + table + " (" + strings.Join(columns, ", ") + ")",
		table:       table,
	}
}

func (f *CHFlusher) Flush(batch []ingest.Row) error {
	if len(batch) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	b, err := f.conn.PrepareBatch(ctx, f.queryPrefix)
	if err != nil {
		slog.Error("ingest: prepare failed", slog.String("table", f.table), slog.Any("error", err))
		return err
	}
	for i, row := range batch {
		if err := b.Append(row.Values...); err != nil {
			slog.Error("ingest: append failed", slog.String("table", f.table), slog.Int("index", i), slog.Any("error", err))
			return err
		}
	}
	if err := b.Send(); err != nil {
		slog.Error("ingest: send failed", slog.String("table", f.table), slog.Any("error", err))
		return err
	}
	return nil
}
