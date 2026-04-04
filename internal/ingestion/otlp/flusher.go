package otlp

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/internal/ingest"
)

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

func (f *CHFlusher) Flush(batch []ingest.Row) {
	if len(batch) == 0 {
		return
	}
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	b, err := f.conn.PrepareBatch(ctx, f.queryPrefix)
	if err != nil {
		slog.Error("ingest: prepare failed", slog.String("table", f.table), slog.Any("error", err))
		return
	}
	for _, row := range batch {
		if err := b.Append(row.Values...); err != nil {
			slog.Error("ingest: append failed", slog.String("table", f.table), slog.Any("error", err))
			return
		}
	}
	if err := b.Send(); err != nil {
		slog.Error("ingest: send failed", slog.String("table", f.table), slog.Any("error", err))
		return
	}
	slog.Info("ingest: flushed", slog.Int("rows", len(batch)), slog.String("table", f.table), slog.Duration("took", time.Since(start)))
}
