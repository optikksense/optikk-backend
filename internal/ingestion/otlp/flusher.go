package otlp

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)


type CHFlusher[T any] struct {
	conn        clickhouse.Conn
	queryPrefix string
	table       string
}

func NewCHFlusher[T any](conn clickhouse.Conn, table string, columns []string) *CHFlusher[T] {
	return &CHFlusher[T]{
		conn:        conn,
		queryPrefix: "INSERT INTO " + table + " (" + strings.Join(columns, ", ") + ")",
		table:       table,
	}
}

func (f *CHFlusher[T]) Flush(batch []T) error {
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
		if err := b.AppendStruct(row); err != nil {
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
