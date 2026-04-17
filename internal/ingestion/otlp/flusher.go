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

// Flush inserts the batch into ClickHouse. If dedupToken is non-empty it is
// passed as `insert_deduplication_token`; ClickHouse collapses identical
// tokens into a single physical write within the table's dedup window, which
// makes at-least-once Kafka redelivery safe.
func (f *CHFlusher[T]) Flush(batch []T, dedupToken string) error {
	if len(batch) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if dedupToken != "" {
		ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
			"insert_deduplication_token": dedupToken,
		}))
	}

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
