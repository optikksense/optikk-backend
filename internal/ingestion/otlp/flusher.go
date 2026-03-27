package otlp

import (
	"context"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/infra/logger"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/internal/ingest"
	"go.uber.org/zap"
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
		logger.L().Error("ingest: prepare failed", zap.String("table", f.table), zap.Error(err))
		return
	}
	for _, row := range batch {
		if err := b.Append(row.Values...); err != nil {
			logger.L().Error("ingest: append failed", zap.String("table", f.table), zap.Error(err))
			return
		}
	}
	if err := b.Send(); err != nil {
		logger.L().Error("ingest: send failed", zap.String("table", f.table), zap.Error(err))
		return
	}
	logger.L().Info("ingest: flushed", zap.Int("rows", len(batch)), zap.String("table", f.table), zap.Duration("took", time.Since(start)))
}
