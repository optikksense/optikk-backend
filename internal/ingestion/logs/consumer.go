package logs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	kafkainfra "github.com/Optikk-Org/optikk-backend/internal/infra/kafka"
	"google.golang.org/protobuf/proto"
)

// Consumer is the persistence side of the log pipeline. It joins a dedicated
// consumer group (<base>.logs.persistence), drains one batch from Kafka per
// loop iteration, inserts the batch into ClickHouse, and only then commits
// the Kafka offsets for that batch. At-least-once delivery; CH primary-key
// dedup collapses any redelivery.
type Consumer struct {
	kafka *kafkainfra.Consumer
	ch    clickhouse.Conn
	query string

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewConsumer(kafka *kafkainfra.Consumer, ch clickhouse.Conn) *Consumer {
	return &Consumer{
		kafka: kafka,
		ch:    ch,
		query: "INSERT INTO " + CHTable + " (" + strings.Join(Columns, ", ") + ")",
	}
}

// Start spawns the persistence goroutine. It is safe to call once.
func (c *Consumer) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.run(ctx)
	}()
}

// Stop signals the goroutine to exit and closes the underlying Kafka client.
func (c *Consumer) Stop() error {
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()
	c.kafka.Close()
	return nil
}

func (c *Consumer) run(ctx context.Context) {
	for {
		records, err := c.kafka.PollBatch(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			slog.Warn("logs consumer: poll error", slog.Any("error", err))
			continue
		}
		if len(records) == 0 {
			continue
		}
		if err := c.flush(ctx, records); err != nil {
			slog.Error("logs consumer: flush failed; offsets NOT committed",
				slog.Int("records", len(records)), slog.Any("error", err))
			continue
		}
		if err := c.kafka.Commit(ctx, records); err != nil {
			slog.Warn("logs consumer: commit failed", slog.Any("error", err))
		}
	}
}

func (c *Consumer) flush(ctx context.Context, records []kafkainfra.Record) error {
	rows := make([]*Row, 0, len(records))
	for _, r := range records {
		row := &Row{}
		if err := proto.Unmarshal(r.Value, row); err != nil {
			slog.Warn("logs consumer: unmarshal dropped one record", slog.Any("error", err))
			continue
		}
		rows = append(rows, row)
	}
	if len(rows) == 0 {
		return nil
	}
	insertCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	batch, err := c.ch.PrepareBatch(insertCtx, c.query)
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
	slog.Info("logs consumer: flushed", slog.Int("rows", len(rows)))
	return nil
}
