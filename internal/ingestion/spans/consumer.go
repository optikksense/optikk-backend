package spans

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
	"github.com/Optikk-Org/optikk-backend/internal/infra/sketch"
	"google.golang.org/protobuf/proto"
)

// flushInterval controls how often closed buckets are pushed to Redis.
// flushGrace keeps the current minute in memory until it's safely closed.
const (
	flushInterval = 15 * time.Second
	flushGrace    = 15 * time.Second
)

// Consumer is the persistence side of the span pipeline. Mirrors the log and
// metric consumers: poll → unmarshal → CH batch insert → commit. Records are
// also fed into a sketch.Aggregator so percentile / cardinality queries can
// be served from Redis instead of ClickHouse.
type Consumer struct {
	kafka *kafkainfra.Consumer
	ch    clickhouse.Conn
	query string

	agg   *sketch.Aggregator
	store sketch.Store

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewConsumer(kafka *kafkainfra.Consumer, ch clickhouse.Conn, store sketch.Store) *Consumer {
	return &Consumer{
		kafka: kafka,
		ch:    ch,
		query: "INSERT INTO " + CHTable + " (" + strings.Join(Columns, ", ") + ")",
		agg:   sketch.NewAggregator(0),
		store: store,
	}
}

func (c *Consumer) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.run(ctx)
	}()
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.flushLoop(ctx)
	}()
}

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
			slog.Warn("spans consumer: poll error", slog.Any("error", err))
			continue
		}
		if len(records) == 0 {
			continue
		}
		if err := c.flush(ctx, records); err != nil {
			slog.Error("spans consumer: flush failed; offsets NOT committed",
				slog.Int("records", len(records)), slog.Any("error", err))
			continue
		}
		if err := c.kafka.Commit(ctx, records); err != nil {
			slog.Warn("spans consumer: commit failed", slog.Any("error", err))
		}
	}
}

func (c *Consumer) flush(ctx context.Context, records []kafkainfra.Record) error {
	rows := make([]*Row, 0, len(records))
	for _, r := range records {
		row := &Row{}
		if err := proto.Unmarshal(r.Value, row); err != nil {
			slog.Warn("spans consumer: unmarshal dropped one record", slog.Any("error", err))
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
		c.observe(row)
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send: %w", err)
	}
	slog.Info("spans consumer: flushed", slog.Int("rows", len(rows)))
	return nil
}

// observe feeds a row into the sketch aggregator. Called inside flush so every
// persisted record is also counted in latency + pod-count sketches.
func (c *Consumer) observe(row *Row) {
	if c.agg == nil {
		return
	}
	teamID := fmt.Sprintf("%d", row.GetTeamId())
	durMs := float64(row.GetDurationNano()) / 1e6
	tsUnix := row.GetTimestampNs() / int64(time.Second)
	svc := ServiceName(row)
	if svc != "" {
		c.agg.ObserveLatency(sketch.SpanLatencyService, teamID, svc, durMs, 1, tsUnix)
	}
	if dim := EndpointDim(row); dim != "|||" && dim != "" {
		c.agg.ObserveLatency(sketch.SpanLatencyEndpoint, teamID, dim, durMs, 1, tsUnix)
	}
	if pod := K8sPodName(row); pod != "" {
		c.agg.ObserveIdentity(sketch.NodePodCount, teamID, HostDim(row), pod, tsUnix)
	}
}

// flushLoop drains closed sketches to Redis every flushInterval until ctx is
// cancelled, then performs one final flush with grace=0 so in-memory state is
// durable across graceful shutdown.
func (c *Consumer) flushLoop(ctx context.Context) {
	if c.agg == nil || c.store == nil {
		return
	}
	t := time.NewTicker(flushInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = c.agg.FlushClosed(shutdownCtx, time.Now(), c.store, 0)
			cancel()
			return
		case now := <-t.C:
			if err := c.agg.FlushClosed(ctx, now, c.store, flushGrace); err != nil {
				slog.Warn("spans consumer: sketch flush failed", slog.Any("error", err))
			}
		}
	}
}
