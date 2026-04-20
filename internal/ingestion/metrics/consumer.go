package metrics

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

// flushInterval / flushGrace match the spans consumer so Redis keys from
// different signals land on the same minute boundary.
const (
	flushInterval = 15 * time.Second
	flushGrace    = 15 * time.Second
)

// Consumer is the persistence side of the metric pipeline. Mirrors the log
// consumer: poll → unmarshal → CH batch insert → commit. See logs/consumer.go
// for the delivery-semantics rationale. Every flushed row is also folded into
// the sketch aggregator so repositories can serve percentiles from Redis.
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
			slog.Warn("metrics consumer: poll error", slog.Any("error", err))
			continue
		}
		if len(records) == 0 {
			continue
		}
		if err := c.flush(ctx, records); err != nil {
			slog.Error("metrics consumer: flush failed; offsets NOT committed",
				slog.Int("records", len(records)), slog.Any("error", err))
			continue
		}
		if err := c.kafka.Commit(ctx, records); err != nil {
			slog.Warn("metrics consumer: commit failed", slog.Any("error", err))
		}
	}
}

func (c *Consumer) flush(ctx context.Context, records []kafkainfra.Record) error {
	rows := make([]*Row, 0, len(records))
	for _, r := range records {
		row := &Row{}
		if err := proto.Unmarshal(r.Value, row); err != nil {
			slog.Warn("metrics consumer: unmarshal dropped one record", slog.Any("error", err))
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
	slog.Info("metrics consumer: flushed", slog.Int("rows", len(rows)))
	return nil
}

// observe routes a metric row to the correct sketch kind based on metric_name.
// Non-matching metrics (CPU / memory / network / etc.) are ignored — sketches
// are only maintained for distributions that repositories actively query.
func (c *Consumer) observe(row *Row) {
	if c.agg == nil {
		return
	}
	teamID := fmt.Sprintf("%d", row.GetTeamId())
	tsUnix := row.GetTimestampNs() / int64(time.Second)
	name := row.GetMetricName()

	switch name {
	case MetricNameDbOpDuration:
		c.observeLatencySample(sketch.DbOpLatency, teamID, DbOpDim(row), row, tsUnix)
		if fp := row.GetAttributes()["db.query.text.fingerprint"]; fp != "" {
			c.observeLatencySample(sketch.DbQueryLatency, teamID, DbQueryDim(row), row, tsUnix)
		}
	case MetricNameKafkaPublish, MetricNameKafkaConsume, MetricNameKafkaProduce:
		c.observeLatencySample(sketch.KafkaTopicLatency, teamID, KafkaTopicDim(row), row, tsUnix)
	case MetricNameHttpServer:
		c.observeLatencySample(sketch.HttpServerDuration, teamID, HttpServerDim(row), row, tsUnix)
	case MetricNameHttpClient:
		c.observeLatencySample(sketch.HttpClientDuration, teamID, HttpClientDim(row), row, tsUnix)
	default:
		if IsJvmMetric(name) {
			c.observeLatencySample(sketch.JvmMetricLatency, teamID, JvmMetricDim(row), row, tsUnix)
		}
	}
}

// observeLatencySample feeds either a histogram sample (bucket midpoints
// weighted by bucket counts) or a single scalar to the aggregator, matching
// the OTLP metric type. Histogram sampling uses upper-bucket bounds as the
// representative value — same convention quantileExactWeighted used on the
// histogram columns.
func (c *Consumer) observeLatencySample(kind sketch.Kind, teamID, dim string, row *Row, tsUnix int64) {
	if dim == "|||" || dim == "|" {
		return
	}
	buckets := row.GetHistBuckets()
	counts := row.GetHistCounts()
	if len(buckets) > 0 && len(counts) > 0 {
		n := len(counts)
		if len(buckets) < n {
			n = len(buckets)
		}
		for i := 0; i < n; i++ {
			if counts[i] == 0 {
				continue
			}
			c.agg.ObserveLatency(kind, teamID, dim, buckets[i], uint32(counts[i]), tsUnix)
		}
		return
	}
	// Fallback: non-histogram metric types — treat value as a single sample.
	c.agg.ObserveLatency(kind, teamID, dim, row.GetValue(), 1, tsUnix)
}

// flushLoop mirrors spans/consumer.go's ticker: drain closed buckets every
// flushInterval, final flush on ctx cancel.
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
				slog.Warn("metrics consumer: sketch flush failed", slog.Any("error", err))
			}
		}
	}
}
