package server

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/Optikk-Org/optikk-backend/internal/config"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/kafka"
	"github.com/Optikk-Org/optikk-backend/internal/infra/redis"
	"github.com/Optikk-Org/optikk-backend/internal/infra/session"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/kafkadispatcher"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/auth"
	otlplogs "github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/logs"
	otlpmetrics "github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/metrics"
	otlpspans "github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/spans"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/proto"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/tracker"
	"github.com/Optikk-Org/optikk-backend/internal/modules/livetail"
	redigoredis "github.com/gomodule/redigo/redis"
	goredis "github.com/redis/go-redis/v9"
	googlepb "google.golang.org/protobuf/proto"
)

// OTLPDependencies wires OTLP ingest services and workers.
type OTLPDependencies struct {
	Authenticator    ingestion.TeamResolver
	Tracker          ingestion.SizeTracker
	LogDispatcher    *kafkadispatcher.KafkaDispatcher[*proto.LogRow]
	SpanDispatcher   *kafkadispatcher.KafkaDispatcher[*proto.SpanRow]
	MetricDispatcher *kafkadispatcher.KafkaDispatcher[*proto.MetricRow]
}

// Infra holds process-wide infrastructure constructed at startup.
type Infra struct {
	DB             *sql.DB
	CH             clickhouse.Conn
	SessionManager session.Manager
	LiveTailHub    livetail.Hub
	RedisClient    *goredis.Client
	RedisPool      *redigoredis.Pool
	OTLP           OTLPDependencies
}

func newInfra(cfg config.Config) (_ *Infra, err error) {
	dbConn, err := dbutil.Open(cfg.MySQLDSN(), cfg.MySQL.MaxOpenConns, cfg.MySQL.MaxIdleConns)
	if err != nil {
		return nil, fmt.Errorf("mysql: %w", err)
	}
	defer func() {
		if err != nil {
			_ = dbConn.Close() //nolint:errcheck // cleanup after failed init
		}
	}()

	chCloud := dbutil.ClickHouseCloudConfig{
		Host:     cfg.ClickHouse.CloudHost,
		Username: cfg.ClickHouse.User,
		Password: cfg.ClickHouse.Password,
	}
	chConn, err := dbutil.OpenClickHouseConn(cfg.ClickHouseDSN(), cfg.ClickHouse.Production, chCloud)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: %w", err)
	}
	defer func() {
		if err != nil {
			_ = chConn.Close() //nolint:errcheck // cleanup after failed init
		}
	}()

	redisClients, err := redis.NewClients(cfg)
	if err != nil {
		return nil, err
	}

	sessionManager, err := newSessionManager(cfg, redisClients.Pool)
	if err != nil {
		redisClients.Pool.Close()
		_ = redisClients.Client.Close() //nolint:errcheck // best-effort cleanup on init failure
		return nil, err
	}

	liveTailHub, err := newLiveTailHub()
	if err != nil {
		redisClients.Pool.Close()
		_ = redisClients.Client.Close() //nolint:errcheck // best-effort cleanup on init failure
		return nil, err
	}

	prefix := cfg.KafkaTopicPrefix()
	if err := kafka.EnsureTopics(cfg.KafkaBrokers(), kafka.IngestTopicNames(prefix)); err != nil {
		redisClients.Pool.Close()
		_ = redisClients.Client.Close() //nolint:errcheck // best-effort cleanup on init failure
		return nil, fmt.Errorf("kafka ingest topics: %w", err)
	}

	logFlusher := otlp.NewCHFlusher[*otlplogs.LogRow](chConn, "observability.logs", otlplogs.LogColumns)
	spanFlusher := otlp.NewCHFlusher[*otlpspans.SpanRow](chConn, "observability.spans", otlpspans.SpanColumns)
	metricFlusher := otlp.NewCHFlusher[*otlpmetrics.MetricRow](chConn, "observability.metrics", otlpmetrics.MetricColumns)

	// Logs: OnPersistence converts proto.LogRow → logs.LogRow (ch: tags) before flushing.
	logHandlers := ingestion.Handlers[*proto.LogRow]{
		OnPersistence: func(ctx context.Context, rows []*proto.LogRow) error {
			chRows := make([]*otlplogs.LogRow, len(rows))
			for i, r := range rows {
				chRows[i] = otlplogs.FromProto(r)
			}
			return logFlusher.Flush(chRows)
		},
		OnStreaming: func(ctx context.Context, batch ingestion.TelemetryBatch[*proto.LogRow]) {
			for _, row := range batch.Rows {
				chRow := otlplogs.FromProto(row)
				if payload, ok := otlplogs.LiveTailStreamPayload(chRow); ok && payload != nil {
					liveTailHub.Publish(batch.TeamID, payload)
				}
			}
		},
	}

	// Spans: OnPersistence converts proto.SpanRow → spans.SpanRow (ch: tags) before flushing.
	spanHandlers := ingestion.Handlers[*proto.SpanRow]{
		OnPersistence: func(ctx context.Context, rows []*proto.SpanRow) error {
			chRows := make([]*otlpspans.SpanRow, len(rows))
			for i, r := range rows {
				chRows[i] = otlpspans.FromProto(r)
			}
			return spanFlusher.Flush(chRows)
		},
		OnStreaming: func(ctx context.Context, batch ingestion.TelemetryBatch[*proto.SpanRow]) {
			for _, row := range batch.Rows {
				chRow := otlpspans.FromProto(row)
				data, err := otlpspans.SpanLiveTailStreamPayload(chRow, time.Now().UnixMilli())
				if err == nil && data != nil {
					liveTailHub.Publish(batch.TeamID, data)
				}
			}
		},
	}

	// Metrics: OnPersistence converts proto.MetricRow → metrics.MetricRow (ch: tags) before flushing.
	metricHandlers := ingestion.Handlers[*proto.MetricRow]{
		OnPersistence: func(ctx context.Context, rows []*proto.MetricRow) error {
			chRows := make([]*otlpmetrics.MetricRow, len(rows))
			for i, r := range rows {
				chRows[i] = otlpmetrics.FromProto(r)
			}
			return metricFlusher.Flush(chRows)
		},
	}

	logDispatcher, err := newIngestDispatcher(
		"logs", cfg,
		func() *proto.LogRow { return &proto.LogRow{} },
		logHandlers,
	)
	if err != nil {
		redisClients.Pool.Close()
		_ = redisClients.Client.Close()
		return nil, err
	}
	spanDispatcher, err := newIngestDispatcher(
		"spans", cfg,
		func() *proto.SpanRow { return &proto.SpanRow{} },
		spanHandlers,
	)
	if err != nil {
		redisClients.Pool.Close()
		_ = redisClients.Client.Close()
		return nil, err
	}
	metricDispatcher, err := newIngestDispatcher(
		"metrics", cfg,
		func() *proto.MetricRow { return &proto.MetricRow{} },
		metricHandlers,
	)
	if err != nil {
		redisClients.Pool.Close()
		_ = redisClients.Client.Close()
		return nil, err
	}

	return &Infra{
		DB:             dbConn,
		CH:             chConn,
		SessionManager: sessionManager,
		LiveTailHub:    liveTailHub,
		RedisClient:    redisClients.Client,
		RedisPool:      redisClients.Pool,
		OTLP: OTLPDependencies{
			Authenticator:    auth.NewAuthenticator(dbConn, redisClients.Client),
			Tracker:          tracker.NewByteTracker(dbConn, cfg.ByteTrackerFlushInterval()),
			LogDispatcher:    logDispatcher,
			SpanDispatcher:   spanDispatcher,
			MetricDispatcher: metricDispatcher,
		},
	}, nil
}

func (i *Infra) Close() error {
	if i == nil {
		return nil
	}
	if stopper, ok := i.OTLP.Tracker.(interface{ Stop() }); ok {
		stopper.Stop()
	}
	if i.OTLP.LogDispatcher != nil {
		i.OTLP.LogDispatcher.Close()
	}
	if i.OTLP.SpanDispatcher != nil {
		i.OTLP.SpanDispatcher.Close()
	}
	if i.OTLP.MetricDispatcher != nil {
		i.OTLP.MetricDispatcher.Close()
	}
	if i.RedisPool != nil {
		_ = i.RedisPool.Close() //nolint:errcheck // best-effort cleanup
	}
	if i.RedisClient != nil {
		_ = i.RedisClient.Close() //nolint:errcheck // best-effort cleanup
	}
	if i.CH != nil {
		_ = i.CH.Close() //nolint:errcheck // best-effort cleanup
	}
	if i.DB != nil {
		_ = i.DB.Close() //nolint:errcheck // best-effort cleanup
	}
	return nil
}

func newSessionManager(cfg config.Config, pool *redigoredis.Pool) (session.Manager, error) {
	if pool == nil {
		return nil, fmt.Errorf("session manager requires redis pool")
	}
	return session.NewManager(cfg, pool)
}

func newLiveTailHub() (livetail.Hub, error) {
	return livetail.NewHub(), nil
}

func newIngestDispatcher[T googlepb.Message](
	name string, cfg config.Config,
	newMsg func() T,
	handlers ingestion.Handlers[T],
) (*kafkadispatcher.KafkaDispatcher[T], error) {
	prefix := cfg.KafkaTopicPrefix()
	topic := fmt.Sprintf("%s.%s", prefix, name)
	return kafkadispatcher.New[T](cfg.KafkaBrokers(), cfg.KafkaConsumerGroup(), topic, name, newMsg, handlers)
}
