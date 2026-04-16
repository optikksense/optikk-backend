package server

import (
	"database/sql"
	"fmt"

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
)

// OTLPDependencies wires OTLP ingest services and workers.
type OTLPDependencies struct {
	Authenticator    ingestion.TeamResolver
	Tracker          ingestion.SizeTracker
	LogDispatcher    *kafkadispatcher.Dispatcher[*proto.LogRow]
	SpanDispatcher   *kafkadispatcher.Dispatcher[*proto.SpanRow]
	MetricDispatcher *kafkadispatcher.Dispatcher[*proto.MetricRow]

	LogPersist    *otlplogs.PersistenceConsumer
	LogStream     *otlplogs.StreamingConsumer
	SpanPersist   *otlpspans.PersistenceConsumer
	SpanStream    *otlpspans.StreamingConsumer
	MetricPersist *otlpmetrics.PersistenceConsumer
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
			_ = dbConn.Close() //nolint:errcheck
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
			_ = chConn.Close() //nolint:errcheck
		}
	}()

	redisClients, err := redis.NewClients(cfg)
	if err != nil {
		return nil, err
	}

	sessionManager, err := newSessionManager(cfg, redisClients.Pool)
	if err != nil {
		redisClients.Pool.Close()
		_ = redisClients.Client.Close()
		return nil, err
	}

	liveTailHub, err := newLiveTailHub()
	if err != nil {
		redisClients.Pool.Close()
		_ = redisClients.Client.Close()
		return nil, err
	}

	prefix := cfg.KafkaTopicPrefix()
	if err := kafka.EnsureTopics(cfg.KafkaBrokers(), kafka.IngestTopicNames(prefix)); err != nil {
		redisClients.Pool.Close()
		_ = redisClients.Client.Close()
		return nil, fmt.Errorf("kafka ingest topics: %w", err)
	}

	// Logs
	logP, logS, logR, err := kafka.InitIngestClients(cfg.KafkaBrokers(), cfg.KafkaConsumerGroup(), prefix+".logs", "logs")
	if err != nil {
		return nil, err
	}
	logFlusher := otlp.NewCHFlusher[*otlplogs.LogRow](chConn, "observability.logs", otlplogs.LogColumns)
	logConsumerP := otlplogs.NewPersistenceConsumer(logR, logFlusher)
	logConsumerS := otlplogs.NewStreamingConsumer(logS, liveTailHub)

	// Spans
	spanP, spanS, spanR, err := kafka.InitIngestClients(cfg.KafkaBrokers(), cfg.KafkaConsumerGroup(), prefix+".spans", "spans")
	if err != nil {
		return nil, err
	}
	spanFlusher := otlp.NewCHFlusher[*otlpspans.SpanRow](chConn, "observability.spans", otlpspans.SpanColumns)
	spanConsumerP := otlpspans.NewPersistenceConsumer(spanR, spanFlusher)
	spanConsumerS := otlpspans.NewStreamingConsumer(spanS, liveTailHub)

	// Metrics
	metricP, _, metricR, err := kafka.InitIngestClients(cfg.KafkaBrokers(), cfg.KafkaConsumerGroup(), prefix+".metrics", "metrics")
	if err != nil {
		return nil, err
	}
	metricFlusher := otlp.NewCHFlusher[*otlpmetrics.MetricRow](chConn, "observability.metrics", otlpmetrics.MetricColumns)
	metricConsumerP := otlpmetrics.NewPersistenceConsumer(metricR, metricFlusher)

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
			LogDispatcher:    kafkadispatcher.New[*proto.LogRow](logP, prefix+".logs", "logs"),
			SpanDispatcher:   kafkadispatcher.New[*proto.SpanRow](spanP, prefix+".spans", "spans"),
			MetricDispatcher: kafkadispatcher.New[*proto.MetricRow](metricP, prefix+".metrics", "metrics"),
			LogPersist:       logConsumerP,
			LogStream:        logConsumerS,
			SpanPersist:      spanConsumerP,
			SpanStream:       spanConsumerS,
			MetricPersist:    metricConsumerP,
		},
	}, nil
}

func (i *Infra) Close() error {
	if i == nil {
		return nil
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
		_ = i.RedisPool.Close()
	}
	if i.RedisClient != nil {
		_ = i.RedisClient.Close()
	}
	if i.CH != nil {
		_ = i.CH.Close()
	}
	if i.DB != nil {
		_ = i.DB.Close()
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
