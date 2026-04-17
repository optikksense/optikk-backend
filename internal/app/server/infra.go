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
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/auth"
	otlplogs "github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/logs"
	otlpmetrics "github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/metrics"
	otlpspans "github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/spans"
	"github.com/Optikk-Org/optikk-backend/internal/modules/livetail"
	redigoredis "github.com/gomodule/redigo/redis"
	goredis "github.com/redis/go-redis/v9"
)

// OTLPDependencies wires OTLP ingest services and workers.
type OTLPDependencies struct {
	Authenticator    ingestion.TeamResolver
	Tracker          ingestion.SizeTracker
	LogDispatcher    ingestion.Dispatcher[*otlplogs.LogRow]
	SpanDispatcher   ingestion.Dispatcher[*otlpspans.SpanRow]
	MetricDispatcher ingestion.Dispatcher[*otlpmetrics.MetricRow]
}

// Infra holds process-wide infrastructure constructed at startup.
type Infra struct {
	DB             *sql.DB
	CH             clickhouse.Conn
	SessionManager session.Manager
	LiveTailHub    livetail.Hub
	RedisClient    *goredis.Client
	RedisPool       *redigoredis.Pool
	OTLP            OTLPDependencies
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

	liveTailHub := livetail.NewHub(redisClients.Client, 0)

	prefix := cfg.KafkaTopicPrefix()
	if err := kafka.EnsureTopics(cfg.KafkaBrokers(), kafka.IngestTopicNames(prefix)); err != nil {
		redisClients.Pool.Close()
		_ = redisClients.Client.Close() //nolint:errcheck // best-effort cleanup on init failure
		return nil, fmt.Errorf("kafka ingest topics: %w", err)
	}

	logDispatcher, err := newIngestDispatcher[*otlplogs.LogRow]("logs", cfg)
	if err != nil {
		redisClients.Pool.Close()
		_ = redisClients.Client.Close() //nolint:errcheck // best-effort cleanup on init failure
		return nil, err
	}
	spanDispatcher, err := newIngestDispatcher[*otlpspans.SpanRow]("spans", cfg)
	if err != nil {
		redisClients.Pool.Close()
		_ = redisClients.Client.Close() //nolint:errcheck // best-effort cleanup on init failure
		return nil, err
	}
	metricDispatcher, err := newIngestDispatcher[*otlpmetrics.MetricRow]("metrics", cfg)
	if err != nil {
		redisClients.Pool.Close()
		_ = redisClients.Client.Close() //nolint:errcheck // best-effort cleanup on init failure
		return nil, err
	}

	return &Infra{
		DB:             dbConn,
		CH:             chConn,
		SessionManager: sessionManager,
		LiveTailHub:    liveTailHub,
		RedisClient:    redisClients.Client,
		RedisPool:       redisClients.Pool,
		OTLP: OTLPDependencies{
			Authenticator:    auth.NewAuthenticator(dbConn, redisClients.Client),
			Tracker:          otlp.NewByteTracker(dbConn, cfg.ByteTrackerFlushInterval()),
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

func newIngestDispatcher[T any](name string, cfg config.Config) (ingestion.Dispatcher[T], error) {
	prefix := cfg.KafkaTopicPrefix()
	topic := fmt.Sprintf("%s.%s", prefix, name)
	return ingestion.NewKafkaDispatcher[T](cfg.KafkaBrokers(), cfg.KafkaConsumerGroup(), topic, name)
}
