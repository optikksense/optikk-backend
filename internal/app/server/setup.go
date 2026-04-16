package server

import (
	"fmt"

	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/Optikk-Org/optikk-backend/internal/config"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/kafka"
	"github.com/Optikk-Org/optikk-backend/internal/infra/middleware"
	"github.com/Optikk-Org/optikk-backend/internal/infra/redis"
	"github.com/Optikk-Org/optikk-backend/internal/infra/session"
	"github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/auth"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/tracker"
	"github.com/Optikk-Org/optikk-backend/internal/modules/livetail"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	redigoredis "github.com/gomodule/redigo/redis"
)

// newDeps constructs all shared dependencies and registers their closers.
func newDeps(cfg config.Config) (_ *registry.Deps, err error) {
	utils.Init(cfg.SpansBucketSeconds(), cfg.LogsBucketSeconds())

	dbConn, err := dbutil.Open(cfg.MySQLDSN(), cfg.MySQL.MaxOpenConns, cfg.MySQL.MaxIdleConns)
	if err != nil {
		return nil, fmt.Errorf("mysql: %w", err)
	}
	defer func() {
		if err != nil {
			_ = dbConn.Close()
		}
	}()

	chConn, err := dbutil.OpenClickHouseConn(cfg.ClickHouseDSN())
	if err != nil {
		return nil, fmt.Errorf("clickhouse: %w", err)
	}
	defer func() {
		if err != nil {
			_ = chConn.Close()
		}
	}()

	redisClients, err := redis.NewClients(cfg)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = redisClients.Pool.Close()
			_ = redisClients.Client.Close()
		}
	}()

	sessionManager, err := newSessionManager(cfg, redisClients.Pool)
	if err != nil {
		return nil, err
	}

	if err := kafka.EnsureTopics(cfg.KafkaBrokers(), kafka.IngestTopicNames(cfg.KafkaTopicPrefix())); err != nil {
		return nil, fmt.Errorf("kafka ingest topics: %w", err)
	}

	sizeTracker := tracker.NewByteTracker(dbConn, cfg.ByteTrackerFlushInterval())

	deps := &registry.Deps{
		NativeQuerier:  dbutil.NewNativeQuerier(chConn),
		GetTenant:      modulecommon.GetTenantFunc(middleware.GetTenant),
		DB:             dbConn,
		CH:             chConn,
		SessionManager: sessionManager,
		AppConfig:      cfg,
		RedisClient:    redisClients.Client,
		LiveTailHub:    livetail.NewHub(),
		TeamResolver:   auth.NewAuthenticator(dbConn, redisClients.Client),
		SizeTracker:    sizeTracker,
		KafkaBrokers:   cfg.KafkaBrokers(),
		ConsumerGroup:  cfg.KafkaConsumerGroup(),
		TopicPrefix:    cfg.KafkaTopicPrefix(),
	}

	deps.OnClose(func() error { sizeTracker.Stop(); return nil })
	deps.OnClose(func() error { return redisClients.Pool.Close() })
	deps.OnClose(redisClients.Client.Close)
	deps.OnClose(chConn.Close)
	deps.OnClose(dbConn.Close)

	return deps, nil
}

func newSessionManager(cfg config.Config, pool *redigoredis.Pool) (session.Manager, error) {
	if pool == nil {
		return nil, fmt.Errorf("session manager requires redis pool")
	}
	return session.NewManager(cfg, pool)
}
