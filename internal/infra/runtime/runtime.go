package runtime

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/Optikk-Org/optikk-backend/internal/config"
	"github.com/Optikk-Org/optikk-backend/internal/infra/dashboardcfg"
	dashboarddefaults "github.com/Optikk-Org/optikk-backend/internal/infra/dashboardcfg/defaults"
	"github.com/Optikk-Org/optikk-backend/internal/infra/ingestion"
	"github.com/Optikk-Org/optikk-backend/internal/modules/livetail"
	"github.com/Optikk-Org/optikk-backend/internal/infra/redis"
	"github.com/Optikk-Org/optikk-backend/internal/infra/session"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/auth"
	otlplogs "github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/logs"
	otlpmetrics "github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/metrics"
	otlpspans "github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/spans"
	redigoredis "github.com/gomodule/redigo/redis"
	goredis "github.com/redis/go-redis/v9"
)

type OTLPDependencies struct {
	Authenticator    ingestion.TeamResolver
	Tracker          ingestion.SizeTracker
	LogDispatcher    ingestion.Dispatcher[*otlplogs.LogRow]
	SpanDispatcher   ingestion.Dispatcher[*otlpspans.SpanRow]
	MetricDispatcher ingestion.Dispatcher[*otlpmetrics.MetricRow]
}

type Runtime struct {
	SessionManager  session.Manager
	LiveTailHub     livetail.Hub
	DashboardConfig *dashboardcfg.Registry
	RedisClient     *goredis.Client
	RedisPool       *redigoredis.Pool
	OTLP            OTLPDependencies
}

func New(sqlDB *sql.DB, cfg config.Config) (*Runtime, error) {
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

	liveTailHub, err := newLiveTailHub(cfg)
	if err != nil {
		redisClients.Pool.Close()
		_ = redisClients.Client.Close() //nolint:errcheck // best-effort cleanup on init failure
		return nil, err
	}

	registry, err := dashboarddefaults.Load()
	if err != nil {
		redisClients.Pool.Close()
		_ = redisClients.Client.Close() //nolint:errcheck // best-effort cleanup on init failure
		return nil, fmt.Errorf("failed to load embedded default config registry: %w", err)
	}

	logDispatcher, err := newDispatcher[*otlplogs.LogRow]("logs", cfg)
	if err != nil {
		redisClients.Pool.Close()
		_ = redisClients.Client.Close() //nolint:errcheck // best-effort cleanup on init failure
		return nil, err
	}
	spanDispatcher, err := newDispatcher[*otlpspans.SpanRow]("spans", cfg)
	if err != nil {
		redisClients.Pool.Close()
		_ = redisClients.Client.Close() //nolint:errcheck // best-effort cleanup on init failure
		return nil, err
	}
	metricDispatcher, err := newDispatcher[*otlpmetrics.MetricRow]("metrics", cfg)
	if err != nil {
		redisClients.Pool.Close()
		_ = redisClients.Client.Close() //nolint:errcheck // best-effort cleanup on init failure
		return nil, err
	}

	return &Runtime{
		SessionManager:  sessionManager,
		LiveTailHub:     liveTailHub,
		DashboardConfig: registry,
		RedisClient:     redisClients.Client,
		RedisPool:       redisClients.Pool,
		OTLP: OTLPDependencies{
			Authenticator:    auth.NewAuthenticator(sqlDB),
			Tracker:          otlp.NewByteTracker(sqlDB, cfg.ByteTrackerFlushInterval()),
			LogDispatcher:    logDispatcher,
			SpanDispatcher:   spanDispatcher,
			MetricDispatcher: metricDispatcher,
		},
	}, nil
}

func (r *Runtime) Close() error {
	if r == nil {
		return nil
	}
	if stopper, ok := r.OTLP.Tracker.(interface{ Stop() }); ok {
		stopper.Stop()
	}
	if r.OTLP.LogDispatcher != nil {
		r.OTLP.LogDispatcher.Close()
	}
	if r.OTLP.SpanDispatcher != nil {
		r.OTLP.SpanDispatcher.Close()
	}
	if r.OTLP.MetricDispatcher != nil {
		r.OTLP.MetricDispatcher.Close()
	}
	if r.RedisPool != nil {
		_ = r.RedisPool.Close() //nolint:errcheck // best-effort cleanup
	}
	if r.RedisClient != nil {
		_ = r.RedisClient.Close() //nolint:errcheck // best-effort cleanup
	}
	return nil
}

func newSessionManager(cfg config.Config, pool *redigoredis.Pool) (session.Manager, error) {
	switch normalizeProvider(cfg.SessionProvider()) {
	case "redis":
		if pool == nil {
			return nil, fmt.Errorf("redis session provider requires redis pool")
		}
		return session.NewManager(cfg, pool), nil
	case "local":
		return session.NewManager(cfg, nil), nil
	default:
		return nil, fmt.Errorf("unsupported session provider %q", cfg.SessionProvider())
	}
}

func newLiveTailHub(cfg config.Config) (livetail.Hub, error) {
	switch normalizeProvider(cfg.LiveTailHubProvider()) {
	case "local":
		return livetail.NewHub(), nil
	default:
		return nil, fmt.Errorf("unsupported live tail hub provider %q", cfg.LiveTailHubProvider())
	}
}

func newDispatcher[T any](name string, cfg config.Config) (ingestion.Dispatcher[T], error) {
	switch normalizeProvider(cfg.IngestionDispatcherProvider()) {
	case "local":
		return ingestion.NewLocalDispatcher[T](name, cfg.IngestionQueueSize()), nil
	default:
		return nil, fmt.Errorf("unsupported ingestion dispatcher provider %q", cfg.IngestionDispatcherProvider())
	}
}

func normalizeProvider(provider string) string {
	provider = strings.TrimSpace(strings.ToLower(provider))
	if provider == "" {
		return "local"
	}
	return provider
}
