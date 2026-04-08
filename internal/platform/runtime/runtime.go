package runtime

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/Optikk-Org/optikk-backend/internal/config"
	infraingestion "github.com/Optikk-Org/optikk-backend/internal/infra/ingestion"
	infralivetail "github.com/Optikk-Org/optikk-backend/internal/infra/livetail"
	infrasession "github.com/Optikk-Org/optikk-backend/internal/infra/session"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/auth"
	otlplogs "github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/logs"
	otlpmetrics "github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/metrics"
	otlpspans "github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/spans"
	platformdashboardcfg "github.com/Optikk-Org/optikk-backend/internal/platform/dashboardcfg"
	platformdefaults "github.com/Optikk-Org/optikk-backend/internal/platform/dashboardcfg/defaults"
	platformingestion "github.com/Optikk-Org/optikk-backend/internal/platform/ingestion"
	platformlivetail "github.com/Optikk-Org/optikk-backend/internal/platform/livetail"
	platformratelimit "github.com/Optikk-Org/optikk-backend/internal/platform/ratelimit"
	platformsession "github.com/Optikk-Org/optikk-backend/internal/platform/session"
)

type OTLPDependencies struct {
	Authenticator    platformingestion.TeamResolver
	Tracker          platformingestion.SizeTracker
	Limiter          platformingestion.Limiter
	LogDispatcher    platformingestion.Dispatcher[*otlplogs.LogRow]
	SpanDispatcher   platformingestion.Dispatcher[*otlpspans.SpanRow]
	MetricDispatcher platformingestion.Dispatcher[*otlpmetrics.MetricRow]
}

type Runtime struct {
	SessionManager  platformsession.Manager
	RateLimiter     platformratelimit.Limiter
	LiveTailHub     platformlivetail.Hub
	DashboardConfig *platformdashboardcfg.Registry
	OTLP            OTLPDependencies
}

func New(sqlDB *sql.DB, cfg config.Config) (*Runtime, error) {
	sessionManager, err := newSessionManager(cfg)
	if err != nil {
		return nil, err
	}

	rateLimiter, err := platformratelimit.New(cfg, 2000, 2000, config.DefaultRateLimitWindow)
	if err != nil {
		return nil, err
	}

	liveTailHub, err := newLiveTailHub(cfg)
	if err != nil {
		return nil, err
	}

	registry, err := platformdefaults.Load()
	if err != nil {
		return nil, fmt.Errorf("failed to load embedded default config registry: %w", err)
	}

	logDispatcher, err := newDispatcher[*otlplogs.LogRow](cfg)
	if err != nil {
		return nil, err
	}
	spanDispatcher, err := newDispatcher[*otlpspans.SpanRow](cfg)
	if err != nil {
		return nil, err
	}
	metricDispatcher, err := newDispatcher[*otlpmetrics.MetricRow](cfg)
	if err != nil {
		return nil, err
	}

	return &Runtime{
		SessionManager:  sessionManager,
		RateLimiter:     rateLimiter,
		LiveTailHub:     liveTailHub,
		DashboardConfig: registry,
		OTLP: OTLPDependencies{
			Authenticator:    auth.NewAuthenticator(sqlDB),
			Tracker:          otlp.NewByteTracker(sqlDB, cfg.ByteTrackerFlushInterval()),
			Limiter:          otlp.NewLimiter(100_000, 100_000),
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
	return nil
}

func newSessionManager(cfg config.Config) (platformsession.Manager, error) {
	switch normalizeProvider(cfg.SessionProvider()) {
	case "local":
		return infrasession.NewManager(cfg), nil
	default:
		return nil, fmt.Errorf("unsupported session provider %q", cfg.SessionProvider())
	}
}

func newLiveTailHub(cfg config.Config) (platformlivetail.Hub, error) {
	switch normalizeProvider(cfg.LiveTailHubProvider()) {
	case "local":
		return infralivetail.NewHub(), nil
	default:
		return nil, fmt.Errorf("unsupported live tail hub provider %q", cfg.LiveTailHubProvider())
	}
}

func newDispatcher[T any](cfg config.Config) (platformingestion.Dispatcher[T], error) {
	switch normalizeProvider(cfg.IngestionDispatcherProvider()) {
	case "local":
		return infraingestion.NewLocalDispatcher[T](cfg.IngestionQueueSize()), nil
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
