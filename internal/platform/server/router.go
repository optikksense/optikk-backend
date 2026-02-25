package server

import (
	"log"

	"github.com/gin-gonic/gin"
	"github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/internal/modules/ai"
	"github.com/observability/observability-backend-go/internal/modules/alerts"
	"github.com/observability/observability-backend-go/internal/modules/dashboardconfig"
	"github.com/observability/observability-backend-go/internal/modules/deployments"
	"github.com/observability/observability-backend-go/internal/modules/explore"
	"github.com/observability/observability-backend-go/internal/modules/health"
	"github.com/observability/observability-backend-go/internal/modules/identity"
	"github.com/observability/observability-backend-go/internal/modules/infrastructure"
	"github.com/observability/observability-backend-go/internal/modules/insights"
	logsmodule "github.com/observability/observability-backend-go/internal/modules/logs"
	"github.com/observability/observability-backend-go/internal/modules/metrics"
	"github.com/observability/observability-backend-go/internal/modules/saturation"
	"github.com/observability/observability-backend-go/internal/modules/traces"
	"github.com/observability/observability-backend-go/internal/telemetry"
)

type moduleConfigs struct {
	Identity        identity.Config
	Alerts          alerts.Config
	Metrics         metrics.Config
	Infrastructure  infrastructure.Config
	Saturation      saturation.Config
	Logs            logsmodule.Config
	Traces          traces.Config
	Health          health.Config
	Deployments     deployments.Config
	Insights        insights.Config
	AI              ai.Config
	DashboardConfig dashboardconfig.Config
	Explore         explore.Config
	Telemetry       telemetry.Config
}

func defaultModuleConfigs() moduleConfigs {
	return moduleConfigs{
		Identity:        identity.DefaultConfig(),
		Alerts:          alerts.DefaultConfig(),
		Metrics:         metrics.DefaultConfig(),
		Infrastructure:  infrastructure.DefaultConfig(),
		Saturation:      saturation.DefaultConfig(),
		Logs:            logsmodule.DefaultConfig(),
		Traces:          traces.DefaultConfig(),
		Health:          health.DefaultConfig(),
		Deployments:     deployments.DefaultConfig(),
		Insights:        insights.DefaultConfig(),
		AI:              ai.DefaultConfig(),
		DashboardConfig: dashboardconfig.DefaultConfig(),
		Explore:         explore.DefaultConfig(),
		Telemetry:       telemetry.DefaultConfig(),
	}
}

func (a *App) registerRoutes(r *gin.Engine) {
	cfg := defaultModuleConfigs()

	api := r.Group("/api")
	v1 := r.Group("/api/v1")

	identity.RegisterRoutes(cfg.Identity, api, a.Auth, a.Users)
	alerts.RegisterRoutes(cfg.Alerts, api, v1, a.Alerts)
	metrics.RegisterRoutes(cfg.Metrics, api, v1, a.Metrics)
	infrastructure.RegisterRoutes(cfg.Infrastructure, api, v1, a.Infrastructure)
	saturation.RegisterRoutes(cfg.Saturation, api, v1, a.Saturation)
	logsmodule.RegisterRoutes(cfg.Logs, api, v1, a.Logs)
	traces.RegisterRoutes(cfg.Traces, api, v1, a.Traces)
	health.RegisterRoutes(cfg.Health, api, v1, a.Health)
	deployments.RegisterRoutes(cfg.Deployments, api, v1, a.Deployments)
	insights.RegisterRoutes(cfg.Insights, api, v1, a.Insights)
	ai.RegisterRoutes(cfg.AI, api, v1, a.AI)
	dashboardconfig.RegisterRoutes(cfg.DashboardConfig, api, v1, a.DashboardConfig)
	explore.RegisterRoutes(cfg.Explore, api, v1, a.Explore)

	// OTLP ingestion endpoint — authenticated via api_key (not JWT).
	repo := telemetry.NewRepository(database.NewMySQLWrapper(a.CH))

	var ingester telemetry.Ingester
	if a.Config.KafkaEnabled {
		kafkaIngester, err := telemetry.NewKafkaIngester(a.Config.KafkaBrokerList())
		if err != nil {
			log.Fatalf("failed to create kafka producer: %v", err)
		}
		ingester = kafkaIngester

		consumerCfg := telemetry.KafkaConsumerConfig{
			Brokers:       a.Config.KafkaBrokerList(),
			BatchSize:     a.Config.QueueBatchSize,
			FlushInterval: a.Config.QueueFlushInterval(),
		}
		consumer, err := telemetry.NewKafkaConsumer(repo, consumerCfg)
		if err != nil {
			log.Fatalf("failed to create kafka consumer: %v", err)
		}
		a.TelemetryConsumer = consumer
		log.Println("telemetry: Kafka mode enabled")
	} else {
		ingester = telemetry.NewDirectIngester(repo)
		log.Println("telemetry: direct mode (sync ClickHouse writes)")
	}

	a.TelemetryIngester = ingester
	otlpHandler := telemetry.NewHandler(ingester, a.DB)
	otlp := r.Group("/otlp")
	telemetry.RegisterRoutes(cfg.Telemetry, otlp, otlpHandler)
}
