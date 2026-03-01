package server

import (
	"log"

	"github.com/gin-gonic/gin"
	util "github.com/observability/observability-backend-go/internal/helpers"
	"github.com/observability/observability-backend-go/internal/platform/middleware"
	"github.com/observability/observability-backend-go/internal/platform/sse"
	"github.com/observability/observability-backend-go/internal/modules/ai"
	"github.com/observability/observability-backend-go/internal/modules/alerts"
	"github.com/observability/observability-backend-go/internal/modules/dashboardconfig"
	deployments "github.com/observability/observability-backend-go/internal/modules/infrastructure/deployments"
	nodes "github.com/observability/observability-backend-go/internal/modules/infrastructure/nodes"
	resourceutilisation "github.com/observability/observability-backend-go/internal/modules/infrastructure/resource_utilisation"
	overviewerrors "github.com/observability/observability-backend-go/internal/modules/overview/errors"
	overviewmodule "github.com/observability/observability-backend-go/internal/modules/overview/overview"
	overviewslo "github.com/observability/observability-backend-go/internal/modules/overview/slo"
	"github.com/observability/observability-backend-go/internal/modules/saturation"
	servicepage "github.com/observability/observability-backend-go/internal/modules/services/service"
	servicetopology "github.com/observability/observability-backend-go/internal/modules/services/topology"
	telemetry "github.com/observability/observability-backend-go/modules/ingestion"
	logsmodule "github.com/observability/observability-backend-go/modules/log"
	traces "github.com/observability/observability-backend-go/modules/spans"
	identity "github.com/observability/observability-backend-go/modules/user"
)

type moduleConfigs struct {
	Identity            identity.Config
	Alerts              alerts.Config
	Overview            overviewmodule.Config
	OverviewSLO         overviewslo.Config
	OverviewErrors      overviewerrors.Config
	ServicesPage        servicepage.Config
	ServicesTopology    servicetopology.Config
	Nodes               nodes.Config
	ResourceUtilisation resourceutilisation.Config
	Saturation          saturation.Config
	Logs                logsmodule.Config
	Traces              traces.Config
	Deployments         deployments.Config
	AI                  ai.Config
	DashboardConfig     dashboardconfig.Config
	Telemetry           telemetry.Config
}

func defaultModuleConfigs() moduleConfigs {
	return moduleConfigs{
		Identity:            identity.DefaultConfig(),
		Alerts:              alerts.DefaultConfig(),
		Overview:            overviewmodule.DefaultConfig(),
		OverviewSLO:         overviewslo.DefaultConfig(),
		OverviewErrors:      overviewerrors.DefaultConfig(),
		ServicesPage:        servicepage.DefaultConfig(),
		ServicesTopology:    servicetopology.DefaultConfig(),
		Nodes:               nodes.DefaultConfig(),
		ResourceUtilisation: resourceutilisation.DefaultConfig(),
		Saturation:          saturation.DefaultConfig(),
		Logs:                logsmodule.DefaultConfig(),
		Traces:              traces.DefaultConfig(),
		Deployments:         deployments.DefaultConfig(),
		AI:                  ai.DefaultConfig(),
		DashboardConfig:     dashboardconfig.DefaultConfig(),
		Telemetry:           telemetry.DefaultConfig(),
	}
}

func (a *App) registerRoutes(r *gin.Engine) {
	cfg := defaultModuleConfigs()

	api := r.Group("/api")
	v1 := r.Group("/api/v1")

	identity.RegisterRoutes(cfg.Identity, api, v1, a.Auth, a.Users)
	alerts.RegisterRoutes(cfg.Alerts, api, v1, a.Alerts)
	overviewmodule.RegisterRoutes(cfg.Overview, api, v1, a.Overview)
	overviewslo.RegisterRoutes(cfg.OverviewSLO, api, v1, a.OverviewSLO)
	overviewerrors.RegisterRoutes(cfg.OverviewErrors, api, v1, a.OverviewErrors)
	servicepage.RegisterRoutes(cfg.ServicesPage, api, v1, a.ServicesPage)
	servicetopology.RegisterRoutes(cfg.ServicesTopology, api, v1, a.ServicesTopology)
	nodes.RegisterRoutes(cfg.Nodes, api, v1, a.Nodes)
	resourceutilisation.RegisterRoutes(cfg.ResourceUtilisation, api, v1, a.ResourceUtilisation)
	saturation.RegisterRoutes(cfg.Saturation, api, v1, a.Saturation)
	logsmodule.RegisterRoutes(cfg.Logs, api, v1, a.Logs)
	traces.RegisterRoutes(cfg.Traces, api, v1, a.Traces)
	deployments.RegisterRoutes(cfg.Deployments, api, v1, a.Deployments)
	ai.RegisterRoutes(cfg.AI, api, v1, a.AI)
	dashboardconfig.RegisterRoutes(cfg.DashboardConfig, api, v1, a.DashboardConfig)

	// SSE real-time event stream — authenticated via JWT (same as other API routes).
	// Pass JWTManager so the handler can validate tokens from query params
	// (EventSource does not support custom HTTP headers).
	sseHandler := sse.NewHandler(a.SSEBroker, middleware.GetTenant, a.JWTManager)
	api.GET("/events/stream", sseHandler.Stream)
	v1.GET("/events/stream", sseHandler.Stream)

	// OTLP ingestion endpoint — authenticated via api_key (not JWT).
	// NewRepository takes *sql.DB directly to use clickhouse-go/v2 batch mode.
	repo := telemetry.NewRepository(a.CH)

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
		ingester = telemetry.NewDirectIngester(repo, telemetry.DirectIngesterConfig{
			SpansBatchSize:   a.Config.QueueBatchSize,
			MetricsBatchSize: a.Config.QueueBatchSize,
			LogsBatchSize:    a.Config.QueueBatchSize,
			FlushInterval:    a.Config.QueueFlushInterval(),
		})
		log.Println("telemetry: direct mode (async buffered ClickHouse writes)")
	}

	a.TelemetryIngester = ingester
	otlpHandler := telemetry.NewHandler(ingester, a.DB)

	// Wire SSE notifications: after successful ingest, publish a lightweight
	// event to the broker so SSE-connected dashboards can refresh.
	broker := a.SSEBroker
	otlpHandler.SetOnIngest(func(teamUUID string, signal string, count int) {
		teamID := util.FromTeamUUID(teamUUID)
		if teamID > 0 {
			broker.Publish(teamID, "data-update", map[string]any{
				"signal": signal,
				"count":  count,
			})
		}
	})

	otlp := r.Group("/otlp")
	telemetry.RegisterRoutes(cfg.Telemetry, otlp, otlpHandler)
}
