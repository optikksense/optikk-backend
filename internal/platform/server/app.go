package server

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/observability/observability-backend-go/internal/config"
	dbutil "github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/internal/modules/ai"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
	"github.com/observability/observability-backend-go/internal/modules/dashboardconfig"
	nodes "github.com/observability/observability-backend-go/internal/modules/infrastructure/nodes"
	"github.com/observability/observability-backend-go/internal/modules/infrastructure/resource_utilisation"
	logsapi "github.com/observability/observability-backend-go/internal/modules/log"
	overviewerrors "github.com/observability/observability-backend-go/internal/modules/overview/errors"
	overviewmodule "github.com/observability/observability-backend-go/internal/modules/overview/overview"
	overviewslo "github.com/observability/observability-backend-go/internal/modules/overview/slo"
	"github.com/observability/observability-backend-go/internal/modules/saturation/database"
	satdatabase "github.com/observability/observability-backend-go/internal/modules/saturation/database"
	"github.com/observability/observability-backend-go/internal/modules/saturation/kafka"
	servicepage "github.com/observability/observability-backend-go/internal/modules/services/service"
	servicetopology "github.com/observability/observability-backend-go/internal/modules/services/topology"
	tracesapi "github.com/observability/observability-backend-go/internal/modules/spans"
	usermodule "github.com/observability/observability-backend-go/internal/modules/user"
	"github.com/observability/observability-backend-go/internal/platform/auth"
	"github.com/observability/observability-backend-go/internal/platform/ingest"
	"github.com/observability/observability-backend-go/internal/platform/middleware"
	"github.com/observability/observability-backend-go/internal/platform/otlp"
	otlpauth "github.com/observability/observability-backend-go/internal/platform/otlp/auth"
	otlpgrpc "github.com/observability/observability-backend-go/internal/platform/otlp/grpc"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

type moduleConfigs struct {
	Identity            usermodule.Config
	Overview            overviewmodule.Config
	OverviewSLO         overviewslo.Config
	OverviewErrors      overviewerrors.Config
	ServicesPage        servicepage.Config
	ServicesTopology    servicetopology.Config
	Nodes               nodes.Config
	ResourceUtilisation resource_utilisation.Config
	SaturationDatabase  database.Config
	SaturationKafka     kafka.Config
	Logs                logsapi.Config
	Traces              tracesapi.Config
	AI                  ai.Config
	DashboardConfig     dashboardconfig.Config
}

func defaultModuleConfigs() moduleConfigs {
	return moduleConfigs{
		Identity:            usermodule.DefaultConfig(),
		Overview:            overviewmodule.DefaultConfig(),
		OverviewSLO:         overviewslo.DefaultConfig(),
		OverviewErrors:      overviewerrors.DefaultConfig(),
		ServicesPage:        servicepage.DefaultConfig(),
		ServicesTopology:    servicetopology.DefaultConfig(),
		Nodes:               nodes.DefaultConfig(),
		ResourceUtilisation: resource_utilisation.DefaultConfig(),
		SaturationDatabase:  database.DefaultConfig(),
		SaturationKafka:     kafka.DefaultConfig(),
		Logs:                logsapi.DefaultConfig(),
		Traces:              tracesapi.DefaultConfig(),
		AI:                  ai.DefaultConfig(),
		DashboardConfig:     dashboardconfig.DefaultConfig(),
	}
}

type App struct {
	DB             *sql.DB
	CH             *sql.DB
	Config         config.Config
	JWTManager     auth.JWTManager
	TokenBlacklist *auth.TokenBlacklist

	Auth                *usermodule.AuthHandler
	Users               *usermodule.UserHandler
	Logs                *logsapi.LogHandler
	Traces              *tracesapi.TraceHandler
	Overview            *overviewmodule.OverviewHandler
	OverviewSLO         *overviewslo.SLOHandler
	OverviewErrors      *overviewerrors.ErrorHandler
	ServicesPage        *servicepage.ServiceHandler
	ServicesTopology    *servicetopology.TopologyHandler
	Nodes               *nodes.NodeHandler
	ResourceUtilisation *resource_utilisation.ResourceUtilisationHandler
	SaturationDatabase  *satdatabase.DatabaseHandler
	SaturationKafka     *kafka.KafkaHandler
	AI                  *ai.AIHandler
	DashboardConfig     *dashboardconfig.DashboardConfigHandler

	// OTLP ingest handlers & queues.
	OTLPHTTP     *otlp.Handler
	OTLPGRPC     *otlpgrpc.Handler
	SpansQueue   *ingest.Queue
	LogsQueue    *ingest.Queue
	MetricsQueue *ingest.Queue
}

func New(db *sql.DB, ch *sql.DB, cfg config.Config) *App {
	jwt := auth.JWTManager{
		Secret:     []byte(cfg.JWTSecret),
		Expiration: cfg.JWTDuration(),
	}

	getTenant := modulecommon.GetTenantFunc(middleware.GetTenant)

	userStore := usermodule.NewStore(db)

	// Ensure dashboard-config storage exists so page config APIs can always
	// serve defaults/fallbacks even on a freshly reset database.
	if err := dashboardconfig.NewRepository(db).EnsureTable(); err != nil {
		log.Printf("WARN: failed to ensure dashboard_chart_configs table: %v", err)
	}

	var blacklist *auth.TokenBlacklist

	if cfg.RedisEnabled {
		blacklist = auth.NewTokenBlacklist(cfg.RedisHost, cfg.RedisPort)
	} else {
		blacklist = auth.NewInMemoryTokenBlacklist()
	}

	// OTLP ingest queues — batch-write to ClickHouse.
	spansQueue := ingest.NewQueue(ch, "observability.spans", otlp.SpanColumns)
	logsQueue := ingest.NewQueue(ch, "observability.logs", otlp.LogColumns)
	metricsQueue := ingest.NewQueue(ch, "observability.metrics", otlp.MetricColumns)

	authResolver := otlpauth.NewAuthenticator(db)
	otlpHTTPHandler := otlp.NewHandler(authResolver, spansQueue, logsQueue, metricsQueue)
	otlpGRPCHandler := otlpgrpc.NewHandler(authResolver, spansQueue, logsQueue, metricsQueue)

	return &App{
		DB:             db,
		CH:             ch,
		Config:         cfg,
		JWTManager:     jwt,
		TokenBlacklist: blacklist,

		Auth:  usermodule.NewAuthHandler(getTenant, userStore, jwt, blacklist, cfg.JWTExpirationMs),
		Users: usermodule.NewUserHandler(getTenant, userStore),
		Logs: logsapi.NewHandler(
			getTenant,
			logsapi.NewRepository(dbutil.NewMySQLWrapper(ch)),
		),
		Traces: tracesapi.NewHandler(
			getTenant,
			tracesapi.NewRepository(dbutil.NewMySQLWrapper(ch)),
		),
		Overview: &overviewmodule.OverviewHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Service: overviewmodule.NewService(
				overviewmodule.NewRepository(dbutil.NewMySQLWrapper(ch)),
			),
		},
		OverviewSLO: &overviewslo.SLOHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Service: overviewslo.NewService(
				overviewslo.NewRepository(dbutil.NewMySQLWrapper(ch)),
			),
		},
		OverviewErrors: &overviewerrors.ErrorHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Service: overviewerrors.NewService(
				overviewerrors.NewRepository(dbutil.NewMySQLWrapper(ch)),
			),
		},
		ServicesPage: &servicepage.ServiceHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Service: servicepage.NewService(
				servicepage.NewRepository(dbutil.NewMySQLWrapper(ch)),
			),
		},
		ServicesTopology: &servicetopology.TopologyHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Service: servicetopology.NewService(
				servicetopology.NewRepository(dbutil.NewMySQLWrapper(ch)),
			),
		},
		Nodes: &nodes.NodeHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Service: nodes.NewService(
				nodes.NewRepository(dbutil.NewMySQLWrapper(ch)),
			),
		},
		ResourceUtilisation: &resource_utilisation.ResourceUtilisationHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Service: resource_utilisation.NewService(
				resource_utilisation.NewRepository(dbutil.NewMySQLWrapper(ch)),
			),
		},
		SaturationDatabase: &satdatabase.DatabaseHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Service: satdatabase.NewService(
				satdatabase.NewRepository(dbutil.NewMySQLWrapper(ch)),
			),
		},
		SaturationKafka: &kafka.KafkaHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Service: kafka.NewService(
				kafka.NewRepository(dbutil.NewMySQLWrapper(ch)),
			),
		},
		AI: &ai.AIHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Service: ai.NewService(
				ai.NewRepository(dbutil.NewMySQLWrapper(ch)),
			),
		},
		DashboardConfig: &dashboardconfig.DashboardConfigHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        db,
				GetTenant: getTenant,
			},
			Service: dashboardconfig.NewService(
				dashboardconfig.NewRepository(db),
			),
		},

		OTLPHTTP:     otlpHTTPHandler,
		OTLPGRPC:     otlpGRPCHandler,
		SpansQueue:   spansQueue,
		LogsQueue:    logsQueue,
		MetricsQueue: metricsQueue,
	}
}

func (a *App) Router() *gin.Engine {
	r := gin.New()
	r.Use(gin.Logger())
	r.Use(middleware.ErrorRecovery())
	r.Use(middleware.CORSMiddleware())

	// Health check endpoints (unauthenticated, no rate limits).
	r.GET("/health", a.healthLive)
	r.GET("/health/live", a.healthLive)
	r.GET("/health/ready", a.healthReady)

	// API v1 Group
	v1 := r.Group("/api/v1")

	// Apply identity and rate limiting middleware ONLY to versioned API routes.
	// This ensures unversioned legacy paths return 404 instead of 401.
	v1.Use(middleware.TenantMiddleware(a.JWTManager, a.TokenBlacklist))

	// Rate limiting: 1000 requests per second with burst of 2000.
	rl := middleware.NewRateLimiter(1000, 2000, time.Second)
	v1.Use(middleware.RateLimitMiddleware(rl))

	a.registerRoutesToGroup(v1)
	return r
}

func (a *App) registerRoutesToGroup(v1 *gin.RouterGroup) {
	cfg := defaultModuleConfigs()

	usermodule.RegisterRoutes(cfg.Identity, v1, a.Auth, a.Users)
	overviewmodule.RegisterRoutes(cfg.Overview, v1, a.Overview)
	overviewslo.RegisterRoutes(cfg.OverviewSLO, v1, a.OverviewSLO)
	overviewerrors.RegisterRoutes(cfg.OverviewErrors, v1, a.OverviewErrors)
	servicepage.RegisterRoutes(cfg.ServicesPage, v1, a.ServicesPage)
	servicetopology.RegisterRoutes(cfg.ServicesTopology, v1, a.ServicesTopology)
	nodes.RegisterRoutes(cfg.Nodes, v1, a.Nodes)
	resource_utilisation.RegisterRoutes(cfg.ResourceUtilisation, v1, a.ResourceUtilisation)
	database.RegisterRoutes(cfg.SaturationDatabase, v1, a.SaturationDatabase)
	kafka.RegisterRoutes(cfg.SaturationKafka, v1, a.SaturationKafka)
	logsapi.RegisterRoutes(cfg.Logs, v1, a.Logs)
	tracesapi.RegisterRoutes(cfg.Traces, v1, a.Traces)
	ai.RegisterRoutes(cfg.AI, v1, a.AI)
	dashboardconfig.RegisterRoutes(cfg.DashboardConfig, v1, a.DashboardConfig)
}

func (a *App) healthLive(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (a *App) healthReady(c *gin.Context) {
	if err := a.DB.Ping(); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not_ready", "mysql": err.Error()})
		return
	}
	if err := a.CH.Ping(); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not_ready", "clickhouse": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ready", "mysql": "ok", "clickhouse": "ok"})
}

func (a *App) Start(ctx context.Context) error {
	router := a.Router()

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%s", a.Config.Port),
		Handler: h2c.NewHandler(router, &http2.Server{}),
	}

	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case err := <-errCh:
		// Cleanup on early HTTP failure.
		return err
	case <-ctx.Done():
		log.Println("shutdown signal received, draining connections…")
		if a.TokenBlacklist != nil {
			a.TokenBlacklist.Stop()
		}

		// Phase 1: Flush all ingest queues before accepting no more requests.
		for _, q := range []*ingest.Queue{a.SpansQueue, a.LogsQueue, a.MetricsQueue} {
			if q != nil {
				if err := q.Close(); err != nil {
					log.Printf("WARN: error flushing ingest queue on shutdown: %v", err)
				}
			}
		}

		shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Phase 2: Stop main API server.
		if err := srv.Shutdown(shutCtx); err != nil {
			return fmt.Errorf("http shutdown: %w", err)
		}

		return <-errCh
	}
}
