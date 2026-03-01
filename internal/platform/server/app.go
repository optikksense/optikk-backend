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
	"github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/internal/modules/ai"
	aiservice "github.com/observability/observability-backend-go/internal/modules/ai/service"
	aistore "github.com/observability/observability-backend-go/internal/modules/ai/store"
	"github.com/observability/observability-backend-go/internal/modules/alerts"
	alertsservice "github.com/observability/observability-backend-go/internal/modules/alerts/service"
	alertsstore "github.com/observability/observability-backend-go/internal/modules/alerts/store"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
	"github.com/observability/observability-backend-go/internal/modules/dashboardconfig"
	dashboardconfigservice "github.com/observability/observability-backend-go/internal/modules/dashboardconfig/service"
	dashboardconfigstore "github.com/observability/observability-backend-go/internal/modules/dashboardconfig/store"
	deployments "github.com/observability/observability-backend-go/internal/modules/infrastructure/deployments"
	deploymentsservice "github.com/observability/observability-backend-go/internal/modules/infrastructure/deployments/service"
	deploymentsstore "github.com/observability/observability-backend-go/internal/modules/infrastructure/deployments/store"
	nodes "github.com/observability/observability-backend-go/internal/modules/infrastructure/nodes"
	nodesservice "github.com/observability/observability-backend-go/internal/modules/infrastructure/nodes/service"
	nodesstore "github.com/observability/observability-backend-go/internal/modules/infrastructure/nodes/store"
	resourceutilisation "github.com/observability/observability-backend-go/internal/modules/infrastructure/resource_utilisation"
	resourceutilisationservice "github.com/observability/observability-backend-go/internal/modules/infrastructure/resource_utilisation/service"
	resourceutilisationstore "github.com/observability/observability-backend-go/internal/modules/infrastructure/resource_utilisation/store"
	overviewerrors "github.com/observability/observability-backend-go/internal/modules/overview/errors"
	overviewerrorsservice "github.com/observability/observability-backend-go/internal/modules/overview/errors/service"
	overviewerrorsstore "github.com/observability/observability-backend-go/internal/modules/overview/errors/store"
	overviewmodule "github.com/observability/observability-backend-go/internal/modules/overview/overview"
	overviewservice "github.com/observability/observability-backend-go/internal/modules/overview/overview/service"
	overviewstore "github.com/observability/observability-backend-go/internal/modules/overview/overview/store"
	overviewslo "github.com/observability/observability-backend-go/internal/modules/overview/slo"
	overviewsloservice "github.com/observability/observability-backend-go/internal/modules/overview/slo/service"
	overviewslostore "github.com/observability/observability-backend-go/internal/modules/overview/slo/store"
	"github.com/observability/observability-backend-go/internal/modules/saturation"
	saturationservice "github.com/observability/observability-backend-go/internal/modules/saturation/service"
	saturationstore "github.com/observability/observability-backend-go/internal/modules/saturation/store"
	servicepage "github.com/observability/observability-backend-go/internal/modules/services/service"
	servicepageservice "github.com/observability/observability-backend-go/internal/modules/services/service/service"
	servicepagestore "github.com/observability/observability-backend-go/internal/modules/services/service/store"
	servicetopology "github.com/observability/observability-backend-go/internal/modules/services/topology"
	servicetopologyservice "github.com/observability/observability-backend-go/internal/modules/services/topology/service"
	servicetopologystore "github.com/observability/observability-backend-go/internal/modules/services/topology/store"
	"github.com/observability/observability-backend-go/internal/platform/auth"
	"github.com/observability/observability-backend-go/internal/platform/handlers"
	"github.com/observability/observability-backend-go/internal/platform/leader"
	appmetrics "github.com/observability/observability-backend-go/internal/platform/metrics"
	"github.com/observability/observability-backend-go/internal/platform/middleware"
	"github.com/observability/observability-backend-go/internal/platform/sse"
	telemetry "github.com/observability/observability-backend-go/modules/ingestion"
	logsapi "github.com/observability/observability-backend-go/modules/log"
	tracesapi "github.com/observability/observability-backend-go/modules/spans"
	identity "github.com/observability/observability-backend-go/modules/user"
	identityservice "github.com/observability/observability-backend-go/modules/user/service"
	identitystore "github.com/observability/observability-backend-go/modules/user/store"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

type App struct {
	DB             *sql.DB
	CH             *sql.DB
	Config         config.Config
	JWTManager     auth.JWTManager
	TokenBlacklist *auth.TokenBlacklist

	SSEBroker     SSEPublisher   // real-time event broker for SSE streams
	LeaderElector leader.Elector // decides which pod runs singleton background jobs

	TelemetryIngester telemetry.Ingester
	TelemetryHandler  *telemetry.Handler       // OTLP handler (owns span cache)
	TelemetryConsumer *telemetry.KafkaConsumer // nil when Kafka is disabled
	RetentionManager  *telemetry.RetentionManager

	Auth                AuthModule
	Users               UserModule
	Alerts              *alerts.AlertHandler
	Deployments         *deployments.DeploymentHandler
	Logs                *logsapi.LogHandler
	Traces              *tracesapi.TraceHandler
	Overview            *overviewmodule.OverviewHandler
	OverviewSLO         *overviewslo.SLOHandler
	OverviewErrors      *overviewerrors.ErrorHandler
	ServicesPage        *servicepage.ServiceHandler
	ServicesTopology    *servicetopology.TopologyHandler
	Nodes               *nodes.NodeHandler
	ResourceUtilisation *resourceutilisation.ResourceUtilisationHandler
	Saturation          *saturation.SaturationHandler
	AI                  *ai.AIHandler
	DashboardConfig     *dashboardconfig.DashboardConfigHandler
}

// SSEPublisher is the interface satisfied by both *sse.Broker (in-process)
// and *sse.RedisBroker (multi-pod Redis Pub/Sub).
type SSEPublisher interface {
	Subscribe(teamID int64) chan sse.Event
	Unsubscribe(teamID int64, ch chan sse.Event)
	Publish(teamID int64, eventType string, data any)
}

type AuthModule interface {
	Login(*gin.Context)
	Logout(*gin.Context)
	AuthMe(*gin.Context)
	AuthContext(*gin.Context)
	ValidateToken(*gin.Context)
}

type UserModule interface {
	GetCurrentUser(*gin.Context)
	GetUsers(*gin.Context)
	GetUserByID(*gin.Context)
	Signup(*gin.Context)
	CreateUser(*gin.Context)
	AddUserToTeam(*gin.Context)
	RemoveUserFromTeam(*gin.Context)
	GetTeams(*gin.Context)
	GetMyTeams(*gin.Context)
	GetTeamByID(*gin.Context)
	GetTeamBySlug(*gin.Context)
	CreateTeam(*gin.Context)
	GetProfile(*gin.Context)
	UpdateProfile(*gin.Context)
}

func New(db *sql.DB, ch *sql.DB, cfg config.Config) *App {
	jwt := auth.JWTManager{
		Secret:     []byte(cfg.JWTSecret),
		Expiration: cfg.JWTDuration(),
	}

	getTenant := handlers.GetTenantFunc(middleware.GetTenant)

	alertCondCol := resolveAlertConditionColumn(db)
	identityTables := identitystore.NewMySQLProvider(db)
	identityAuthService := identityservice.NewAuthService(identityTables, jwt, cfg.JWTExpirationMs)
	identityUserService := identityservice.NewUserService(identityTables)

	// Run all schema migrations under a MySQL advisory lock to prevent races
	// when multiple pods start simultaneously during rolling Kubernetes deploys.

	runMigrations(db, cfg.DefaultRetentionDays)

	blacklist := auth.NewTokenBlacklist(cfg.RedisHost, cfg.RedisPort)

	retentionMgr := telemetry.NewRetentionManager(db, ch, telemetry.RetentionManagerConfig{
		DefaultRetentionDays: cfg.DefaultRetentionDays,
	})

	// Fix 6: Use Redis-backed SSE broker for cross-pod event fanout.
	// When Redis is available all pods share the same Pub/Sub channel:
	// "sse:team:{teamID}". Falls back to in-process on Redis errors.
	redisClient := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%s", cfg.RedisHost, cfg.RedisPort),
	})
	sseBroker := sse.NewRedisBroker(redisClient)

	// Fix 7: Leader election for RetentionManager — only the elected pod runs it.
	// In local dev / single-pod deployments, ProcessElector is always elected.
	// For multi-pod production, swap to leader.NewRedisElector(redisClient, ...) here.
	leaderElector := leader.NewProcessElector()

	return &App{
		DB:               db,
		CH:               ch,
		Config:           cfg,
		JWTManager:       jwt,
		TokenBlacklist:   blacklist,
		SSEBroker:        sseBroker,
		RetentionManager: retentionMgr,
		LeaderElector:    leaderElector,

		Auth:  identity.NewAuthHandler(getTenant, identityAuthService, jwt, blacklist),
		Users: identity.NewUserHandler(getTenant, identityUserService),
		Alerts: &alerts.AlertHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        db,
				GetTenant: getTenant,
			},
			Service: alertsservice.NewService(
				alertsstore.NewRepository(db, alertCondCol),
			),
		},
		Deployments: &deployments.DeploymentHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Service: deploymentsservice.NewService(
				deploymentsstore.NewRepository(database.NewMySQLWrapper(ch)),
			),
		},
		Logs: logsapi.NewHandler(
			getTenant,
			logsapi.NewRepository(database.NewMySQLWrapper(ch)),
		),
		Traces: tracesapi.NewHandler(
			getTenant,
			tracesapi.NewRepository(database.NewMySQLWrapper(ch)),
		),
		Overview: &overviewmodule.OverviewHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Service: overviewservice.NewService(
				overviewstore.NewRepository(database.NewMySQLWrapper(ch)),
			),
		},
		OverviewSLO: &overviewslo.SLOHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Service: overviewsloservice.NewService(
				overviewslostore.NewRepository(database.NewMySQLWrapper(ch)),
			),
		},
		OverviewErrors: &overviewerrors.ErrorHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Service: overviewerrorsservice.NewService(
				overviewerrorsstore.NewRepository(database.NewMySQLWrapper(ch)),
			),
		},
		ServicesPage: &servicepage.ServiceHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Service: servicepageservice.NewService(
				servicepagestore.NewRepository(database.NewMySQLWrapper(ch)),
			),
		},
		ServicesTopology: &servicetopology.TopologyHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Service: servicetopologyservice.NewService(
				servicetopologystore.NewRepository(database.NewMySQLWrapper(ch)),
			),
		},
		Nodes: &nodes.NodeHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Service: nodesservice.NewService(
				nodesstore.NewRepository(database.NewMySQLWrapper(ch)),
			),
		},
		ResourceUtilisation: &resourceutilisation.ResourceUtilisationHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Service: resourceutilisationservice.NewService(
				resourceutilisationstore.NewRepository(database.NewMySQLWrapper(ch)),
			),
		},
		Saturation: &saturation.SaturationHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Service: saturationservice.NewService(
				saturationstore.NewRepository(database.NewMySQLWrapper(ch)),
			),
		},
		AI: &ai.AIHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Service: aiservice.NewService(
				aistore.NewRepository(database.NewMySQLWrapper(ch)),
			),
		},
		DashboardConfig: &dashboardconfig.DashboardConfigHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        db,
				GetTenant: getTenant,
			},
			Service: dashboardconfigservice.NewService(
				dashboardconfigstore.NewRepository(db),
			),
		},
	}
}

func (a *App) Router() *gin.Engine {
	// Register Prometheus metrics collectors once.
	appmetrics.Register()

	r := gin.New()
	r.Use(gin.Logger())
	r.Use(middleware.ErrorRecovery())
	r.Use(middleware.CORSMiddleware())
	r.Use(appmetrics.GinMiddleware())
	r.Use(middleware.TenantMiddleware(a.JWTManager, a.TokenBlacklist))

	// Rate limiting: 1000 requests per second with burst of 2000.
	rl := middleware.NewRateLimiter(1000, 2000, time.Second)
	r.Use(middleware.RateLimitMiddleware(rl))

	// Health check endpoints (unauthenticated).
	r.GET("/health", a.healthLive)
	r.GET("/health/live", a.healthLive)
	r.GET("/health/ready", a.healthReady)

	// Prometheus metrics endpoint (unauthenticated).
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	a.registerRoutes(r)
	a.registerSwagger(r)
	return r
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

	// Start Kafka consumer workers if Kafka mode is enabled.
	// Use a separate context so we can drain consumers AFTER the HTTP server shuts down.
	consumerCtx, consumerCancel := context.WithCancel(context.Background())
	defer consumerCancel()

	if a.TelemetryConsumer != nil {
		a.TelemetryConsumer.Start(consumerCtx)
	}

	// Start the retention manager to periodically enforce per-team data retention.
	if a.RetentionManager != nil {
		a.RetentionManager.Start(consumerCtx)
	}

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
		consumerCancel()
		if a.TelemetryConsumer != nil {
			a.TelemetryConsumer.Wait()
		}
		if a.TelemetryIngester != nil {
			a.TelemetryIngester.Close()
		}
		return err
	case <-ctx.Done():
		log.Println("shutdown signal received, draining connections…")
		if a.RetentionManager != nil {
			a.RetentionManager.Stop()
		}
		if a.TokenBlacklist != nil {
			a.TokenBlacklist.Stop()
		}
		shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Phase 1: Stop accepting new HTTP requests and drain in-flight ones.
		if err := srv.Shutdown(shutCtx); err != nil {
			consumerCancel()
			if a.TelemetryConsumer != nil {
				a.TelemetryConsumer.Wait()
			}
			if a.TelemetryIngester != nil {
				a.TelemetryIngester.Close()
			}
			return fmt.Errorf("http shutdown: %w", err)
		}

		// Phase 2: Close producer, then drain consumer.
		if a.TelemetryIngester != nil {
			log.Println("closing telemetry ingester…")
			a.TelemetryIngester.Close()
		}
		if a.TelemetryConsumer != nil {
			log.Println("draining kafka consumer…")
			consumerCancel()
			a.TelemetryConsumer.Wait()
			a.TelemetryConsumer.Close()
			log.Println("kafka consumer drained")
		}

		return <-errCh
	}
}

func resolveAlertConditionColumn(db *sql.DB) string {
	var count int64
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE()
		  AND TABLE_NAME = 'alerts'
		  AND COLUMN_NAME = 'condition_expr'
	`).Scan(&count); err == nil && count > 0 {
		return "condition_expr"
	}
	return "`condition`"
}
