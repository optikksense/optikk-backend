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
	"github.com/observability/observability-backend-go/internal/modules/alerts"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
	"github.com/observability/observability-backend-go/internal/modules/dashboardconfig"
	"github.com/observability/observability-backend-go/internal/modules/deployments"
	"github.com/observability/observability-backend-go/internal/modules/explore"
	"github.com/observability/observability-backend-go/internal/modules/health"
	"github.com/observability/observability-backend-go/internal/modules/identity"
	"github.com/observability/observability-backend-go/internal/modules/insights"
	logsmodule "github.com/observability/observability-backend-go/internal/modules/logs"
	"github.com/observability/observability-backend-go/internal/modules/metrics"
	"github.com/observability/observability-backend-go/internal/modules/traces"
	"github.com/observability/observability-backend-go/internal/platform/auth"
	"github.com/observability/observability-backend-go/internal/platform/handlers"
	"github.com/observability/observability-backend-go/internal/platform/middleware"
	"github.com/observability/observability-backend-go/internal/telemetry"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

type App struct {
	DB         *sql.DB
	CH         *sql.DB
	Config     config.Config
	JWTManager auth.JWTManager

	TelemetryIngester telemetry.Ingester
	TelemetryConsumer *telemetry.KafkaConsumer // nil when Kafka is disabled

	Auth            AuthModule
	Users           UserModule
	Alerts          *alerts.AlertHandler
	Health          *health.HealthHandler
	Deployments     *deployments.DeploymentHandler
	Logs            *logsmodule.LogHandler
	Traces          *traces.TraceHandler
	Metrics         *metrics.MetricHandler
	Insights        *insights.InsightHandler
	AI              *ai.AIHandler
	DashboardConfig *dashboardconfig.DashboardConfigHandler
	Explore         *explore.ExploreHandler
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
	identityTables := identity.NewMySQLProvider(db)

	// Auto-create dashboard_chart_configs table if needed.
	dcRepo := dashboardconfig.NewRepository(db)
	if err := dcRepo.EnsureTable(); err != nil {
		log.Printf("WARN: dashboard_chart_configs table migration: %v", err)
	}

	exploreRepo := explore.NewRepository(db)
	if err := exploreRepo.EnsureTable(); err != nil {
		log.Printf("WARN: explore_saved_queries table migration: %v", err)
	}

	return &App{
		DB:         db,
		CH:         ch,
		Config:     cfg,
		JWTManager: jwt,

		Auth: &identity.AuthHandler{
			Tables:       identityTables,
			GetTenant:    getTenant,
			JWTManager:   jwt,
			JWTExpiresMs: cfg.JWTExpirationMs,
		},
		Users: &identity.UserHandler{
			Tables:    identityTables,
			GetTenant: getTenant,
		},
		Alerts: &alerts.AlertHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        db,
				GetTenant: getTenant,
			},
			AlertCondCol: alertCondCol,
			Repo:         alerts.NewRepository(db, alertCondCol),
		},
		Health: &health.HealthHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        db,
				GetTenant: getTenant,
			},
			Repo: health.NewRepository(db),
		},
		Deployments: &deployments.DeploymentHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Repo: deployments.NewRepository(database.NewMySQLWrapper(ch)),
		},
		Logs: &logsmodule.LogHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Repo: logsmodule.NewRepository(database.NewMySQLWrapper(ch)),
		},
		Traces: &traces.TraceHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Repo: traces.NewRepository(database.NewMySQLWrapper(ch)),
		},
		Metrics: &metrics.MetricHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Repo: metrics.NewRepository(database.NewMySQLWrapper(ch)),
		},
		Insights: &insights.InsightHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Repo: insights.NewRepository(database.NewMySQLWrapper(ch)),
		},
		AI: &ai.AIHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Repo: ai.NewRepository(database.NewMySQLWrapper(ch)),
		},
		DashboardConfig: &dashboardconfig.DashboardConfigHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        db,
				GetTenant: getTenant,
			},
			Repo: dcRepo,
		},
		Explore: &explore.ExploreHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        db,
				GetTenant: getTenant,
			},
			Repo: exploreRepo,
		},
	}
}

func (a *App) Router() *gin.Engine {
	r := gin.New()
	r.Use(gin.Logger())
	r.Use(middleware.ErrorRecovery())
	r.Use(middleware.CORSMiddleware())
	r.Use(middleware.TenantMiddleware(a.JWTManager))

	a.registerRoutes(r)
	a.registerSwagger(r)
	return r
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
