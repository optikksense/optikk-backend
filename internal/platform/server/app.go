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
	satdatabase "github.com/observability/observability-backend-go/internal/modules/saturation/database"
	"github.com/observability/observability-backend-go/internal/modules/saturation/kafka"
	servicepage "github.com/observability/observability-backend-go/internal/modules/services/service"
	servicetopology "github.com/observability/observability-backend-go/internal/modules/services/topology"
	tracesapi "github.com/observability/observability-backend-go/internal/modules/spans"
	identity "github.com/observability/observability-backend-go/internal/modules/user"
	identityservice "github.com/observability/observability-backend-go/internal/modules/user/service"
	identitystore "github.com/observability/observability-backend-go/internal/modules/user/store"
	"github.com/observability/observability-backend-go/internal/platform/auth"
	"github.com/observability/observability-backend-go/internal/platform/middleware"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

type App struct {
	DB             *sql.DB
	CH             *sql.DB
	Config         config.Config
	JWTManager     auth.JWTManager
	TokenBlacklist *auth.TokenBlacklist

	Auth                AuthModule
	Users               UserModule
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

	getTenant := modulecommon.GetTenantFunc(middleware.GetTenant)

	identityTables := identitystore.NewMySQLProvider(db)
	identityAuthService := identityservice.NewAuthService(identityTables, jwt, cfg.JWTExpirationMs)
	identityUserService := identityservice.NewUserService(identityTables)

	// Run all schema migrations under a MySQL advisory lock to prevent races
	// when multiple pods start simultaneously during rolling Kubernetes deploys.

	runMigrations(db, cfg.DefaultRetentionDays)

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

	return &App{
		DB:             db,
		CH:             ch,
		Config:         cfg,
		JWTManager:     jwt,
		TokenBlacklist: blacklist,

		Auth:  identity.NewAuthHandler(getTenant, identityAuthService, jwt, blacklist),
		Users: identity.NewUserHandler(getTenant, identityUserService),
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
	}
}

func (a *App) Router() *gin.Engine {
	r := gin.New()
	r.Use(gin.Logger())
	r.Use(middleware.ErrorRecovery())
	r.Use(middleware.CORSMiddleware())
	r.Use(middleware.TenantMiddleware(a.JWTManager, a.TokenBlacklist))

	// Rate limiting: 1000 requests per second with burst of 2000.
	rl := middleware.NewRateLimiter(1000, 2000, time.Second)
	r.Use(middleware.RateLimitMiddleware(rl))

	// Health check endpoints (unauthenticated).
	r.GET("/health", a.healthLive)
	r.GET("/health/live", a.healthLive)
	r.GET("/health/ready", a.healthReady)

	a.registerRoutes(r)
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
		shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Phase 2: Stop main API server.
		if err := srv.Shutdown(shutCtx); err != nil {
			return fmt.Errorf("http shutdown: %w", err)
		}

		return <-errCh
	}
}
