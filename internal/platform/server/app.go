package server

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/observability/observability-backend-go/internal/config"
	dbutil "github.com/observability/observability-backend-go/internal/database"
	configdefaults "github.com/observability/observability-backend-go/internal/defaultconfig"
	"github.com/observability/observability-backend-go/internal/modules/ai"
	"github.com/observability/observability-backend-go/internal/modules/apm"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
	defaultconfig "github.com/observability/observability-backend-go/internal/modules/defaultconfig"
	"github.com/observability/observability-backend-go/internal/modules/httpmetrics"
	kubernetes "github.com/observability/observability-backend-go/internal/modules/infrastructure/kubernetes"
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
	servicemap "github.com/observability/observability-backend-go/internal/modules/services/servicemap"
	servicetopology "github.com/observability/observability-backend-go/internal/modules/services/topology"
	tracesapi "github.com/observability/observability-backend-go/internal/modules/spans"
	errortracking "github.com/observability/observability-backend-go/internal/modules/spans/errortracking"
	redmetrics "github.com/observability/observability-backend-go/internal/modules/spans/redmetrics"
	tracedetail "github.com/observability/observability-backend-go/internal/modules/spans/tracedetail"
	usermodule "github.com/observability/observability-backend-go/internal/modules/user"
	"github.com/observability/observability-backend-go/internal/platform/alerting"
	"github.com/observability/observability-backend-go/internal/platform/auth"
	"github.com/observability/observability-backend-go/internal/platform/ingest"
	"github.com/observability/observability-backend-go/internal/platform/middleware"
	"github.com/observability/observability-backend-go/internal/platform/otlp"
	otlpauth "github.com/observability/observability-backend-go/internal/platform/otlp/auth"
	otlpgrpc "github.com/observability/observability-backend-go/internal/platform/otlp/grpc"
	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
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
	Kubernetes          kubernetes.Config
	HTTPMetrics         httpmetrics.Config
	APM                 apm.Config
	Logs                logsapi.Config
	Traces              tracesapi.Config
	TraceDetail         tracedetail.Config
	ServiceMap          servicemap.Config
	REDMetrics          redmetrics.Config
	ErrorTracking       errortracking.Config
	AI                  ai.Config
	DefaultConfig       defaultconfig.Config
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
		Kubernetes:          kubernetes.DefaultConfig(),
		HTTPMetrics:         httpmetrics.DefaultConfig(),
		APM:                 apm.DefaultConfig(),
		Logs:                logsapi.DefaultConfig(),
		Traces:              tracesapi.DefaultConfig(),
		TraceDetail:         tracedetail.DefaultConfig(),
		ServiceMap:          servicemap.DefaultConfig(),
		REDMetrics:          redmetrics.DefaultConfig(),
		ErrorTracking:       errortracking.DefaultConfig(),
		AI:                  ai.DefaultConfig(),
		DefaultConfig:       defaultconfig.DefaultConfig(),
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
	TraceDetail         *tracedetail.TraceDetailHandler
	ServiceMap          *servicemap.ServiceMapHandler
	REDMetrics          *redmetrics.REDMetricsHandler
	ErrorTracking       *errortracking.ErrorTrackingHandler
	Overview            *overviewmodule.OverviewHandler
	OverviewSLO         *overviewslo.SLOHandler
	OverviewErrors      *overviewerrors.ErrorHandler
	ServicesPage        *servicepage.ServiceHandler
	ServicesTopology    *servicetopology.TopologyHandler
	Nodes               *nodes.NodeHandler
	ResourceUtilisation *resource_utilisation.ResourceUtilisationHandler
	SaturationDatabase  *satdatabase.DatabaseHandler
	SaturationKafka     *kafka.KafkaHandler
	Kubernetes          *kubernetes.KubernetesHandler
	HTTPMetrics         *httpmetrics.HTTPMetricsHandler
	APM                 *apm.APMHandler
	AI                  *ai.AIHandler
	DefaultConfig       *defaultconfig.Handler

	// OTLP ingest handlers & queues.
	OTLPHTTP     *otlp.Handler
	OTLPGRPC     *otlpgrpc.Handler
	SpansQueue   *ingest.Queue
	LogsQueue    *ingest.Queue
	MetricsQueue *ingest.Queue
	Tracker      *ingest.ByteTracker
	AlertEngine  *alerting.Engine
}

func New(db *sql.DB, ch *sql.DB, cfg config.Config) *App {
	jwt := auth.JWTManager{
		Secret:     []byte(cfg.JWTSecret),
		Expiration: cfg.JWTDuration(),
	}

	getTenant := modulecommon.GetTenantFunc(middleware.GetTenant)

	userStore := usermodule.NewStore(db)

	registry, err := configdefaults.Load()
	if err != nil {
		log.Fatalf("failed to load embedded default config registry: %v", err)
	}

	var blacklist *auth.TokenBlacklist

	if cfg.RedisEnabled {
		blacklist = auth.NewTokenBlacklist(cfg.RedisHost, cfg.RedisPort)
	} else {
		blacklist = auth.NewInMemoryTokenBlacklist()
	}

	// OTLP ingest queues — batch-write to ClickHouse.
	queueOpts := []ingest.Option{
		ingest.WithBatchSize(cfg.QueueBatchSize),
		ingest.WithFlushInterval(int(cfg.QueueFlushIntervalMs)),
	}
	brokers := cfg.KafkaBrokerList()
	spansQueue := ingest.NewQueue(ch, cfg.KafkaEnabled, brokers, "observability.spans", otlp.SpanColumns, queueOpts...)
	logsQueue := ingest.NewQueue(ch, cfg.KafkaEnabled, brokers, "observability.logs", otlp.LogColumns, queueOpts...)
	metricsQueue := ingest.NewQueue(ch, cfg.KafkaEnabled, brokers, "observability.metrics", otlp.MetricColumns, queueOpts...)

	authResolver := otlpauth.NewAuthenticator(db)
	tracker := ingest.NewByteTracker(db, time.Hour)
	otlpHTTPHandler := otlp.NewHandler(authResolver, spansQueue, logsQueue, metricsQueue, tracker)
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
		TraceDetail: &tracedetail.TraceDetailHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Service: tracedetail.NewService(
				tracedetail.NewRepository(dbutil.NewMySQLWrapper(ch)),
			),
		},
		ServiceMap: &servicemap.ServiceMapHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Service: servicemap.NewService(
				servicemap.NewRepository(dbutil.NewMySQLWrapper(ch)),
			),
		},
		REDMetrics: &redmetrics.REDMetricsHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Service: redmetrics.NewService(
				redmetrics.NewRepository(dbutil.NewMySQLWrapper(ch)),
			),
		},
		ErrorTracking: &errortracking.ErrorTrackingHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Service: errortracking.NewService(
				errortracking.NewRepository(dbutil.NewMySQLWrapper(ch)),
			),
		},
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
		Kubernetes: &kubernetes.KubernetesHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Service: kubernetes.NewService(
				kubernetes.NewRepository(dbutil.NewMySQLWrapper(ch)),
			),
		},
		HTTPMetrics: &httpmetrics.HTTPMetricsHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Service: httpmetrics.NewService(
				httpmetrics.NewRepository(dbutil.NewMySQLWrapper(ch)),
			),
		},
		APM: &apm.APMHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Service: apm.NewService(
				apm.NewRepository(dbutil.NewMySQLWrapper(ch)),
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
		DefaultConfig: &defaultconfig.Handler{
			DBTenant: modulecommon.DBTenant{
				DB:        db,
				GetTenant: getTenant,
			},
			Repo:     defaultconfig.NewRepository(db),
			Registry: registry,
		},

		OTLPHTTP:   otlpHTTPHandler,
		OTLPGRPC:   otlpGRPCHandler,
		SpansQueue:     spansQueue,
		LogsQueue:      logsQueue,
		MetricsQueue:   metricsQueue,
		Tracker:        tracker,
		AlertEngine:    alerting.NewEngine(db, ch),
	}
}

func (a *App) Router() *gin.Engine {
	r := gin.New()
	r.Use(gin.Logger())
	r.Use(middleware.ErrorRecovery())
	r.Use(middleware.CORSMiddleware(a.Config.AllowedOrigins))

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

func (a *App) OTLPRouter() *gin.Engine {
	r := gin.New()
	r.Use(gin.Logger())
	r.Use(middleware.ErrorRecovery())
	r.Use(middleware.CORSMiddleware(a.Config.AllowedOrigins))

	// Global rate limiting for ingest: 1000 requests per second with burst of 2000.
	rl := middleware.NewRateLimiter(1000, 2000, time.Second)
	r.Use(middleware.RateLimitMiddleware(rl))

	// Use the OTLP HTTP handler to register routes
	// Note: Authentication is handled inside the handler (resolves API key from headers)
	a.OTLPHTTP.RegisterRoutes(r.Group(""))

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
	kubernetes.RegisterRoutes(cfg.Kubernetes, v1, a.Kubernetes)
	httpmetrics.RegisterRoutes(cfg.HTTPMetrics, v1, a.HTTPMetrics)
	apm.RegisterRoutes(cfg.APM, v1, a.APM)
	logsapi.RegisterRoutes(cfg.Logs, v1, a.Logs)
	tracesapi.RegisterRoutes(cfg.Traces, v1, a.Traces)
	tracedetail.RegisterRoutes(cfg.TraceDetail, v1, a.TraceDetail)
	servicemap.RegisterRoutes(cfg.ServiceMap, v1, a.ServiceMap)
	redmetrics.RegisterRoutes(cfg.REDMetrics, v1, a.REDMetrics)
	errortracking.RegisterRoutes(cfg.ErrorTracking, v1, a.ErrorTracking)
	ai.RegisterRoutes(cfg.AI, v1, a.AI)
	defaultconfig.RegisterRoutes(cfg.DefaultConfig, v1, a.DefaultConfig)
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
	// Start background metric tracking / alert evaluation
	a.Tracker.Start()
	a.AlertEngine.Start()

	// 1. Setup Main API Server (9090)
	mainRouter := a.Router()
	mainSrv := &http.Server{
		Addr:    fmt.Sprintf(":%s", a.Config.Port),
		Handler: h2c.NewHandler(mainRouter, &http2.Server{}),
	}

	// 2. Setup OTLP HTTP Server (4318)
	otlpRouter := a.OTLPRouter()
	otlpSrv := &http.Server{
		Addr:    fmt.Sprintf(":%s", a.Config.HTTPPortOTLP),
		Handler: h2c.NewHandler(otlpRouter, &http2.Server{}),
	}

	// 3. Setup OTLP gRPC Server (4317)
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", a.Config.GRPCPort))
	if err != nil {
		return fmt.Errorf("gRPC listen failed: %v", err)
	}
	grpcSrv := grpc.NewServer(
		grpc.MaxRecvMsgSize(a.Config.GRPCMaxRecvMsgSizeMB * 1024 * 1024),
	)
	tracepb.RegisterTraceServiceServer(grpcSrv, a.OTLPGRPC.TraceServer)
	logspb.RegisterLogsServiceServer(grpcSrv, a.OTLPGRPC.LogsServer)
	metricspb.RegisterMetricsServiceServer(grpcSrv, a.OTLPGRPC.MetricsServer)

	// Enable reflection for tools like grpcurl
	reflection.Register(grpcSrv)

	errCh := make(chan error, 3)

	// Start Main API Server
	go func() {
		log.Printf("Main API server starting on %s", mainSrv.Addr)
		if err := mainSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- fmt.Errorf("main api server: %w", err)
		}
	}()

	// Start OTLP HTTP Server
	go func() {
		log.Printf("OTLP HTTP server starting on %s", otlpSrv.Addr)
		if err := otlpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- fmt.Errorf("otlp http server: %w", err)
		}
	}()

	// Start OTLP gRPC Server
	go func() {
		log.Printf("OTLP gRPC server starting on %s", a.Config.GRPCPort)
		if err := grpcSrv.Serve(lis); err != nil {
			errCh <- fmt.Errorf("otlp grpc server: %w", err)
		}
	}()

	select {
	case err := <-errCh:
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

		// Phase 2: Shutdown HTTP servers and stop gRPC
		grpcSrv.GracefulStop()
		if err := mainSrv.Shutdown(shutCtx); err != nil {
			log.Printf("WARN: main api shutdown error: %v", err)
		}
		if err := otlpSrv.Shutdown(shutCtx); err != nil {
			log.Printf("WARN: otlp http shutdown error: %v", err)
		}

		return nil
	}
}
