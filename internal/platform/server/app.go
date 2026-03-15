package server

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/observability/observability-backend-go/internal/config"
	dbutil "github.com/observability/observability-backend-go/internal/database"
	configdefaults "github.com/observability/observability-backend-go/internal/defaultconfig"
	alertingmod "github.com/observability/observability-backend-go/internal/modules/alerting"
	anomalymod "github.com/observability/observability-backend-go/internal/modules/anomaly"
	"github.com/observability/observability-backend-go/internal/modules/ai"
	aidashboard "github.com/observability/observability-backend-go/internal/modules/ai/dashboard"
	airundetail "github.com/observability/observability-backend-go/internal/modules/ai/rundetail"
	airuns "github.com/observability/observability-backend-go/internal/modules/ai/runs"
	aitraces "github.com/observability/observability-backend-go/internal/modules/ai/traces"
	aiconversations "github.com/observability/observability-backend-go/internal/modules/ai/conversations"
	"github.com/observability/observability-backend-go/internal/modules/apm"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
	defaultconfig "github.com/observability/observability-backend-go/internal/modules/defaultconfig"
	"github.com/observability/observability-backend-go/internal/modules/httpmetrics"
	kubernetes "github.com/observability/observability-backend-go/internal/modules/infrastructure/kubernetes"
	nodes "github.com/observability/observability-backend-go/internal/modules/infrastructure/nodes"
	"github.com/observability/observability-backend-go/internal/modules/infrastructure/resource_utilisation"
	infraCPU "github.com/observability/observability-backend-go/internal/modules/infrastructure/cpu"
	infraMemory "github.com/observability/observability-backend-go/internal/modules/infrastructure/memory"
	infraDisk "github.com/observability/observability-backend-go/internal/modules/infrastructure/disk"
	infraNetwork "github.com/observability/observability-backend-go/internal/modules/infrastructure/network"
	infraJVM "github.com/observability/observability-backend-go/internal/modules/infrastructure/jvm"
	logsapi "github.com/observability/observability-backend-go/internal/modules/log"
	overviewerrors "github.com/observability/observability-backend-go/internal/modules/overview/errors"
	overviewmodule "github.com/observability/observability-backend-go/internal/modules/overview/overview"
	overviewslo "github.com/observability/observability-backend-go/internal/modules/overview/slo"

	"github.com/observability/observability-backend-go/internal/modules/saturation/kafka"
	redisaturation "github.com/observability/observability-backend-go/internal/modules/saturation/redis"
	servicepage "github.com/observability/observability-backend-go/internal/modules/services/service"
	servicemap "github.com/observability/observability-backend-go/internal/modules/services/servicemap"
	servicetopology "github.com/observability/observability-backend-go/internal/modules/services/topology"
	tracesapi "github.com/observability/observability-backend-go/internal/modules/spans"
	errorfingerprint "github.com/observability/observability-backend-go/internal/modules/spans/errorfingerprint"
	errortracking "github.com/observability/observability-backend-go/internal/modules/spans/errortracking"
	livetail "github.com/observability/observability-backend-go/internal/modules/spans/livetail"
	redmetrics "github.com/observability/observability-backend-go/internal/modules/spans/redmetrics"
	spananalytics "github.com/observability/observability-backend-go/internal/modules/spans/analytics"
	savedviews "github.com/observability/observability-backend-go/internal/modules/spans/savedviews"
	tracecompare "github.com/observability/observability-backend-go/internal/modules/spans/tracecompare"
	tracedetail "github.com/observability/observability-backend-go/internal/modules/spans/tracedetail"
	annotationsmod "github.com/observability/observability-backend-go/internal/modules/annotations"
	dashboardsmod "github.com/observability/observability-backend-go/internal/modules/dashboards"
	errorsinbox "github.com/observability/observability-backend-go/internal/modules/errorsinbox"
	explorermod "github.com/observability/observability-backend-go/internal/modules/explorer"
	sharingmod "github.com/observability/observability-backend-go/internal/modules/sharing"
	workloadsmod "github.com/observability/observability-backend-go/internal/modules/workloads"
	databaseotel "github.com/observability/observability-backend-go/internal/modules/database"
	usermodule "github.com/observability/observability-backend-go/internal/modules/user"
	"github.com/observability/observability-backend-go/internal/platform/alerting"
	"github.com/observability/observability-backend-go/internal/platform/auth"
	"github.com/observability/observability-backend-go/internal/platform/cache"
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
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/keepalive"
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
	CPU                 infraCPU.Config
	Memory              infraMemory.Config
	Disk                infraDisk.Config
	Network             infraNetwork.Config
	JVM                 infraJVM.Config

	SaturationKafka     kafka.Config
	SaturationRedis     redisaturation.Config
	Kubernetes          kubernetes.Config
	HTTPMetrics         httpmetrics.Config
	APM                 apm.Config
	Logs                logsapi.Config
	Traces              tracesapi.Config
	TraceDetail         tracedetail.Config
	ServiceMap          servicemap.Config
	REDMetrics          redmetrics.Config
	ErrorTracking       errortracking.Config
	SpanAnalytics       spananalytics.Config
	TraceCompare        tracecompare.Config
	SavedViews          savedviews.Config
	LiveTail            livetail.Config
	ErrorFingerprint    errorfingerprint.Config
	AI                  ai.Config
	DefaultConfig       defaultconfig.Config
	DatabaseOTel        databaseotel.Config
	Annotations         annotationsmod.Config
	Alerting            alertingmod.Config
	Dashboards          dashboardsmod.Config
	ErrorsInbox         errorsinbox.Config
	Explorer            explorermod.Config
	Sharing             sharingmod.Config
	Workloads           workloadsmod.Config
	Anomaly             anomalymod.Config
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
		CPU:                 infraCPU.DefaultConfig(),
		Memory:              infraMemory.DefaultConfig(),
		Disk:                infraDisk.DefaultConfig(),
		Network:             infraNetwork.DefaultConfig(),
		JVM:                 infraJVM.DefaultConfig(),

		SaturationKafka:     kafka.DefaultConfig(),
		SaturationRedis:     redisaturation.DefaultConfig(),
		Kubernetes:          kubernetes.DefaultConfig(),
		HTTPMetrics:         httpmetrics.DefaultConfig(),
		APM:                 apm.DefaultConfig(),
		Logs:                logsapi.DefaultConfig(),
		Traces:              tracesapi.DefaultConfig(),
		TraceDetail:         tracedetail.DefaultConfig(),
		ServiceMap:          servicemap.DefaultConfig(),
		REDMetrics:          redmetrics.DefaultConfig(),
		ErrorTracking:       errortracking.DefaultConfig(),
		SpanAnalytics:       spananalytics.DefaultConfig(),
		TraceCompare:        tracecompare.DefaultConfig(),
		SavedViews:          savedviews.DefaultConfig(),
		LiveTail:            livetail.DefaultConfig(),
		ErrorFingerprint:    errorfingerprint.DefaultConfig(),
		AI:                  ai.DefaultConfig(),
		DefaultConfig:       defaultconfig.DefaultConfig(),
		DatabaseOTel:        databaseotel.DefaultConfig(),
		Annotations:         annotationsmod.DefaultConfig(),
		Alerting:            alertingmod.DefaultConfig(),
		Dashboards:          dashboardsmod.DefaultConfig(),
		ErrorsInbox:         errorsinbox.DefaultConfig(),
		Explorer:            explorermod.DefaultConfig(),
		Sharing:             sharingmod.DefaultConfig(),
		Workloads:           workloadsmod.DefaultConfig(),
		Anomaly:             anomalymod.DefaultConfig(),
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
	OAuth               *usermodule.OAuthHandler
	Logs                *logsapi.LogHandler
	Traces              *tracesapi.TraceHandler
	TraceDetail         *tracedetail.TraceDetailHandler
	ServiceMap          *servicemap.ServiceMapHandler
	REDMetrics          *redmetrics.REDMetricsHandler
	ErrorTracking       *errortracking.ErrorTrackingHandler
	SpanAnalytics       *spananalytics.Handler
	TraceCompare        *tracecompare.Handler
	SavedViews          *savedviews.Handler
	LiveTail            *livetail.Handler
	ErrorFingerprint    *errorfingerprint.Handler
	Overview            *overviewmodule.OverviewHandler
	OverviewSLO         *overviewslo.SLOHandler
	OverviewErrors      *overviewerrors.ErrorHandler
	ServicesPage        *servicepage.ServiceHandler
	ServicesTopology    *servicetopology.TopologyHandler
	Nodes               *nodes.NodeHandler
	ResourceUtilisation *resource_utilisation.ResourceUtilisationHandler
	CPU                 *infraCPU.CPUHandler
	Memory              *infraMemory.MemoryHandler
	Disk                *infraDisk.DiskHandler
	Network             *infraNetwork.NetworkHandler
	JVM                 *infraJVM.JVMHandler

	SaturationKafka     *kafka.KafkaHandler
	SaturationRedis     *redisaturation.RedisHandler
	Kubernetes          *kubernetes.KubernetesHandler
	HTTPMetrics         *httpmetrics.HTTPMetricsHandler
	APM                 *apm.APMHandler
	AI                  *ai.Handlers
	DefaultConfig       *defaultconfig.Handler
	DatabaseOTel        *databaseotel.Handler
	Annotations         *annotationsmod.Handler
	Alerting            *alertingmod.Handler
	Dashboards          *dashboardsmod.Handler
	ErrorsInbox         *errorsinbox.Handler
	Explorer            *explorermod.Handler
	Sharing             *sharingmod.Handler
	Workloads           *workloadsmod.Handler
	Anomaly             *anomalymod.Handler

	// Query result cache (Redis-gated, nil-safe).
	Cache *cache.QueryCache

	// OTLP ingest handlers & queues.
	OTLPHTTP     *otlp.Handler
	OTLPGRPC     *otlpgrpc.Handler
	SpansQueue   *ingest.Queue
	LogsQueue    *ingest.Queue
	MetricsQueue *ingest.Queue
	Tracker      *ingest.ByteTracker
	AlertEngine  *alerting.Engine
}

func New(db *sql.DB, ch *sql.DB, chNative clickhouse.Conn, cfg config.Config) *App {
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
	_ = brokers // Kafka path removed; kept to avoid config breakage
	spansQueue := ingest.NewQueue(chNative, "observability.spans", otlp.SpanColumns, queueOpts...)
	logsQueue := ingest.NewQueue(chNative, "observability.logs", otlp.LogColumns, queueOpts...)
	metricsQueue := ingest.NewQueue(chNative, "observability.metrics", otlp.MetricColumns, queueOpts...)

	// Query result cache — only active when Redis is enabled.
	var queryCache *cache.QueryCache
	if cfg.RedisEnabled {
		rc := redis.NewClient(&redis.Options{
			Addr: fmt.Sprintf("%s:%s", cfg.RedisHost, cfg.RedisPort),
		})
		queryCache = cache.New(rc)
	} else {
		queryCache = cache.New(nil)
	}

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
		Users: usermodule.NewUserHandler(getTenant, userStore, registry),
		OAuth: usermodule.NewOAuthHandler(
			userStore, jwt, cfg.JWTExpirationMs,
			cfg.GoogleClientID, cfg.GoogleClientSecret,
			cfg.GitHubClientID, cfg.GitHubClientSecret,
			cfg.OAuthRedirectBase,
		),
		Logs: logsapi.NewHandler(
			getTenant,
			logsapi.NewRepository(dbutil.NewClickHouseWrapper(ch)),
		),
		Traces: tracesapi.NewHandler(
			getTenant,
			tracesapi.NewRepository(dbutil.NewClickHouseWrapper(ch)),
		),
		SpanAnalytics: spananalytics.NewHandler(
			getTenant,
			spananalytics.NewService(
				spananalytics.NewRepository(dbutil.NewClickHouseWrapper(ch)),
			),
		),
		TraceCompare: tracecompare.NewHandler(
			getTenant,
			tracecompare.NewService(
				tracecompare.NewRepository(dbutil.NewClickHouseWrapper(ch)),
			),
		),
		SavedViews: savedviews.NewHandler(
			getTenant,
			savedviews.NewService(
				savedviews.NewRepository(db),
			),
		),
		LiveTail: livetail.NewHandler(
			getTenant,
			livetail.NewRepository(dbutil.NewClickHouseWrapper(ch)),
		),
		ErrorFingerprint: errorfingerprint.NewHandler(
			getTenant,
			errorfingerprint.NewRepository(dbutil.NewClickHouseWrapper(ch)),
		),
		TraceDetail: &tracedetail.TraceDetailHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Service: tracedetail.NewService(
				tracedetail.NewRepository(dbutil.NewClickHouseWrapper(ch)),
			),
		},
		ServiceMap: &servicemap.ServiceMapHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Service: servicemap.NewService(
				servicemap.NewRepository(dbutil.NewClickHouseWrapper(ch)),
			),
		},
		REDMetrics: &redmetrics.REDMetricsHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Service: redmetrics.NewService(
				redmetrics.NewRepository(dbutil.NewClickHouseWrapper(ch)),
			),
		},
		ErrorTracking: &errortracking.ErrorTrackingHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Service: errortracking.NewService(
				errortracking.NewRepository(dbutil.NewClickHouseWrapper(ch)),
			),
		},
		Overview: &overviewmodule.OverviewHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Service: overviewmodule.NewService(
				overviewmodule.NewRepository(dbutil.NewClickHouseWrapper(ch)),
			),
		},
		OverviewSLO: &overviewslo.SLOHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Service: overviewslo.NewService(
				overviewslo.NewRepository(dbutil.NewClickHouseWrapper(ch)),
			),
		},
		OverviewErrors: &overviewerrors.ErrorHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Service: overviewerrors.NewService(
				overviewerrors.NewRepository(dbutil.NewClickHouseWrapper(ch)),
			),
		},
		ServicesPage: &servicepage.ServiceHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Service: servicepage.NewService(
				servicepage.NewRepository(dbutil.NewClickHouseWrapper(ch)),
			),
		},
		ServicesTopology: &servicetopology.TopologyHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Service: servicetopology.NewService(
				servicetopology.NewRepository(dbutil.NewClickHouseWrapper(ch)),
			),
		},
		Nodes: &nodes.NodeHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Service: nodes.NewService(
				nodes.NewRepository(dbutil.NewClickHouseWrapper(ch)),
			),
		},
		ResourceUtilisation: &resource_utilisation.ResourceUtilisationHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Service: resource_utilisation.NewService(
				resource_utilisation.NewRepository(dbutil.NewClickHouseWrapper(ch)),
			),
		},
		CPU:     infraCPU.NewHandler(dbutil.NewClickHouseWrapper(ch), getTenant),
		Memory:  infraMemory.NewHandler(dbutil.NewClickHouseWrapper(ch), getTenant),
		Disk:    infraDisk.NewHandler(dbutil.NewClickHouseWrapper(ch), getTenant),
		Network: infraNetwork.NewHandler(dbutil.NewClickHouseWrapper(ch), getTenant),
		JVM:     infraJVM.NewHandler(dbutil.NewClickHouseWrapper(ch), getTenant),

		SaturationKafka: &kafka.KafkaHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Service: kafka.NewService(
				kafka.NewRepository(dbutil.NewClickHouseWrapper(ch)),
			),
		},
		SaturationRedis: &redisaturation.RedisHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Service: redisaturation.NewService(
				redisaturation.NewRepository(dbutil.NewClickHouseWrapper(ch)),
			),
		},
		Kubernetes: &kubernetes.KubernetesHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Service: kubernetes.NewService(
				kubernetes.NewRepository(dbutil.NewClickHouseWrapper(ch)),
			),
		},
		HTTPMetrics: &httpmetrics.HTTPMetricsHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Service: httpmetrics.NewService(
				httpmetrics.NewRepository(dbutil.NewClickHouseWrapper(ch)),
			),
		},
		APM: &apm.APMHandler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Service: apm.NewService(
				apm.NewRepository(dbutil.NewClickHouseWrapper(ch)),
			),
		},
		AI: &ai.Handlers{
			Dashboard: &aidashboard.Handler{
				DBTenant: modulecommon.DBTenant{
					DB:        ch,
					GetTenant: getTenant,
				},
				Service: aidashboard.NewService(
					aidashboard.NewRepository(dbutil.NewClickHouseWrapper(ch)),
				),
			},
			Runs: &airuns.Handler{
				DBTenant: modulecommon.DBTenant{
					DB:        ch,
					GetTenant: getTenant,
				},
				Service: airuns.NewService(
					airuns.NewRepository(dbutil.NewClickHouseWrapper(ch)),
				),
			},
			RunDetail: &airundetail.Handler{
				DBTenant: modulecommon.DBTenant{
					DB:        ch,
					GetTenant: getTenant,
				},
				Service: airundetail.NewService(
					airundetail.NewRepository(dbutil.NewClickHouseWrapper(ch)),
				),
			},
			Traces: &aitraces.Handler{
				DBTenant: modulecommon.DBTenant{
					DB:        ch,
					GetTenant: getTenant,
				},
				Service: aitraces.NewService(
					aitraces.NewRepository(dbutil.NewClickHouseWrapper(ch)),
				),
			},
			Conversations: &aiconversations.Handler{
				DBTenant: modulecommon.DBTenant{
					DB:        ch,
					GetTenant: getTenant,
				},
				Service: aiconversations.NewService(
					aiconversations.NewRepository(dbutil.NewClickHouseWrapper(ch)),
				),
			},
		},
		DefaultConfig: &defaultconfig.Handler{
			DBTenant: modulecommon.DBTenant{
				DB:        db,
				GetTenant: getTenant,
			},
			Repo:     defaultconfig.NewRepository(db),
			Registry: registry,
		},

		DatabaseOTel: &databaseotel.Handler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Service: databaseotel.NewService(
				databaseotel.NewRepository(dbutil.NewClickHouseWrapper(ch)),
			),
		},

		Alerting: alertingmod.NewHandler(
			getTenant,
			alertingmod.NewService(
				alertingmod.NewRepository(db),
			),
		),

		Dashboards: dashboardsmod.NewHandler(
			getTenant,
			dashboardsmod.NewService(
				dashboardsmod.NewRepository(db),
			),
		),

		ErrorsInbox: errorsinbox.NewHandler(
			getTenant,
			errorsinbox.NewService(
				errorsinbox.NewRepository(db),
			),
		),

		Explorer: &explorermod.Handler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Service: explorermod.NewService(
				explorermod.NewRepository(dbutil.NewClickHouseWrapper(ch)),
			),
		},

		Sharing: sharingmod.NewHandler(
			getTenant,
			sharingmod.NewService(
				sharingmod.NewRepository(db),
			),
		),

		Workloads: workloadsmod.NewHandler(
			getTenant,
			workloadsmod.NewService(
				workloadsmod.NewRepository(db),
			),
		),

		Anomaly: &anomalymod.Handler{
			DBTenant: modulecommon.DBTenant{
				DB:        ch,
				GetTenant: getTenant,
			},
			Service: anomalymod.NewService(
				anomalymod.NewRepository(dbutil.NewClickHouseWrapper(ch)),
			),
		},

		Annotations: annotationsmod.NewHandler(
			getTenant,
			annotationsmod.NewService(
				annotationsmod.NewRepository(db),
			),
		),

		Cache:      queryCache,

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
	r.Use(middleware.RequestIDMiddleware())
	r.Use(middleware.APIDebugLogger(a.Config.DebugAPILogs))
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
	r.Use(middleware.APIDebugLogger(a.Config.DebugAPILogs))
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

	// Cached route group — applies 30s Redis cache to expensive aggregation endpoints.
	// When Redis is disabled the middleware is a no-op and requests pass through.
	cached := v1.Group("", cache.CacheResponse(a.Cache, 30*time.Second))

	usermodule.RegisterRoutes(cfg.Identity, v1, a.Auth, a.Users, a.OAuth)
	overviewmodule.RegisterRoutes(cfg.Overview, cached, a.Overview)
	overviewslo.RegisterRoutes(cfg.OverviewSLO, cached, a.OverviewSLO)
	overviewerrors.RegisterRoutes(cfg.OverviewErrors, cached, a.OverviewErrors)
	servicepage.RegisterRoutes(cfg.ServicesPage, cached, a.ServicesPage)
	servicetopology.RegisterRoutes(cfg.ServicesTopology, cached, a.ServicesTopology)
	nodes.RegisterRoutes(cfg.Nodes, cached, a.Nodes)
	resource_utilisation.RegisterRoutes(cfg.ResourceUtilisation, cached, a.ResourceUtilisation)
	infraCPU.RegisterRoutes(cfg.CPU, cached, a.CPU)
	infraMemory.RegisterRoutes(cfg.Memory, cached, a.Memory)
	infraDisk.RegisterRoutes(cfg.Disk, cached, a.Disk)
	infraNetwork.RegisterRoutes(cfg.Network, cached, a.Network)
	infraJVM.RegisterRoutes(cfg.JVM, cached, a.JVM)

	kafka.RegisterRoutes(cfg.SaturationKafka, cached, a.SaturationKafka)
	redisaturation.RegisterRoutes(cfg.SaturationRedis, cached, a.SaturationRedis)
	kubernetes.RegisterRoutes(cfg.Kubernetes, cached, a.Kubernetes)
	httpmetrics.RegisterRoutes(cfg.HTTPMetrics, cached, a.HTTPMetrics)
	apm.RegisterRoutes(cfg.APM, cached, a.APM)
	logsapi.RegisterRoutes(cfg.Logs, v1, a.Logs)
	tracesapi.RegisterRoutes(cfg.Traces, v1, a.Traces)
	spananalytics.RegisterRoutes(cfg.SpanAnalytics, v1, a.SpanAnalytics)
	tracecompare.RegisterRoutes(cfg.TraceCompare, v1, a.TraceCompare)
	savedviews.RegisterRoutes(cfg.SavedViews, v1, a.SavedViews)
	livetail.RegisterRoutes(cfg.LiveTail, v1, a.LiveTail)
	errorfingerprint.RegisterRoutes(cfg.ErrorFingerprint, v1, a.ErrorFingerprint)
	tracedetail.RegisterRoutes(cfg.TraceDetail, v1, a.TraceDetail)
	servicemap.RegisterRoutes(cfg.ServiceMap, cached, a.ServiceMap)
	redmetrics.RegisterRoutes(cfg.REDMetrics, cached, a.REDMetrics)
	errortracking.RegisterRoutes(cfg.ErrorTracking, cached, a.ErrorTracking)
	ai.RegisterRoutes(cfg.AI, v1, a.AI)
	defaultconfig.RegisterRoutes(cfg.DefaultConfig, v1, a.DefaultConfig)
	databaseotel.RegisterRoutes(cfg.DatabaseOTel, cached, a.DatabaseOTel)
	annotationsmod.RegisterRoutes(cfg.Annotations, v1, a.Annotations)
	alertingmod.RegisterRoutes(cfg.Alerting, v1, a.Alerting)
	dashboardsmod.RegisterRoutes(cfg.Dashboards, v1, a.Dashboards)
	errorsinbox.RegisterRoutes(cfg.ErrorsInbox, v1, a.ErrorsInbox)
	explorermod.RegisterRoutes(cfg.Explorer, v1, a.Explorer)
	sharingmod.RegisterRoutes(cfg.Sharing, v1, a.Sharing)
	workloadsmod.RegisterRoutes(cfg.Workloads, v1, a.Workloads)
	anomalymod.RegisterRoutes(cfg.Anomaly, cached, a.Anomaly)
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
	result := gin.H{"status": "ready", "mysql": "ok", "clickhouse": "ok"}
	if a.Cache != nil && a.Cache.Enabled() {
		if err := a.Cache.Ping(c.Request.Context()); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not_ready", "redis": err.Error()})
			return
		}
		result["redis"] = "ok"
	}
	c.JSON(http.StatusOK, result)
}

func (a *App) Start(ctx context.Context) error {
	// Start background metric tracking / alert evaluation
	a.Tracker.Start()
	a.AlertEngine.Start()

	// 1. Setup Main API Server (9090)
	mainRouter := a.Router()
	mainSrv := &http.Server{
		Addr:         fmt.Sprintf(":%s", a.Config.Port),
		Handler:      h2c.NewHandler(mainRouter, &http2.Server{}),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// 2. Setup OTLP HTTP Server (4318)
	otlpRouter := a.OTLPRouter()
	otlpSrv := &http.Server{
		Addr:         fmt.Sprintf(":%s", a.Config.HTTPPortOTLP),
		Handler:      h2c.NewHandler(otlpRouter, &http2.Server{}),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// 3. Setup OTLP gRPC Server (4317)
	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", a.Config.GRPCPort))
	if err != nil {
		return fmt.Errorf("gRPC listen failed: %v", err)
	}
	grpcSrv := grpc.NewServer(
		grpc.MaxRecvMsgSize(a.Config.GRPCMaxRecvMsgSizeMB*1024*1024),
		grpc.MaxConcurrentStreams(100),
		grpc.ConnectionTimeout(30*time.Second),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    20 * time.Second,
			Timeout: 10 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
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
		grpcDone := make(chan struct{})
		go func() {
			grpcSrv.GracefulStop()
			close(grpcDone)
		}()
		select {
		case <-grpcDone:
		case <-time.After(10 * time.Second):
			log.Println("WARN: gRPC graceful stop timed out, forcing stop")
			grpcSrv.Stop()
		}
		if err := mainSrv.Shutdown(shutCtx); err != nil {
			log.Printf("WARN: main api shutdown error: %v", err)
		}
		if err := otlpSrv.Shutdown(shutCtx); err != nil {
			log.Printf("WARN: otlp http shutdown error: %v", err)
		}

		return nil
	}
}
