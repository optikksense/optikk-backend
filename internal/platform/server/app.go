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
	"github.com/observability/observability-backend-go/internal/config"
	dbutil "github.com/observability/observability-backend-go/internal/database"
	configdefaults "github.com/observability/observability-backend-go/internal/defaultconfig"
	aiconversations "github.com/observability/observability-backend-go/internal/modules/ai/conversations"
	aidashboard "github.com/observability/observability-backend-go/internal/modules/ai/dashboard"
	airundetail "github.com/observability/observability-backend-go/internal/modules/ai/rundetail"
	airuns "github.com/observability/observability-backend-go/internal/modules/ai/runs"
	aitraces "github.com/observability/observability-backend-go/internal/modules/ai/traces"
	"github.com/observability/observability-backend-go/internal/modules/apm"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
	dbcollection "github.com/observability/observability-backend-go/internal/modules/database/collection"
	dbconnections "github.com/observability/observability-backend-go/internal/modules/database/connections"
	dberrors "github.com/observability/observability-backend-go/internal/modules/database/errors"
	dblatency "github.com/observability/observability-backend-go/internal/modules/database/latency"
	dbslowqueries "github.com/observability/observability-backend-go/internal/modules/database/slowqueries"
	dbsummary "github.com/observability/observability-backend-go/internal/modules/database/summary"
	dbsystem "github.com/observability/observability-backend-go/internal/modules/database/system"
	dbsystems "github.com/observability/observability-backend-go/internal/modules/database/systems"
	dbvolume "github.com/observability/observability-backend-go/internal/modules/database/volume"
	defaultconfig "github.com/observability/observability-backend-go/internal/modules/defaultconfig"
	"github.com/observability/observability-backend-go/internal/modules/httpmetrics"
	infraCPU "github.com/observability/observability-backend-go/internal/modules/infrastructure/cpu"
	infraDisk "github.com/observability/observability-backend-go/internal/modules/infrastructure/disk"
	infraJVM "github.com/observability/observability-backend-go/internal/modules/infrastructure/jvm"
	kubernetes "github.com/observability/observability-backend-go/internal/modules/infrastructure/kubernetes"
	infraMemory "github.com/observability/observability-backend-go/internal/modules/infrastructure/memory"
	infraNetwork "github.com/observability/observability-backend-go/internal/modules/infrastructure/network"
	nodes "github.com/observability/observability-backend-go/internal/modules/infrastructure/nodes"
	"github.com/observability/observability-backend-go/internal/modules/infrastructure/resource_utilisation"
	loganalytics "github.com/observability/observability-backend-go/internal/modules/log/analytics"
	logdetail "github.com/observability/observability-backend-go/internal/modules/log/detail"
	logsearch "github.com/observability/observability-backend-go/internal/modules/log/search"
	logtracelogs "github.com/observability/observability-backend-go/internal/modules/log/tracelogs"
	overviewerrors "github.com/observability/observability-backend-go/internal/modules/overview/errors"
	overviewmodule "github.com/observability/observability-backend-go/internal/modules/overview/overview"
	overviewslo "github.com/observability/observability-backend-go/internal/modules/overview/slo"
	"github.com/redis/go-redis/v9"

	"github.com/observability/observability-backend-go/internal/modules/saturation/kafka"
	redisaturation "github.com/observability/observability-backend-go/internal/modules/saturation/redis"
	servicepage "github.com/observability/observability-backend-go/internal/modules/services/service"
	servicemap "github.com/observability/observability-backend-go/internal/modules/services/servicemap"
	servicetopology "github.com/observability/observability-backend-go/internal/modules/services/topology"
	spananalytics "github.com/observability/observability-backend-go/internal/modules/spans/analytics"
	errorfingerprint "github.com/observability/observability-backend-go/internal/modules/spans/errorfingerprint"
	errortracking "github.com/observability/observability-backend-go/internal/modules/spans/errortracking"
	livetail "github.com/observability/observability-backend-go/internal/modules/spans/livetail"
	redmetrics "github.com/observability/observability-backend-go/internal/modules/spans/redmetrics"
	tracecompare "github.com/observability/observability-backend-go/internal/modules/spans/tracecompare"
	tracedetail "github.com/observability/observability-backend-go/internal/modules/spans/tracedetail"
	tracesapi "github.com/observability/observability-backend-go/internal/modules/spans/traces"
	userauth "github.com/observability/observability-backend-go/internal/modules/user/auth"
	userteam "github.com/observability/observability-backend-go/internal/modules/user/team"
	userpage "github.com/observability/observability-backend-go/internal/modules/user/user"
	"github.com/observability/observability-backend-go/internal/platform/cache"
	"github.com/observability/observability-backend-go/internal/platform/ingest"
	"github.com/observability/observability-backend-go/internal/platform/middleware"
	"github.com/observability/observability-backend-go/internal/platform/otlp"
	otlpauth "github.com/observability/observability-backend-go/internal/platform/otlp/auth"
	otlpgrpc "github.com/observability/observability-backend-go/internal/platform/otlp/grpc"
	sessionauth "github.com/observability/observability-backend-go/internal/platform/session"
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
	UserAuth            userauth.Config
	UserPage            userpage.Config
	UserTeam            userteam.Config
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

	SaturationKafka  kafka.Config
	SaturationRedis  redisaturation.Config
	Kubernetes       kubernetes.Config
	HTTPMetrics      httpmetrics.Config
	APM              apm.Config
	LogSearch        logsearch.Config
	LogAnalytics     loganalytics.Config
	LogDetail        logdetail.Config
	LogTraceLogs     logtracelogs.Config
	Traces           tracesapi.Config
	TraceDetail      tracedetail.Config
	ServiceMap       servicemap.Config
	REDMetrics       redmetrics.Config
	ErrorTracking    errortracking.Config
	SpanAnalytics    spananalytics.Config
	TraceCompare     tracecompare.Config
	LiveTail         livetail.Config
	ErrorFingerprint errorfingerprint.Config
	AIDashboard      aidashboard.Config
	AIRuns           airuns.Config
	AIRunDetail      airundetail.Config
	AITraces         aitraces.Config
	AIConversations  aiconversations.Config
	DefaultConfig    defaultconfig.Config
	DatabaseSummary  dbsummary.Config
	DatabaseSystems  dbsystems.Config
	DatabaseLatency  dblatency.Config
	DatabaseVolume   dbvolume.Config
	DatabaseSlow     dbslowqueries.Config
	DatabaseErrors   dberrors.Config
	DatabaseConn     dbconnections.Config
	DatabaseCollect  dbcollection.Config
	DatabaseSystem   dbsystem.Config
}

func defaultModuleConfigs() moduleConfigs {
	return moduleConfigs{
		UserAuth:            userauth.DefaultConfig(),
		UserPage:            userpage.DefaultConfig(),
		UserTeam:            userteam.DefaultConfig(),
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

		SaturationKafka:  kafka.DefaultConfig(),
		SaturationRedis:  redisaturation.DefaultConfig(),
		Kubernetes:       kubernetes.DefaultConfig(),
		HTTPMetrics:      httpmetrics.DefaultConfig(),
		APM:              apm.DefaultConfig(),
		LogSearch:        logsearch.DefaultConfig(),
		LogAnalytics:     loganalytics.DefaultConfig(),
		LogDetail:        logdetail.DefaultConfig(),
		LogTraceLogs:     logtracelogs.DefaultConfig(),
		Traces:           tracesapi.DefaultConfig(),
		TraceDetail:      tracedetail.DefaultConfig(),
		ServiceMap:       servicemap.DefaultConfig(),
		REDMetrics:       redmetrics.DefaultConfig(),
		ErrorTracking:    errortracking.DefaultConfig(),
		SpanAnalytics:    spananalytics.DefaultConfig(),
		TraceCompare:     tracecompare.DefaultConfig(),
		LiveTail:         livetail.DefaultConfig(),
		ErrorFingerprint: errorfingerprint.DefaultConfig(),
		AIDashboard:      aidashboard.DefaultConfig(),
		AIRuns:           airuns.DefaultConfig(),
		AIRunDetail:      airundetail.DefaultConfig(),
		AITraces:         aitraces.DefaultConfig(),
		AIConversations:  aiconversations.DefaultConfig(),
		DefaultConfig:    defaultconfig.DefaultConfig(),
		DatabaseSummary:  dbsummary.DefaultConfig(),
		DatabaseSystems:  dbsystems.DefaultConfig(),
		DatabaseLatency:  dblatency.DefaultConfig(),
		DatabaseVolume:   dbvolume.DefaultConfig(),
		DatabaseSlow:     dbslowqueries.DefaultConfig(),
		DatabaseErrors:   dberrors.DefaultConfig(),
		DatabaseConn:     dbconnections.DefaultConfig(),
		DatabaseCollect:  dbcollection.DefaultConfig(),
		DatabaseSystem:   dbsystem.DefaultConfig(),
	}
}

type App struct {
	DB             *sql.DB
	CH             clickhouse.Conn
	Redis          *redis.Client
	Config         config.Config
	SessionManager *sessionauth.Manager

	Auth                *userauth.Handler
	User                *userpage.Handler
	Team                *userteam.Handler
	LogSearch           *logsearch.Handler
	LogAnalytics        *loganalytics.Handler
	LogDetail           *logdetail.Handler
	LogTraceLogs        *logtracelogs.Handler
	Traces              *tracesapi.TraceHandler
	TraceDetail         *tracedetail.TraceDetailHandler
	ServiceMap          *servicemap.ServiceMapHandler
	REDMetrics          *redmetrics.REDMetricsHandler
	ErrorTracking       *errortracking.ErrorTrackingHandler
	SpanAnalytics       *spananalytics.Handler
	TraceCompare        *tracecompare.Handler
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

	SaturationKafka *kafka.KafkaHandler
	SaturationRedis *redisaturation.RedisHandler
	Kubernetes      *kubernetes.KubernetesHandler
	HTTPMetrics     *httpmetrics.HTTPMetricsHandler
	APM             *apm.APMHandler
	AIDashboard     *aidashboard.Handler
	AIRuns          *airuns.Handler
	AIRunDetail     *airundetail.Handler
	AITraces        *aitraces.Handler
	AIConversations *aiconversations.Handler
	DefaultConfig   *defaultconfig.Handler
	DatabaseSummary *dbsummary.Handler
	DatabaseSystems *dbsystems.Handler
	DatabaseLatency *dblatency.Handler
	DatabaseVolume  *dbvolume.Handler
	DatabaseSlow    *dbslowqueries.Handler
	DatabaseErrors  *dberrors.Handler
	DatabaseConn    *dbconnections.Handler
	DatabaseCollect *dbcollection.Handler
	DatabaseSystem  *dbsystem.Handler

	// Query result cache (Redis-gated, nil-safe).
	Cache *cache.QueryCache

	// OTLP ingest handlers & queues.
	OTLPHTTP     *otlp.Handler
	OTLPGRPC     *otlpgrpc.Handler
	SpansQueue   *ingest.Queue
	LogsQueue    *ingest.Queue
	MetricsQueue *ingest.Queue
	Tracker      *ingest.ByteTracker
}

func New(db *sql.DB, ch clickhouse.Conn, cfg config.Config) *App {
	getTenant := modulecommon.GetTenantFunc(middleware.GetTenant)

	sessionManager := sessionauth.NewManager(cfg)

	registry, err := configdefaults.Load()
	if err != nil {
		log.Fatalf("failed to load embedded default config registry: %v", err)
	}

	// OTLP ingest queues — batch-write to ClickHouse.
	queueOpts := []ingest.Option{
		ingest.WithBatchSize(cfg.QueueBatchSize),
		ingest.WithFlushInterval(int(cfg.QueueFlushIntervalMs)),
	}
	brokers := cfg.KafkaBrokerList()
	_ = brokers // Kafka path removed; kept to avoid config breakage
	spansQueue := ingest.NewQueue(ch, "observability.spans", otlp.SpanColumns, queueOpts...)
	logsQueue := ingest.NewQueue(ch, "observability.logs", otlp.LogColumns, queueOpts...)
	metricsQueue := ingest.NewQueue(ch, "observability.metrics", otlp.MetricColumns, queueOpts...)

	// Query result cache — only active when Redis is enabled.
	var redisClient *redis.Client
	var queryCache *cache.QueryCache
	if cfg.RedisEnabled {
		redisClient = redis.NewClient(&redis.Options{
			Addr: fmt.Sprintf("%s:%s", cfg.RedisHost, cfg.RedisPort),
		})
		queryCache = cache.New(redisClient)
	} else {
		queryCache = cache.New(nil)
	}

	authResolver := otlpauth.NewAuthenticator(db)
	tracker := ingest.NewByteTracker(db, time.Hour)
	otlpHTTPHandler := otlp.NewHandler(authResolver, spansQueue, logsQueue, metricsQueue, tracker)
	otlpGRPCHandler := otlpgrpc.NewHandler(authResolver, spansQueue, logsQueue, metricsQueue)

	nativeQuerier := dbutil.NewNativeQuerier(ch)

	return &App{
		DB:             db,
		CH:             ch,
		Redis:          redisClient,
		Config:         cfg,
		SessionManager: sessionManager,

		Auth: userauth.NewHandler(
			getTenant,
			userauth.NewService(
				userauth.NewRepository(db),
				sessionManager,
				cfg.GoogleClientID, cfg.GoogleClientSecret,
				cfg.GitHubClientID, cfg.GitHubClientSecret,
				cfg.OAuthRedirectBase,
			),
		),
		User: userpage.NewHandler(
			getTenant,
			userpage.NewService(userpage.NewRepository(db)),
		),
		Team: userteam.NewHandler(
			getTenant,
			userteam.NewService(userteam.NewRepository(db), registry),
		),
		LogSearch: &logsearch.Handler{
			DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
			Service:  logsearch.NewService(logsearch.NewRepository(nativeQuerier)),
		},
		LogAnalytics: &loganalytics.Handler{
			DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
			Service:  loganalytics.NewService(loganalytics.NewRepository(nativeQuerier)),
		},
		LogDetail: &logdetail.Handler{
			DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
			Service:  logdetail.NewService(logdetail.NewRepository(nativeQuerier)),
		},
		LogTraceLogs: &logtracelogs.Handler{
			DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
			Service:  logtracelogs.NewService(logtracelogs.NewRepository(nativeQuerier)),
		},
		Traces: tracesapi.NewHandler(
			getTenant,
			tracesapi.NewService(tracesapi.NewRepository(nativeQuerier)),
		),
		SpanAnalytics: spananalytics.NewHandler(
			getTenant,
			spananalytics.NewService(
				spananalytics.NewRepository(nativeQuerier),
			),
		),
		TraceCompare: tracecompare.NewHandler(
			getTenant,
			tracecompare.NewService(
				tracecompare.NewRepository(nativeQuerier),
			),
		),
		LiveTail: livetail.NewHandler(
			getTenant,
			livetail.NewService(livetail.NewRepository(nativeQuerier)),
		),
		ErrorFingerprint: errorfingerprint.NewHandler(
			getTenant,
			errorfingerprint.NewService(errorfingerprint.NewRepository(nativeQuerier)),
		),
		TraceDetail: &tracedetail.TraceDetailHandler{
			DBTenant: modulecommon.DBTenant{
				GetTenant: getTenant,
			},
			Service: tracedetail.NewService(
				tracedetail.NewRepository(nativeQuerier),
			),
		},
		ServiceMap: &servicemap.ServiceMapHandler{
			DBTenant: modulecommon.DBTenant{
				GetTenant: getTenant,
			},
			Service: servicemap.NewService(
				servicemap.NewRepository(nativeQuerier),
			),
		},
		REDMetrics: &redmetrics.REDMetricsHandler{
			DBTenant: modulecommon.DBTenant{
				GetTenant: getTenant,
			},
			Service: redmetrics.NewService(
				redmetrics.NewRepository(nativeQuerier),
			),
		},
		ErrorTracking: &errortracking.ErrorTrackingHandler{
			DBTenant: modulecommon.DBTenant{
				GetTenant: getTenant,
			},
			Service: errortracking.NewService(errortracking.NewRepository(nativeQuerier)),
		},
		Overview: &overviewmodule.OverviewHandler{
			DBTenant: modulecommon.DBTenant{
				GetTenant: getTenant,
			},
			Service: overviewmodule.NewService(overviewmodule.NewRepository(nativeQuerier)),
		},
		OverviewSLO: &overviewslo.SLOHandler{
			DBTenant: modulecommon.DBTenant{
				GetTenant: getTenant,
			},
			Service: overviewslo.NewService(
				overviewslo.NewRepository(nativeQuerier),
			),
		},
		OverviewErrors: &overviewerrors.ErrorHandler{
			DBTenant: modulecommon.DBTenant{
				GetTenant: getTenant,
			},
			Service: overviewerrors.NewService(overviewerrors.NewRepository(nativeQuerier)),
		},
		ServicesPage: &servicepage.ServiceHandler{
			DBTenant: modulecommon.DBTenant{
				GetTenant: getTenant,
			},
			Service: servicepage.NewService(servicepage.NewRepository(nativeQuerier)),
		},
		ServicesTopology: &servicetopology.TopologyHandler{
			DBTenant: modulecommon.DBTenant{
				GetTenant: getTenant,
			},
			Service: servicetopology.NewService(
				servicetopology.NewRepository(nativeQuerier),
			),
		},
		Nodes: &nodes.NodeHandler{
			DBTenant: modulecommon.DBTenant{
				GetTenant: getTenant,
			},
			Service: nodes.NewService(
				nodes.NewRepository(nativeQuerier),
			),
		},
		ResourceUtilisation: &resource_utilisation.ResourceUtilisationHandler{
			DBTenant: modulecommon.DBTenant{
				GetTenant: getTenant,
			},
			Service: resource_utilisation.NewService(
				resource_utilisation.NewRepository(nativeQuerier),
			),
		},
		CPU:     infraCPU.NewHandler(nativeQuerier, getTenant),
		Memory:  infraMemory.NewHandler(nativeQuerier, getTenant),
		Disk:    infraDisk.NewHandler(nativeQuerier, getTenant),
		Network: infraNetwork.NewHandler(nativeQuerier, getTenant),
		JVM:     infraJVM.NewHandler(nativeQuerier, getTenant),

		SaturationKafka: &kafka.KafkaHandler{
			DBTenant: modulecommon.DBTenant{
				GetTenant: getTenant,
			},
			Service: kafka.NewService(
				kafka.NewRepository(nativeQuerier),
			),
		},
		SaturationRedis: &redisaturation.RedisHandler{
			DBTenant: modulecommon.DBTenant{
				GetTenant: getTenant,
			},
			Service: redisaturation.NewService(redisaturation.NewRepository(nativeQuerier)),
		},
		Kubernetes: &kubernetes.KubernetesHandler{
			DBTenant: modulecommon.DBTenant{
				GetTenant: getTenant,
			},
			Service: kubernetes.NewService(kubernetes.NewRepository(nativeQuerier)),
		},
		HTTPMetrics: &httpmetrics.HTTPMetricsHandler{
			DBTenant: modulecommon.DBTenant{
				GetTenant: getTenant,
			},
			Service: httpmetrics.NewService(
				httpmetrics.NewRepository(nativeQuerier),
			),
		},
		APM: &apm.APMHandler{
			DBTenant: modulecommon.DBTenant{
				GetTenant: getTenant,
			},
			Service: apm.NewService(apm.NewRepository(nativeQuerier)),
		},
		AIDashboard: &aidashboard.Handler{
			DBTenant: modulecommon.DBTenant{
				GetTenant: getTenant,
			},
			Service: aidashboard.NewService(aidashboard.NewRepository(nativeQuerier)),
		},
		AIRuns: &airuns.Handler{
			DBTenant: modulecommon.DBTenant{
				GetTenant: getTenant,
			},
			Service: airuns.NewService(airuns.NewRepository(nativeQuerier)),
		},
		AIRunDetail: &airundetail.Handler{
			DBTenant: modulecommon.DBTenant{
				GetTenant: getTenant,
			},
			Service: airundetail.NewService(
				airundetail.NewRepository(nativeQuerier),
			),
		},
		AITraces: &aitraces.Handler{
			DBTenant: modulecommon.DBTenant{
				GetTenant: getTenant,
			},
			Service: aitraces.NewService(
				aitraces.NewRepository(nativeQuerier),
			),
		},
		AIConversations: &aiconversations.Handler{
			DBTenant: modulecommon.DBTenant{
				GetTenant: getTenant,
			},
			Service: aiconversations.NewService(aiconversations.NewRepository(nativeQuerier)),
		},
		DefaultConfig: &defaultconfig.Handler{
			DBTenant: modulecommon.DBTenant{
				DB:        db,
				GetTenant: getTenant,
			},
			Service: defaultconfig.NewService(defaultconfig.NewRepository(db), registry),
		},
		DatabaseSummary: &dbsummary.Handler{
			DBTenant: modulecommon.DBTenant{
				GetTenant: getTenant,
			},
			Service: dbsummary.NewService(dbsummary.NewRepository(nativeQuerier)),
		},
		DatabaseSystems: &dbsystems.Handler{
			DBTenant: modulecommon.DBTenant{
				GetTenant: getTenant,
			},
			Service: dbsystems.NewService(dbsystems.NewRepository(nativeQuerier)),
		},
		DatabaseLatency: &dblatency.Handler{
			DBTenant: modulecommon.DBTenant{
				GetTenant: getTenant,
			},
			Service: dblatency.NewService(dblatency.NewRepository(nativeQuerier)),
		},
		DatabaseVolume: &dbvolume.Handler{
			DBTenant: modulecommon.DBTenant{
				GetTenant: getTenant,
			},
			Service: dbvolume.NewService(dbvolume.NewRepository(nativeQuerier)),
		},
		DatabaseSlow: &dbslowqueries.Handler{
			DBTenant: modulecommon.DBTenant{
				GetTenant: getTenant,
			},
			Service: dbslowqueries.NewService(dbslowqueries.NewRepository(nativeQuerier)),
		},
		DatabaseErrors: &dberrors.Handler{
			DBTenant: modulecommon.DBTenant{
				GetTenant: getTenant,
			},
			Service: dberrors.NewService(dberrors.NewRepository(nativeQuerier)),
		},
		DatabaseConn: &dbconnections.Handler{
			DBTenant: modulecommon.DBTenant{
				GetTenant: getTenant,
			},
			Service: dbconnections.NewService(dbconnections.NewRepository(nativeQuerier)),
		},
		DatabaseCollect: &dbcollection.Handler{
			DBTenant: modulecommon.DBTenant{
				GetTenant: getTenant,
			},
			Service: dbcollection.NewService(dbcollection.NewRepository(nativeQuerier)),
		},
		DatabaseSystem: &dbsystem.Handler{
			DBTenant: modulecommon.DBTenant{
				GetTenant: getTenant,
			},
			Service: dbsystem.NewService(dbsystem.NewRepository(nativeQuerier)),
		},

		Cache: queryCache,

		OTLPHTTP:     otlpHTTPHandler,
		OTLPGRPC:     otlpGRPCHandler,
		SpansQueue:   spansQueue,
		LogsQueue:    logsQueue,
		MetricsQueue: metricsQueue,
		Tracker:      tracker,
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
	v1.Use(middleware.TenantMiddleware(a.SessionManager))

	// Rate limiting: 1000 requests per second with burst of 2000.
	if a.Redis != nil {
		rl := middleware.NewRedisRateLimiter(a.Redis, 2000, time.Second)
		v1.Use(middleware.RedisRateLimitMiddleware(rl))
	} else {
		rl := middleware.NewRateLimiter(1000, 2000, time.Second)
		v1.Use(middleware.RateLimitMiddleware(rl))
	}

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
	if a.Redis != nil {
		rl := middleware.NewRedisRateLimiter(a.Redis, 2000, time.Second)
		r.Use(middleware.RedisRateLimitMiddleware(rl))
	} else {
		rl := middleware.NewRateLimiter(1000, 2000, time.Second)
		r.Use(middleware.RateLimitMiddleware(rl))
	}

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

	userauth.RegisterRoutes(cfg.UserAuth, v1, a.Auth)
	userpage.RegisterRoutes(cfg.UserPage, v1, a.User)
	userteam.RegisterRoutes(cfg.UserTeam, v1, a.Team)
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
	logsearch.RegisterRoutes(cfg.LogSearch, v1, a.LogSearch)
	loganalytics.RegisterRoutes(cfg.LogAnalytics, v1, a.LogAnalytics)
	logdetail.RegisterRoutes(cfg.LogDetail, v1, a.LogDetail)
	logtracelogs.RegisterRoutes(cfg.LogTraceLogs, v1, a.LogTraceLogs)
	tracesapi.RegisterRoutes(cfg.Traces, v1, a.Traces)
	spananalytics.RegisterRoutes(cfg.SpanAnalytics, v1, a.SpanAnalytics)
	tracecompare.RegisterRoutes(cfg.TraceCompare, v1, a.TraceCompare)
	livetail.RegisterRoutes(cfg.LiveTail, v1, a.LiveTail)
	errorfingerprint.RegisterRoutes(cfg.ErrorFingerprint, v1, a.ErrorFingerprint)
	tracedetail.RegisterRoutes(cfg.TraceDetail, v1, a.TraceDetail)
	servicemap.RegisterRoutes(cfg.ServiceMap, cached, a.ServiceMap)
	redmetrics.RegisterRoutes(cfg.REDMetrics, cached, a.REDMetrics)
	errortracking.RegisterRoutes(cfg.ErrorTracking, cached, a.ErrorTracking)
	aidashboard.RegisterRoutes(cfg.AIDashboard, v1, a.AIDashboard)
	airuns.RegisterRoutes(cfg.AIRuns, v1, a.AIRuns)
	airundetail.RegisterRoutes(cfg.AIRunDetail, v1, a.AIRunDetail)
	aitraces.RegisterRoutes(cfg.AITraces, v1, a.AITraces)
	aiconversations.RegisterRoutes(cfg.AIConversations, v1, a.AIConversations)
	defaultconfig.RegisterRoutes(cfg.DefaultConfig, v1, a.DefaultConfig)
	dbsummary.RegisterRoutes(cfg.DatabaseSummary, cached, a.DatabaseSummary)
	dbsystems.RegisterRoutes(cfg.DatabaseSystems, cached, a.DatabaseSystems)
	dblatency.RegisterRoutes(cfg.DatabaseLatency, cached, a.DatabaseLatency)
	dbvolume.RegisterRoutes(cfg.DatabaseVolume, cached, a.DatabaseVolume)
	dbslowqueries.RegisterRoutes(cfg.DatabaseSlow, cached, a.DatabaseSlow)
	dberrors.RegisterRoutes(cfg.DatabaseErrors, cached, a.DatabaseErrors)
	dbconnections.RegisterRoutes(cfg.DatabaseConn, cached, a.DatabaseConn)
	dbcollection.RegisterRoutes(cfg.DatabaseCollect, cached, a.DatabaseCollect)
	dbsystem.RegisterRoutes(cfg.DatabaseSystem, cached, a.DatabaseSystem)
}

func (a *App) healthLive(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (a *App) healthReady(c *gin.Context) {
	if err := a.DB.Ping(); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not_ready", "mysql": err.Error()})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()
	if err := a.CH.Ping(ctx); err != nil {
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

	// 1. Setup Main API Server (9090)
	mainRouter := a.Router()
	mainSrv := &http.Server{
		Addr:         fmt.Sprintf(":%s", a.Config.Port),
		Handler:      h2c.NewHandler(a.SessionManager.LoadAndSave(mainRouter), &http2.Server{}),
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
