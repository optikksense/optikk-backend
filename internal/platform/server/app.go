package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/observability/observability-backend-go/internal/config"
	dbutil "github.com/observability/observability-backend-go/internal/database"
	configdefaults "github.com/observability/observability-backend-go/internal/defaultconfig"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
	"github.com/observability/observability-backend-go/internal/modules/registry"
	"github.com/observability/observability-backend-go/internal/platform/cache"
	"github.com/observability/observability-backend-go/internal/platform/middleware"
	sessionauth "github.com/observability/observability-backend-go/internal/platform/session"
	sio "github.com/observability/observability-backend-go/internal/platform/socketio"
	"github.com/oklog/run"
	"github.com/redis/go-redis/v9"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/keepalive"
)

type App struct {
	DB             *sql.DB
	CH             clickhouse.Conn
	Redis          *redis.Client
	Config         config.Config
	SessionManager *sessionauth.Manager

	// Query result cache (Redis-gated, nil-safe).
	Cache *cache.QueryCache

	// SocketIO is the Socket.IO server for real-time streaming.
	SocketIO *sio.Server
}

func New(db *sql.DB, ch clickhouse.Conn, cfg config.Config) *App {
	getTenant := modulecommon.GetTenantFunc(middleware.GetTenant)
	sessionManager := sessionauth.NewManager(cfg)

	reg, err := configdefaults.Load()
	if err != nil {
		log.Fatalf("failed to load embedded default config registry: %v", err)
	}

	var redisClient *redis.Client
	var queryCache *cache.QueryCache
	if cfg.Redis.Enabled {
		redisClient = redis.NewClient(&redis.Options{
			Addr: fmt.Sprintf("%s:%s", cfg.Redis.Host, cfg.Redis.Port),
		})
		queryCache = cache.New(redisClient)
	} else {
		queryCache = cache.New(nil)
	}

	nativeQuerier := dbutil.NewNativeQuerier(ch)

	deps := registry.Deps{
		NativeQuerier:  nativeQuerier,
		DB:             db,
		ClickHouseConn: ch,
		GetTenant:      getTenant,
		SessionManager: sessionManager,
		Config:         cfg,
		ConfigRegistry: reg,
	}
	for _, mod := range registry.All() {
		if err := mod.Init(deps); err != nil {
			log.Fatalf("failed to init module %s: %v", mod.Name(), err)
		}
	}

	// Create Socket.IO server and register handlers from modules.
	socketIOServer, err := sio.NewServer(strings.Split(cfg.Server.AllowedOrigins, ","))
	if err != nil {
		log.Fatalf("failed to create Socket.IO server: %v", err)
	}
	for _, mod := range registry.All() {
		if r, ok := mod.(registry.SocketIORegistrar); ok {
			r.RegisterSocketIO(socketIOServer)
		}
	}

	return &App{
		DB:             db,
		CH:             ch,
		Redis:          redisClient,
		Config:         cfg,
		SessionManager: sessionManager,
		Cache:          queryCache,
		SocketIO:       socketIOServer,
	}
}

func (a *App) Start(ctx context.Context) error {
	for _, mod := range registry.All() {
		if r, ok := mod.(registry.BackgroundRunner); ok {
			r.Start()
		}
	}

	// Start the Socket.IO event loop.
	a.SocketIO.Serve()

	var g run.Group

	// Signal actor — converts context cancellation into group shutdown.
	{
		ctx, cancel := context.WithCancel(ctx)
		g.Add(func() error { <-ctx.Done(); return ctx.Err() },
			func(error) { cancel() })
	}

	// Main API server
	{
		srv := &http.Server{
			Addr:         fmt.Sprintf(":%s", a.Config.Server.Port),
			Handler:      h2c.NewHandler(a.SessionManager.LoadAndSave(a.Router()), &http2.Server{}),
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 60 * time.Second,
			IdleTimeout:  120 * time.Second,
		}
		g.Add(func() error {
			log.Printf("Main API server starting on %s", srv.Addr)
			return srv.ListenAndServe()
		}, func(error) {
			shutCtx, c := context.WithTimeout(context.Background(), 10*time.Second)
			defer c()
			srv.Shutdown(shutCtx)
		})
	}

	// gRPC server
	{
		lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", a.Config.OTLP.GRPCPort))
		if err != nil {
			return fmt.Errorf("gRPC listen failed: %v", err)
		}
		grpcSrv := grpc.NewServer(
			grpc.MaxRecvMsgSize(a.Config.OTLP.GRPCMaxRecvMsgSizeMB*1024*1024),
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
		for _, mod := range registry.All() {
			if r, ok := mod.(registry.GRPCRegistrar); ok {
				r.RegisterGRPC(grpcSrv)
			}
		}
		g.Add(func() error {
			log.Printf("gRPC server starting on %s", a.Config.OTLP.GRPCPort)
			return grpcSrv.Serve(lis)
		}, func(error) {
			grpcSrv.GracefulStop()
		})
	}

	err := g.Run()

	// Shut down Socket.IO server.
	if closeErr := a.SocketIO.Close(); closeErr != nil {
		log.Printf("WARN: error closing Socket.IO server: %v", closeErr)
	}

	for _, mod := range registry.All() {
		if r, ok := mod.(registry.BackgroundRunner); ok {
			if stopErr := r.Stop(); stopErr != nil {
				log.Printf("WARN: error stopping module %s: %v", mod.Name(), stopErr)
			}
		}
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
