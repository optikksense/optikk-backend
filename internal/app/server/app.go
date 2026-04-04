package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/Optikk-Org/optikk-backend/internal/config"
	"github.com/Optikk-Org/optikk-backend/internal/infra/cache"
	configdefaults "github.com/Optikk-Org/optikk-backend/internal/infra/dashboardcfg/defaults"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/middleware"
	sessionauth "github.com/Optikk-Org/optikk-backend/internal/infra/session"
	sio "github.com/Optikk-Org/optikk-backend/internal/infra/socketio"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
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
	Modules        []registry.Module

	// Query result cache (Redis-gated, nil-safe).
	Cache *cache.QueryCache

	// SocketIO is the Socket.IO server for real-time streaming.
	SocketIO *sio.Server
}

func splitAllowedOrigins(allowed string) []string {
	var out []string
	for _, o := range strings.Split(allowed, ",") {
		o = strings.TrimSpace(o)
		if o != "" {
			out = append(out, o)
		}
	}
	return out
}

func New(db *sql.DB, ch clickhouse.Conn, cfg config.Config) (*App, error) {
	getTenant := modulecommon.GetTenantFunc(middleware.GetTenant)
	sessionManager := sessionauth.NewManager(cfg)

	reg, err := configdefaults.Load()
	if err != nil {
		return nil, fmt.Errorf("failed to load embedded default config registry: %w", err)
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

	nativeQuerier := dbutil.NewNativeQuerier(ch, cfg.CircuitBreakerConsecutiveFailures(), cfg.CircuitBreakerResetTimeout())

	modules := configuredModules(nativeQuerier, db, ch, getTenant, sessionManager, cfg, reg, queryCache)

	// Create Socket.IO server and register handlers from modules.
	// Trim each origin so "a, b" does not leave a leading space that breaks WebSocket CheckOrigin.
	socketIOServer, err := sio.NewServer(splitAllowedOrigins(cfg.Server.AllowedOrigins))
	if err != nil {
		return nil, fmt.Errorf("failed to create Socket.IO server: %w", err)
	}
	for _, mod := range modules {
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
		Modules:        modules,
		Cache:          queryCache,
		SocketIO:       socketIOServer,
	}, nil
}

func (a *App) Start(ctx context.Context) error {
	for _, mod := range a.Modules {
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
			slog.Info("main API server starting", slog.String("addr", srv.Addr))
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
		for _, mod := range a.Modules {
			if r, ok := mod.(registry.GRPCRegistrar); ok {
				r.RegisterGRPC(grpcSrv)
			}
		}
		g.Add(func() error {
			slog.Info("gRPC server starting", slog.String("port", a.Config.OTLP.GRPCPort))
			return grpcSrv.Serve(lis)
		}, func(error) {
			grpcSrv.GracefulStop()
		})
	}

	err := g.Run()

	if closeErr := a.SocketIO.Close(); closeErr != nil {
		slog.Warn("error closing Socket.IO server", slog.Any("error", closeErr))
	}

	for _, mod := range a.Modules {
		if r, ok := mod.(registry.BackgroundRunner); ok {
			if stopErr := r.Stop(); stopErr != nil {
				slog.Warn("error stopping module", slog.String("module", mod.Name()), slog.Any("error", stopErr))
			}
		}
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
