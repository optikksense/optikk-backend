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
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/livetailws"
	"github.com/Optikk-Org/optikk-backend/internal/infra/middleware"
	"github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	log_search "github.com/Optikk-Org/optikk-backend/internal/modules/logs/search"
	platformruntime "github.com/Optikk-Org/optikk-backend/internal/platform/runtime"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
	"github.com/oklog/run"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/keepalive"
)

type App struct {
	DB      *sql.DB
	CH      clickhouse.Conn
	Config  config.Config
	Runtime *platformruntime.Runtime
	Modules []registry.Module

	// LiveTailWS serves GET /api/v1/ws/live (native WebSocket live tail).
	LiveTailWS gin.HandlerFunc
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

	// Initialize global infrastructure parameters.
	utils.Init(cfg.SpansBucketSeconds(), cfg.LogsBucketSeconds())

	runtimeDeps, err := platformruntime.New(db, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize runtime dependencies: %w", err)
	}

	nativeQuerier := dbutil.NewNativeQuerier(ch)

	logSearchSvc := log_search.NewService(log_search.NewRepository(nativeQuerier))
	modules := configuredModules(nativeQuerier, db, ch, getTenant, cfg, runtimeDeps, logSearchSvc)

	liveTailH := livetailws.NewHandler(livetailws.Config{
		Hub:            runtimeDeps.LiveTailHub,
		AllowedOrigins: splitAllowedOrigins(cfg.Server.AllowedOrigins),
		Sessions:       runtimeDeps.SessionManager,
	})

	return &App{
		DB:         db,
		CH:         ch,
		Config:     cfg,
		Runtime:    runtimeDeps,
		Modules:    modules,
		LiveTailWS: liveTailH,
	}, nil
}

func (a *App) Start(ctx context.Context) error {
	for _, mod := range a.Modules {
		if r, ok := mod.(registry.BackgroundRunner); ok {
			r.Start()
		}
	}

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
			Handler:      h2c.NewHandler(a.Runtime.SessionManager.Wrap(a.Router()), &http2.Server{}),
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 60 * time.Second,
			IdleTimeout:  120 * time.Second,
		}
		g.Add(func() error {
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
			return grpcSrv.Serve(lis)
		}, func(error) {
			grpcSrv.GracefulStop()
		})
	}

	err := g.Run()

	for _, mod := range a.Modules {
		if r, ok := mod.(registry.BackgroundRunner); ok {
			if stopErr := r.Stop(); stopErr != nil {
				slog.Warn("error stopping module", slog.String("module", mod.Name()), slog.Any("error", stopErr))
			}
		}
	}
	if closeErr := a.Runtime.Close(); closeErr != nil {
		slog.Warn("error closing runtime", slog.Any("error", closeErr))
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
