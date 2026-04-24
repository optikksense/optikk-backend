package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/Optikk-Org/optikk-backend/internal/auth"
	"github.com/Optikk-Org/optikk-backend/internal/config"
	"github.com/Optikk-Org/optikk-backend/internal/infra/grpcutil"
	"github.com/Optikk-Org/optikk-backend/internal/infra/middleware"
	appotel "github.com/Optikk-Org/optikk-backend/internal/infra/otel"
	"github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/oklog/run"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/keepalive"
)

type App struct {
	Config		config.Config
	Infra		*Infra
	Modules		[]registry.Module
	otelShutdown	func(context.Context) error
}

func New(cfg config.Config) (*App, error) {
	getTenant := modulecommon.GetTenantFunc(middleware.GetTenant)

	// Initialize global infrastructure parameters.
	utils.Init(cfg.SpansBucketSeconds(), cfg.LogsBucketSeconds())

	// Bootstrap OpenTelemetry early so gin middleware + ClickHouse spans
	// see a real TracerProvider. On error we log + continue — a broken
	// monitoring pipeline must not block the product from starting.
	otelShutdown, err := appotel.Init(context.Background(), cfg.Telemetry.OTel, cfg.Environment)
	if err != nil {
		slog.Warn("otel init failed, continuing with no-op provider", slog.Any("error", err))
		otelShutdown = func(context.Context) error { return nil }
	}

	infraDeps, err := newInfra(cfg)
	if err != nil {
		_ = otelShutdown(context.Background())
		return nil, fmt.Errorf("failed to initialize infrastructure: %w", err)
	}

	modules := configuredModules(infraDeps.CH, getTenant, cfg, infraDeps)

	return &App{
		Config:		cfg,
		Infra:		infraDeps,
		Modules:	modules,
		otelShutdown:	otelShutdown,
	}, nil
}

func (a *App) Start(ctx context.Context) error {
	a.startBackgroundModules()

	var g run.Group
	runAddContextCancelActor(&g, ctx)
	a.addHTTPServerActor(&g)
	if err := a.addGRPCServerActor(&g); err != nil {
		a.stopBackgroundModules()
		return err
	}

	err := g.Run()
	a.stopBackgroundModules()
	if closeErr := a.Infra.Close(); closeErr != nil {
		slog.WarnContext(ctx, "error closing infrastructure", slog.Any("error", closeErr))
	}
	if a.otelShutdown != nil {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if shutErr := a.otelShutdown(shutCtx); shutErr != nil {
			slog.WarnContext(ctx, "error shutting down otel", slog.Any("error", shutErr))
		}
		cancel()
	}

	return normalizeRunError(err)
}

func (a *App) startBackgroundModules() {
	for _, mod := range a.Modules {
		if r, ok := mod.(registry.BackgroundRunner); ok {
			r.Start()
		}
	}
}

func (a *App) stopBackgroundModules() {
	for _, mod := range a.Modules {
		if r, ok := mod.(registry.BackgroundRunner); ok {
			if stopErr := r.Stop(); stopErr != nil {
				slog.Warn("error stopping module", slog.String("module", mod.Name()), slog.Any("error", stopErr))
			}
		}
	}
}

// runAddContextCancelActor shuts down the run group when ctx is cancelled.
func runAddContextCancelActor(g *run.Group, ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	g.Add(func() error { <-ctx.Done(); return ctx.Err() },
		func(error) { cancel() })
}

func (a *App) addHTTPServerActor(g *run.Group) {
	srv := &http.Server{
		Addr:		fmt.Sprintf(":%s", a.Config.Server.Port),
		Handler:	h2c.NewHandler(a.Infra.SessionManager.Wrap(a.Router()), &http2.Server{}),
		ReadTimeout:	30 * time.Second,
		WriteTimeout:	60 * time.Second,
		IdleTimeout:	120 * time.Second,
	}
	g.Add(func() error {
		return srv.ListenAndServe()
	}, func(error) {
		shutCtx, c := context.WithTimeout(context.Background(), 10*time.Second)
		defer c()
		srv.Shutdown(shutCtx)
	})
}

func (a *App) addGRPCServerActor(g *run.Group) error {
	port := a.Config.OTLP.GRPCPort
	if port == "" {
		return fmt.Errorf("server: gRPC port is not configured (otlp.grpc_port)")
	}

	addr := fmt.Sprintf(":%s", port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("server: gRPC listen failed on %s: %w", addr, err)
	}

	slog.Info("starting OTLP gRPC server",
		slog.String("addr", addr),
		slog.String("hint", "send gRPC metadata x-api-key (team API key); use OTLP gRPC on this port, not HTTP/protobuf"))

	grpcSrv := grpc.NewServer(
		grpc.MaxConcurrentStreams(100),
		grpc.ConnectionTimeout(30*time.Second),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:		20 * time.Second,
			Timeout:	10 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:		10 * time.Second,
			PermitWithoutStream:	true,
		}),
		// otelgrpc's stats handler extracts server-side trace context
		// from incoming metadata and starts an OTel span per RPC.
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		// Observability interceptors run first so they time the auth
		// interceptor + handler together; auth unauthorised denials
		// still show up in the metrics with code=Unauthenticated.
		grpc.ChainUnaryInterceptor(
			grpcutil.UnaryObservability(),
			auth.UnaryInterceptor(a.Infra.Authenticator),
		),
		grpc.ChainStreamInterceptor(
			grpcutil.StreamObservability(),
			auth.StreamInterceptor(a.Infra.Authenticator),
		),
	)
	for _, mod := range a.Modules {
		if r, ok := mod.(registry.GRPCRegistrar); ok {
			r.RegisterGRPC(grpcSrv)
		}
	}
	g.Add(func() error {
		return grpcSrv.Serve(lis)
	}, func(error) {
		done := make(chan struct{})
		go func() {
			grpcSrv.GracefulStop()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			grpcSrv.Stop()
		}
	})
	return nil
}

func normalizeRunError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
