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
	"github.com/Optikk-Org/optikk-backend/internal/config"
	"github.com/oklog/run"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/keepalive"
)

type App struct {
	Config  config.Config
	Deps    *registry.Deps
	Modules []registry.Module
}

func New(cfg config.Config) (*App, error) {
	deps, err := newDeps(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize deps: %w", err)
	}

	modules, err := configuredModules(deps)
	if err != nil {
		_ = deps.Close()
		return nil, fmt.Errorf("failed to initialize modules: %w", err)
	}

	return &App{
		Config:  cfg,
		Deps:    deps,
		Modules: modules,
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
	if closeErr := a.Deps.Close(); closeErr != nil {
		slog.Warn("error closing deps", slog.Any("error", closeErr))
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
		Addr:         fmt.Sprintf(":%s", a.Config.Server.Port),
		Handler:      h2c.NewHandler(a.Deps.SessionManager.Wrap(a.Router()), &http2.Server{}),
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
	return nil
}

func normalizeRunError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
