package server

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/middleware"
	"github.com/gin-contrib/gzip"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var readyCache = newHealthCache()

func (a *App) Router() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()

	a.setupGlobalMiddleware(r)
	a.setupHealthRoutes(r)
	a.setupMetricsRoute(r)
	a.setupAPIRoutes(r)

	return r
}

func (a *App) setupMetricsRoute(r *gin.Engine) {
	r.GET("/metrics", gin.WrapH(promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{DisableCompression: true})))
}

func (a *App) setupGlobalMiddleware(r *gin.Engine) {
	r.Use(middleware.ErrorRecovery())
	r.Use(middleware.HTTPMetricsMiddleware())
	r.Use(middleware.CORSMiddleware(a.Config.Server.AllowedOrigins))
	r.Use(middleware.BodyLimitMiddleware(10 * 1024 * 1024))
	r.Use(gzip.Gzip(gzip.DefaultCompression, gzip.WithExcludedPaths([]string{"/metrics"})))
}

func (a *App) setupHealthRoutes(r *gin.Engine) {
	r.GET("/health", a.healthLive)
	r.GET("/health/live", a.healthLive)
	r.GET("/health/ready", a.healthReady)
}

func (a *App) setupAPIRoutes(r *gin.Engine) {
	v1 := r.Group("/api/v1")
	v1.Use(middleware.TenantMiddleware(a.Infra.SessionManager))
	for _, mod := range a.Modules {
		mod.RegisterRoutes(v1)
	}
}

func (a *App) healthLive(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (a *App) healthReady(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	res := readyCache.get(ctx, a.probeReady)

	if !res.ready {
		payload := gin.H{"status": "not_ready"}
		if res.mysqlErr != "" {
			payload["mysql"] = res.mysqlErr
		}
		if res.chErr != "" {
			payload["clickhouse"] = res.chErr
		}
		if res.redisErr != "" {
			payload["redis"] = res.redisErr
		}
		c.JSON(http.StatusServiceUnavailable, payload)
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "ready", "mysql": "ok", "clickhouse": "ok", "redis": "ok"})
}

func (a *App) probeReady(ctx context.Context) *healthResult {
	res := &healthResult{}
	if err := a.Infra.DB.Ping(); err != nil {
		res.mysqlErr = err.Error()
		return res
	}
	if err := a.Infra.CH.Ping(ctx); err != nil {
		slog.ErrorContext(ctx, "health check failed", slog.String("service", "clickhouse"), slog.String("error", err.Error()))
		res.chErr = err.Error()
		return res
	}
	if a.Infra.RedisClient == nil {
		res.redisErr = "client not configured"
		return res
	}
	if err := a.Infra.RedisClient.Ping(ctx).Err(); err != nil {
		slog.ErrorContext(ctx, "health check failed", slog.String("service", "redis"), slog.String("error", err.Error()))
		res.redisErr = err.Error()
		return res
	}
	res.ready = true
	return res
}
