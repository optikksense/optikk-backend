package server

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/Optikk-Org/optikk-backend/internal/infra/middleware"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	_ "github.com/Optikk-Org/optikk-backend/internal/infra/metrics" // register Prometheus collectors
)

func (a *App) Router() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()

	// Expose Prometheus metrics before the middleware stack so /metrics
	// is not behind auth or body-limit middleware.
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	r.Use(middleware.ErrorRecovery())
	r.Use(middleware.CORSMiddleware(a.Config.Server.AllowedOrigins))
	r.Use(middleware.PrometheusMiddleware())
	r.Use(middleware.BodyLimitMiddleware(10 * 1024 * 1024)) // 10 MB

	r.GET("/health", a.healthLive)
	r.GET("/health/live", a.healthLive)
	r.GET("/health/ready", a.healthReady)

	v1 := r.Group("/api/v1")
	v1.Use(middleware.TenantMiddleware(a.Runtime.SessionManager))
	v1.GET("/ws/live", a.LiveTailWS)

	cachedV1 := r.Group("/api/v1")
	cachedV1.Use(middleware.TenantMiddleware(a.Runtime.SessionManager))
	cachedV1.Use(middleware.CacheMiddleware(
		middleware.NewRedisResponseCache(a.Runtime.RedisClient),
		middleware.DefaultResponseCacheTTL,
	))

	for _, mod := range a.Modules {
		switch mod.RouteTarget() {
		case registry.Cached:
			mod.RegisterRoutes(cachedV1)
		default:
			mod.RegisterRoutes(v1)
		}
	}

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
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()
	if err := a.CH.Ping(ctx); err != nil {
		slog.Error("request error",
			slog.String("code", "503"), slog.String("msg", err.Error()),
			slog.String("method", c.Request.Method), slog.String("path", c.Request.URL.Path))
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not_ready", "clickhouse": err.Error()})
		return
	}
	if a.Runtime.RedisClient == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not_ready", "redis": "client not configured"})
		return
	}
	if err := a.Runtime.RedisClient.Ping(ctx).Err(); err != nil {
		slog.Error("request error",
			slog.String("code", "503"), slog.String("msg", err.Error()),
			slog.String("method", c.Request.Method), slog.String("path", c.Request.URL.Path))
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not_ready", "redis": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ready", "mysql": "ok", "clickhouse": "ok", "redis": "ok"})
}
