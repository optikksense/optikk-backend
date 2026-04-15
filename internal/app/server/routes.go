package server

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/Optikk-Org/optikk-backend/internal/infra/middleware"
	"github.com/gin-gonic/gin"
)

func (a *App) Router() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()

	a.setupGlobalMiddleware(r)
	a.setupHealthRoutes(r)
	a.setupAPIRoutes(r)

	return r
}

func (a *App) setupGlobalMiddleware(r *gin.Engine) {
	r.Use(middleware.ErrorRecovery())
	r.Use(middleware.CORSMiddleware(a.Config.Server.AllowedOrigins))
	r.Use(middleware.BodyLimitMiddleware(10 * 1024 * 1024)) // 10 MB
}

func (a *App) setupHealthRoutes(r *gin.Engine) {
	r.GET("/health", a.healthLive)
	r.GET("/health/live", a.healthLive)
	r.GET("/health/ready", a.healthReady)
}

func (a *App) setupAPIRoutes(r *gin.Engine) {
	v1 := r.Group("/api/v1")
	v1.Use(middleware.TenantMiddleware(a.Infra.SessionManager))

	cachedV1 := r.Group("/api/v1")
	cachedV1.Use(middleware.TenantMiddleware(a.Infra.SessionManager))
	cachedV1.Use(middleware.CacheMiddleware(
		middleware.NewRedisResponseCache(a.Infra.RedisClient),
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
}

func (a *App) healthLive(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (a *App) healthReady(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	if err := a.Infra.DB.Ping(); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not_ready", "mysql": err.Error()})
		return
	}

	if err := a.Infra.CH.Ping(ctx); err != nil {
		a.logHealthError(c, "clickhouse", err)
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not_ready", "clickhouse": err.Error()})
		return
	}

	if a.Infra.RedisClient == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not_ready", "redis": "client not configured"})
		return
	}

	if err := a.Infra.RedisClient.Ping(ctx).Err(); err != nil {
		a.logHealthError(c, "redis", err)
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not_ready", "redis": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "ready", "mysql": "ok", "clickhouse": "ok", "redis": "ok"})
}

func (a *App) logHealthError(c *gin.Context, service string, err error) {
	slog.Error("health check failed",
		slog.String("service", service),
		slog.String("error", err.Error()),
		slog.String("method", c.Request.Method),
		slog.String("path", c.Request.URL.Path))
}

