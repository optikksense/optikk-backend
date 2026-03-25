package server

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/observability/observability-backend-go/internal/modules/registry"
	"github.com/observability/observability-backend-go/internal/platform/cache"
	"github.com/observability/observability-backend-go/internal/platform/logger"
	"github.com/observability/observability-backend-go/internal/platform/middleware"
)

func (a *App) Router() *gin.Engine {
	r := gin.New()
	r.Use(middleware.RequestIDMiddleware())
	r.Use(middleware.APIDebugLogger(a.Config.Server.DebugAPILogs))
	r.Use(logger.GinMiddleware())
	r.Use(middleware.ErrorRecovery())
	r.Use(middleware.CORSMiddleware(a.Config.Server.AllowedOrigins))

	r.GET("/health", a.healthLive)
	r.GET("/health/live", a.healthLive)
	r.GET("/health/ready", a.healthReady)

	v1 := r.Group("/api/v1")
	v1.Use(middleware.TenantMiddleware(a.SessionManager))
	a.applyRateLimiter(v1)
	a.registerRoutesToGroup(v1)

	// Mount Socket.IO endpoints for real-time streaming.
	r.GET("/socket.io/*any", a.SocketIO.GinMiddleware())
	r.POST("/socket.io/*any", a.SocketIO.GinMiddleware())

	return r
}

func (a *App) registerRoutesToGroup(v1 *gin.RouterGroup) {
	cached := v1.Group("", cache.CacheResponse(a.Cache, 30*time.Second))

	for _, mod := range a.Modules {
		switch mod.RouteTarget() {
		case registry.Cached:
			mod.RegisterRoutes(cached)
		default:
			mod.RegisterRoutes(v1)
		}
	}
}

func (a *App) applyRateLimiter(group gin.IRoutes) {
	if a.Redis != nil {
		rl := middleware.NewRedisRateLimiter(a.Redis, 2000, time.Second)
		group.Use(middleware.RedisRateLimitMiddleware(rl))
	} else {
		rl := middleware.NewRateLimiter(1000, 2000, time.Second)
		group.Use(middleware.RateLimitMiddleware(rl))
	}
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
