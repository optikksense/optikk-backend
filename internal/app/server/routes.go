package server

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/middleware"
	"github.com/gin-gonic/gin"
)

func (a *App) Router() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(middleware.ErrorRecovery())
	r.Use(middleware.CORSMiddleware(a.Config.Server.AllowedOrigins))

	r.GET("/health", a.healthLive)
	r.GET("/health/live", a.healthLive)
	r.GET("/health/ready", a.healthReady)

	v1 := r.Group("/api/v1")
	v1.Use(middleware.TenantMiddleware(a.SessionManager))
	a.applyRateLimiter(v1)
	v1.GET("/ws/live", a.LiveTailWS)

	for _, mod := range a.Modules {
		mod.RegisterRoutes(v1)
	}

	return r
}

func (a *App) applyRateLimiter(group gin.IRoutes) {
	rl := middleware.NewRateLimiter(2000, 2000, time.Second)
	group.Use(middleware.RateLimitMiddleware(rl))
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
	c.JSON(http.StatusOK, gin.H{"status": "ready", "mysql": "ok", "clickhouse": "ok"})
}
