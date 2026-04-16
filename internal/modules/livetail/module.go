package livetail

import (
	"strings"

	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/gin-gonic/gin"
)

// NewModule registers GET /api/v1/ws/live (WebSocket live tail).
func NewModule(deps *registry.Deps) (registry.Module, error) {
	cfg := Config{
		Hub:            deps.LiveTailHub,
		AllowedOrigins: splitAllowedOrigins(deps.AppConfig.Server.AllowedOrigins),
		Sessions:       deps.SessionManager,
	}
	return &liveTailModule{handler: NewHandler(cfg)}, nil
}

// splitAllowedOrigins splits a comma-separated string into trimmed tokens.
func splitAllowedOrigins(raw string) []string {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if s := strings.TrimSpace(p); s != "" {
			out = append(out, s)
		}
	}
	return out
}

type liveTailModule struct {
	handler gin.HandlerFunc
}

func (m *liveTailModule) Name() string                      { return "livetail" }
func (m *liveTailModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *liveTailModule) RegisterRoutes(group *gin.RouterGroup) {
	group.GET("/ws/live", m.handler)
}
