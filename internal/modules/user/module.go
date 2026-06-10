package user

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/Optikk-Org/optikk-backend/internal/infra/token"
	"github.com/gin-gonic/gin"
)

type Config struct {
	Enabled bool
}

func DefaultConfig() Config {
	return Config{Enabled: true}
}

// RegisterRoutes sets up routing for both auth and user profile/settings.
func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *Handler) {
	if !cfg.Enabled || h == nil {
		return
	}

	authGroup := v1.Group("/auth")
	{
		authGroup.POST("/login", h.Login)
		authGroup.POST("/refresh", h.Refresh)
		authGroup.POST("/logout", h.Logout)
		authGroup.GET("/me", h.AuthMe)
	}

	settingsGroup := v1.Group("/settings")
	{
		settingsGroup.GET("/profile", h.GetProfile)
		settingsGroup.PUT("/profile", h.UpdateProfile)
		settingsGroup.PUT("/preferences", h.UpdatePreferences)
	}
}

// NewModule creates the unified user module.
func NewModule(
	getTenant registry.GetTenantFunc,
	service *Service,
	tokens *token.Service,
) registry.Module {
	return &userModule{
		handler: NewHandler(getTenant, service, tokens),
	}
}

type userModule struct {
	handler *Handler
}

func (m *userModule) Name() string { return "user" }

func (m *userModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
