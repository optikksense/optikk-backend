package auth

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/gin-gonic/gin"
)

type Config struct {
	Enabled bool
}

func DefaultConfig() Config {
	return Config{Enabled: true}
}

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *Handler) {
	if !cfg.Enabled || h == nil {
		return
	}

	authGroup := v1.Group("/auth")
	{
		authGroup.POST("/login", h.Login)
		authGroup.POST("/logout", h.Logout)
		authGroup.GET("/me", h.AuthMe)
		authGroup.GET("/validate", h.ValidateToken)
		authGroup.POST("/forgot-password", h.ForgotPassword)
	}
}

func NewModule(
	sqlDB *registry.SQLDB,
	getTenant registry.GetTenantFunc,
	sessionManager registry.SessionManager,
	appConfig registry.AppConfig,
) registry.Module {
	module := &authModule{}
	module.configure(sqlDB, getTenant, sessionManager, appConfig)
	return module
}

type authModule struct {
	handler *Handler
}

func (m *authModule) Name() string { return "auth" }

func (m *authModule) configure(
	sqlDB *registry.SQLDB,
	getTenant registry.GetTenantFunc,
	sessionManager registry.SessionManager,
	appConfig registry.AppConfig,
) {
	m.handler = NewHandler(
		getTenant,
		NewService(
			NewRepository(sqlDB, appConfig),
			sessionManager,
		),
	)
}

func (m *authModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
