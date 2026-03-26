package userpage

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

	usersGroup := v1.Group("/users")
	{
		usersGroup.GET("/me", h.GetCurrentUser)
		usersGroup.GET("", h.GetUsers)
		usersGroup.GET("/:id", h.GetUserByID)
		usersGroup.POST("", h.CreateUser)
	}

	settingsGroup := v1.Group("/settings")
	{
		settingsGroup.GET("/profile", h.GetProfile)
		settingsGroup.PUT("/profile", h.UpdateProfile)
		settingsGroup.PUT("/preferences", h.UpdatePreferences)
	}
}

func NewModule(sqlDB *registry.SQLDB, getTenant registry.GetTenantFunc) registry.Module {
	module := &userPageModule{}
	module.configure(sqlDB, getTenant)
	return module
}

type userPageModule struct {
	handler *Handler
}

func (m *userPageModule) Name() string                      { return "userPage" }
func (m *userPageModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *userPageModule) configure(sqlDB *registry.SQLDB, getTenant registry.GetTenantFunc) {
	m.handler = NewHandler(
		getTenant,
		NewService(NewRepository(sqlDB)),
	)
}

func (m *userPageModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
