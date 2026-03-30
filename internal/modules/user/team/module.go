package team

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
		usersGroup.POST("/:userId/teams/:teamId", h.AddUserToTeam)
		usersGroup.DELETE("/:userId/teams/:teamId", h.RemoveUserFromTeam)
	}

	teamsGroup := v1.Group("/teams")
	{
		teamsGroup.GET("", h.GetTeams)
		teamsGroup.GET("/my-teams", h.GetMyTeams)
		teamsGroup.GET("/:id", h.GetTeamByID)
		teamsGroup.GET("/slug/:slug", h.GetTeamBySlug)
		teamsGroup.POST("", h.CreateTeam)
	}
}

func NewModule(
	sqlDB *registry.SQLDB,
	getTenant registry.GetTenantFunc,
	configRegistry *registry.ConfigRegistry,
) registry.Module {
	module := &teamModule{}
	module.configure(sqlDB, getTenant, configRegistry)
	return module
}

type teamModule struct {
	handler *Handler
}

func (m *teamModule) Name() string                      { return "team" }
func (m *teamModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *teamModule) configure(
	sqlDB *registry.SQLDB,
	getTenant registry.GetTenantFunc,
	configRegistry *registry.ConfigRegistry,
) {
	m.handler = NewHandler(
		getTenant,
		NewService(NewRepository(sqlDB), configRegistry),
	)
}

func (m *teamModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
