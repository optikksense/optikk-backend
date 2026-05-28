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
	// All routes removed as the frontend does not consume them.
}

func NewModule(
	sqlDB *registry.SQLDB,
	getTenant registry.GetTenantFunc,
	appConfig registry.AppConfig,
) registry.Module {
	module := &teamModule{}
	module.configure(sqlDB, getTenant, appConfig)
	return module
}

type teamModule struct {
	handler *Handler
}

func (m *teamModule) Name() string { return "team" }

func (m *teamModule) configure(
	sqlDB *registry.SQLDB,
	getTenant registry.GetTenantFunc,
	appConfig registry.AppConfig,
) {
	m.handler = NewHandler(
		getTenant,
		NewService(NewRepository(sqlDB, appConfig)),
	)
}

func (m *teamModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
