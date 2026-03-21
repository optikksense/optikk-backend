package userpage

import (
	"github.com/gin-gonic/gin"
	"github.com/observability/observability-backend-go/internal/modules/registry"
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
	}
}

func init() {
	registry.Register(&userPageModule{})
}

type userPageModule struct {
	handler *Handler
}

func (m *userPageModule) Name() string                      { return "userPage" }
func (m *userPageModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *userPageModule) Init(deps registry.Deps) error {
	m.handler = NewHandler(
		deps.GetTenant,
		NewService(NewRepository(deps.DB)),
	)
	return nil
}

func (m *userPageModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
