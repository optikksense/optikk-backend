package auth

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

	authGroup := v1.Group("/auth")
	{
		authGroup.POST("/login", h.Login)
		authGroup.POST("/logout", h.Logout)
		authGroup.GET("/me", h.AuthMe)
		authGroup.GET("/context", h.AuthContext)
		authGroup.GET("/validate", h.ValidateToken)
		authGroup.GET("/google", h.GoogleLogin)
		authGroup.GET("/google/callback", h.GoogleCallback)
		authGroup.GET("/github", h.GithubLogin)
		authGroup.GET("/github/callback", h.GithubCallback)
		authGroup.POST("/oauth/complete-signup", h.CompleteSignup)
		authGroup.POST("/forgot-password", h.ForgotPassword)
	}
}

func init() {
	registry.Register(&authModule{})
}

type authModule struct {
	handler *Handler
}

func (m *authModule) Name() string                      { return "auth" }
func (m *authModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *authModule) Init(deps registry.Deps) error {
	m.handler = NewHandler(
		deps.GetTenant,
		NewService(
			NewRepository(deps.DB),
			deps.SessionManager,
			deps.Config.OAuth.GoogleClientID, deps.Config.OAuth.GoogleClientSecret,
			deps.Config.OAuth.GitHubClientID, deps.Config.OAuth.GitHubClientSecret,
			deps.Config.OAuth.RedirectBase,
		),
	)
	return nil
}

func (m *authModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
