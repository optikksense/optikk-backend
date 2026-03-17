package auth

import "github.com/gin-gonic/gin"

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
