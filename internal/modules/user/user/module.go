package userpage

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
