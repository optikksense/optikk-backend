package team

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
