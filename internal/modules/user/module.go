package identity

import "github.com/gin-gonic/gin"

// Config holds identity-module route configuration.
type Config struct {
	Enabled bool
}

// DefaultConfig returns default identity-module configuration.
func DefaultConfig() Config {
	return Config{Enabled: true}
}

func RegisterRoutes(cfg Config, api *gin.RouterGroup, v1 *gin.RouterGroup, auth *AuthHandler, users *UserHandler) {
	if !cfg.Enabled || auth == nil || users == nil {
		return
	}

	// Register on both route groups so /api/v1/* is the canonical prefix while
	// the legacy /api/* paths keep working for existing clients.
	for _, g := range []*gin.RouterGroup{api, v1} {
		authGroup := g.Group("/auth")
		{
			authGroup.POST("/login", auth.Login)
			authGroup.POST("/logout", auth.Logout)
			authGroup.GET("/me", auth.AuthMe)
			authGroup.GET("/context", auth.AuthContext)
			authGroup.GET("/validate", auth.ValidateToken)
		}

		usersGroup := g.Group("/users")
		{
			usersGroup.GET("/me", users.GetCurrentUser)
			usersGroup.GET("", users.GetUsers)
			usersGroup.GET("/:id", users.GetUserByID)
			usersGroup.POST("", users.CreateUser)
			usersGroup.POST("/:userId/teams/:teamId", users.AddUserToTeam)
			usersGroup.DELETE("/:userId/teams/:teamId", users.RemoveUserFromTeam)
		}

		teamsGroup := g.Group("/teams")
		{
			teamsGroup.GET("", users.GetTeams)
			teamsGroup.GET("/my-teams", users.GetMyTeams)
			teamsGroup.GET("/:id", users.GetTeamByID)
			teamsGroup.GET("/slug/:slug", users.GetTeamBySlug)
			teamsGroup.POST("", users.CreateTeam)
		}

		settingsGroup := g.Group("/settings")
		{
			settingsGroup.GET("/profile", users.GetProfile)
			settingsGroup.PUT("/profile", users.UpdateProfile)
		}
	}
}
