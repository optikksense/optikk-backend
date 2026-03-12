package user

import "github.com/gin-gonic/gin"

type Config struct {
	Enabled bool
}

func DefaultConfig() Config {
	return Config{Enabled: true}
}

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, auth *AuthHandler, users *UserHandler, oauth *OAuthHandler) {
	if !cfg.Enabled || auth == nil || users == nil {
		return
	}

	authGroup := v1.Group("/auth")
	{
		authGroup.POST("/login", auth.Login)
		authGroup.POST("/logout", auth.Logout)
		authGroup.GET("/me", auth.AuthMe)
		authGroup.GET("/context", auth.AuthContext)
		authGroup.GET("/validate", auth.ValidateToken)

		if oauth != nil {
			authGroup.GET("/google", oauth.GoogleLogin)
			authGroup.GET("/google/callback", oauth.GoogleCallback)
			authGroup.GET("/github", oauth.GithubLogin)
			authGroup.GET("/github/callback", oauth.GithubCallback)
			authGroup.POST("/oauth/complete-signup", oauth.CompleteSignup)
			authGroup.POST("/forgot-password", oauth.ForgotPassword)
		}
	}

	usersGroup := v1.Group("/users")
	{
		usersGroup.GET("/me", users.GetCurrentUser)
		usersGroup.GET("", users.GetUsers)
		usersGroup.GET("/:id", users.GetUserByID)
		usersGroup.POST("", users.CreateUser)
		usersGroup.POST("/:userId/teams/:teamId", users.AddUserToTeam)
		usersGroup.DELETE("/:userId/teams/:teamId", users.RemoveUserFromTeam)
	}

	teamsGroup := v1.Group("/teams")
	{
		teamsGroup.GET("", users.GetTeams)
		teamsGroup.GET("/my-teams", users.GetMyTeams)
		teamsGroup.GET("/:id", users.GetTeamByID)
		teamsGroup.GET("/slug/:slug", users.GetTeamBySlug)
		teamsGroup.POST("", users.CreateTeam)
	}

	settingsGroup := v1.Group("/settings")
	{
		settingsGroup.GET("/profile", users.GetProfile)
		settingsGroup.PUT("/profile", users.UpdateProfile)
	}
}
