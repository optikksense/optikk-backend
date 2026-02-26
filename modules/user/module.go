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

// AuthRoutes defines auth endpoints used by identity route registration.
type AuthRoutes interface {
	Login(*gin.Context)
	Logout(*gin.Context)
	AuthMe(*gin.Context)
	AuthContext(*gin.Context)
	ValidateToken(*gin.Context)
}

// UserRoutes defines user/team/profile endpoints used by identity route registration.
type UserRoutes interface {
	GetCurrentUser(*gin.Context)
	GetUsers(*gin.Context)
	GetUserByID(*gin.Context)
	Signup(*gin.Context)
	CreateUser(*gin.Context)
	AddUserToTeam(*gin.Context)
	RemoveUserFromTeam(*gin.Context)
	GetTeams(*gin.Context)
	GetMyTeams(*gin.Context)
	GetTeamByID(*gin.Context)
	GetTeamBySlug(*gin.Context)
	CreateTeam(*gin.Context)
	GetProfile(*gin.Context)
	UpdateProfile(*gin.Context)
}

// RegisterRoutes mounts identity routes.
func RegisterRoutes(cfg Config, api *gin.RouterGroup, auth AuthRoutes, users UserRoutes) {
	if !cfg.Enabled || auth == nil || users == nil {
		return
	}

	api.POST("/signup", users.Signup)

	authGroup := api.Group("/auth")
	{
		authGroup.POST("/login", auth.Login)
		authGroup.POST("/logout", auth.Logout)
		authGroup.GET("/me", auth.AuthMe)
		authGroup.GET("/context", auth.AuthContext)
		authGroup.GET("/validate", auth.ValidateToken)
	}

	usersGroup := api.Group("/users")
	{
		usersGroup.GET("/me", users.GetCurrentUser)
		usersGroup.GET("", users.GetUsers)
		usersGroup.GET("/:id", users.GetUserByID)
		usersGroup.POST("", users.CreateUser)
		usersGroup.POST("/:userId/teams/:teamId", users.AddUserToTeam)
		usersGroup.DELETE("/:userId/teams/:teamId", users.RemoveUserFromTeam)
	}

	teamsGroup := api.Group("/teams")
	{
		teamsGroup.GET("", users.GetTeams)
		teamsGroup.GET("/my-teams", users.GetMyTeams)
		teamsGroup.GET("/:id", users.GetTeamByID)
		teamsGroup.GET("/slug/:slug", users.GetTeamBySlug)
		teamsGroup.POST("", users.CreateTeam)
	}

	settingsGroup := api.Group("/settings")
	{
		settingsGroup.GET("/profile", users.GetProfile)
		settingsGroup.PUT("/profile", users.UpdateProfile)
	}
}
