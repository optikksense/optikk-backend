package explore

import "github.com/gin-gonic/gin"

// Config holds explore-module route configuration.
type Config struct {
	Enabled bool
}

// DefaultConfig returns default explore-module configuration.
func DefaultConfig() Config {
	return Config{Enabled: true}
}

// RegisterRoutes mounts explore routes.
func RegisterRoutes(cfg Config, _ *gin.RouterGroup, v1 *gin.RouterGroup, h *ExploreHandler) {
	if !cfg.Enabled || h == nil {
		return
	}

	v1.GET("/explore/saved-queries", h.ListSavedQueries)
	v1.POST("/explore/saved-queries", h.CreateSavedQuery)
	v1.PUT("/explore/saved-queries/:id", h.UpdateSavedQuery)
	v1.DELETE("/explore/saved-queries/:id", h.DeleteSavedQuery)
}
