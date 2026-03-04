package database

import "github.com/gin-gonic/gin"

// Config holds database saturation route configuration.
type Config struct {
	Enabled bool
}

// DefaultConfig returns default configuration.
func DefaultConfig() Config {
	return Config{Enabled: true}
}

// RegisterRoutes mounts saturation routes for the database module.
func RegisterRoutes(cfg Config, _ *gin.RouterGroup, v1 *gin.RouterGroup, h *DatabaseHandler) {
	if !cfg.Enabled || h == nil {
		return
	}
	v1.GET("/saturation/database/query-by-table", h.GetDatabaseQueryByTable)
	v1.GET("/saturation/database/avg-latency", h.GetDatabaseAvgLatency)
	v1.GET("/saturation/database/latency-summary", h.GetDatabaseCacheSummary)
	v1.GET("/saturation/database/systems", h.GetDatabaseSystems)
	v1.GET("/saturation/database/top-tables", h.GetDatabaseTopTables)
}
