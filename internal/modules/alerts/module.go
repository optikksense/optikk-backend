package alerts

import "github.com/gin-gonic/gin"

// Config holds alert-module route configuration.
type Config struct {
	Enabled bool
}

// DefaultConfig returns default alert-module configuration.
func DefaultConfig() Config {
	return Config{Enabled: true}
}

// RegisterRoutes mounts alert routes on both the legacy /api and versioned
// /api/v1 route groups for backward compatibility.
func RegisterRoutes(cfg Config, api *gin.RouterGroup, v1 *gin.RouterGroup, h *AlertHandler) {
	if !cfg.Enabled || h == nil {
		return
	}

	// Register alert CRUD on both groups so /api/v1/alerts/* is canonical
	// while /api/alerts/* keeps working for existing clients.
	for _, g := range []*gin.RouterGroup{api, v1} {
		alerts := g.Group("/alerts")
		{
			alerts.GET("", h.GetAlerts)
			alerts.GET("/paged", h.GetAlertsPaged)
			alerts.GET("/:id", h.GetAlertByID)
			alerts.POST("", h.CreateAlert)
			alerts.POST("/:id/acknowledge", h.AcknowledgeAlert)
			alerts.POST("/:id/resolve", h.ResolveAlert)
			alerts.POST("/:id/mute", h.MuteAlert)
			alerts.POST("/:id/mute-with-reason", h.MuteAlertWithReason)
			alerts.POST("/bulk/mute", h.BulkMuteAlerts)
			alerts.POST("/bulk/resolve", h.BulkResolveAlerts)
			alerts.GET("/for-incident/:policyId", h.GetAlertsForIncident)
			alerts.GET("/count/active", h.CountActiveAlerts)
		}
	}

	v1.GET("/incidents", h.GetIncidents)
}
