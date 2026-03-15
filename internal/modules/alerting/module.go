package alerting

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

	// Rules
	v1.POST("/alerting/rules", h.CreateRule)
	v1.GET("/alerting/rules", h.ListRules)
	v1.GET("/alerting/rules/:id", h.GetRuleByID)
	v1.PUT("/alerting/rules/:id", h.UpdateRule)
	v1.DELETE("/alerting/rules/:id", h.DeleteRule)

	// Channels
	v1.POST("/alerting/channels", h.CreateChannel)
	v1.GET("/alerting/channels", h.ListChannels)
	v1.GET("/alerting/channels/:id", h.GetChannelByID)
	v1.PUT("/alerting/channels/:id", h.UpdateChannel)
	v1.DELETE("/alerting/channels/:id", h.DeleteChannel)

	// Incidents
	v1.GET("/alerting/incidents", h.ListIncidents)
	v1.GET("/alerting/incidents/:id", h.GetIncidentByID)
	v1.PUT("/alerting/incidents/:id", h.UpdateIncidentStatus)

	// Summary
	v1.GET("/alerting/summary", h.GetAlertSummary)
}
