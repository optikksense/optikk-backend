package errorsinbox

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

	v1.GET("/errors-inbox/summary", h.GetSummary)
	v1.GET("/errors-inbox/:groupId", h.GetStatus)
	v1.GET("/errors-inbox", h.ListByStatus)
	v1.PUT("/errors-inbox/:groupId", h.UpdateStatus)
	v1.POST("/errors-inbox/bulk", h.BulkUpdate)
}
