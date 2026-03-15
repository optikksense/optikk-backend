package anomaly

import "github.com/gin-gonic/gin"

// Config controls whether the anomaly module is enabled.
type Config struct {
	Enabled bool
}

// DefaultConfig returns the default module configuration.
func DefaultConfig() Config {
	return Config{Enabled: true}
}

// RegisterRoutes wires the anomaly endpoints into the Gin router group.
func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *Handler) {
	if !cfg.Enabled || h == nil {
		return
	}

	v1.GET("/anomaly/baseline", h.GetBaseline)
}
