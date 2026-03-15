package explorer

import "github.com/gin-gonic/gin"

// Config controls whether the explorer module is enabled.
type Config struct {
	Enabled bool
}

// DefaultConfig returns the default configuration.
func DefaultConfig() Config {
	return Config{Enabled: true}
}

// RegisterRoutes wires the explorer endpoints into the router.
func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *Handler) {
	if !cfg.Enabled || h == nil {
		return
	}
	v1.GET("/explorer", h.Explore)
}
