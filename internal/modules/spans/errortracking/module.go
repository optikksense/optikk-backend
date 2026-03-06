package errortracking

import "github.com/gin-gonic/gin"

// Config holds error tracking module route configuration.
type Config struct {
	Enabled bool
}

// DefaultConfig returns default error tracking module configuration.
func DefaultConfig() Config {
	return Config{Enabled: true}
}

// RegisterRoutes mounts error tracking routes.
func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *ErrorTrackingHandler) {
	if !cfg.Enabled || h == nil {
		return
	}
	v1.GET("/spans/exception-rate-by-type", h.GetExceptionRateByType)
	v1.GET("/spans/error-hotspot", h.GetErrorHotspot)
	v1.GET("/spans/http-5xx-by-route", h.GetHTTP5xxByRoute)
}
