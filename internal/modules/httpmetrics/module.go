package httpmetrics

import "github.com/gin-gonic/gin"

// Config holds HTTP metrics module route configuration.
type Config struct {
	Enabled bool
}

// DefaultConfig returns default configuration.
func DefaultConfig() Config {
	return Config{Enabled: true}
}

// RegisterRoutes mounts HTTP metrics routes.
func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *HTTPMetricsHandler) {
	if !cfg.Enabled || h == nil {
		return
	}
	g := v1.Group("/http")
	g.GET("/request-rate", h.GetRequestRate)
	g.GET("/request-duration", h.GetRequestDuration)
	g.GET("/active-requests", h.GetActiveRequests)
	g.GET("/request-body-size", h.GetRequestBodySize)
	g.GET("/response-body-size", h.GetResponseBodySize)
	g.GET("/client-duration", h.GetClientDuration)
	g.GET("/dns-duration", h.GetDNSDuration)
	g.GET("/tls-duration", h.GetTLSDuration)
}
