package session

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/gin-gonic/gin"
)

// Config holds the session module configuration.
type Config struct {
	Enabled bool
}

// DefaultConfig returns the default module config.
func DefaultConfig() Config {
	return Config{Enabled: true}
}

// NewModule creates a new session module instance.
func NewModule(service *Service) registry.Module {
	return &sessionModule{service: service}
}

type sessionModule struct {
	service *Service
}

// Name returns the module name.
func (m *sessionModule) Name() string {
	return "session"
}

// RegisterRoutes registers HTTP routes for the module.
func (m *sessionModule) RegisterRoutes(group *gin.RouterGroup) {
	// Sessions are managed via middleware, no direct endpoints.
}
