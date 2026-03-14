package ai

import (
	"github.com/gin-gonic/gin"
	"github.com/observability/observability-backend-go/internal/modules/ai/dashboard"
	"github.com/observability/observability-backend-go/internal/modules/ai/rundetail"
	"github.com/observability/observability-backend-go/internal/modules/ai/runs"
)

// Config holds configuration for all AI submodules.
type Config struct {
	Dashboard dashboard.Config
	Runs      runs.Config
	RunDetail rundetail.Config
}

func DefaultConfig() Config {
	return Config{
		Dashboard: dashboard.DefaultConfig(),
		Runs:      runs.DefaultConfig(),
		RunDetail: rundetail.DefaultConfig(),
	}
}

// Handlers holds all AI submodule handlers.
type Handlers struct {
	Dashboard *dashboard.Handler
	Runs      *runs.Handler
	RunDetail *rundetail.Handler
}

// RegisterRoutes registers all AI submodule routes under v1.
func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *Handlers) {
	if h == nil {
		return
	}
	dashboard.RegisterRoutes(cfg.Dashboard, v1, h.Dashboard)
	runs.RegisterRoutes(cfg.Runs, v1, h.Runs)
	rundetail.RegisterRoutes(cfg.RunDetail, v1, h.RunDetail)
}
