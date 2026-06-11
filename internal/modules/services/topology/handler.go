package topology

import (
	"context"

	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

// Handler serves the runtime service topology API.
type Handler struct {
	modulecommon.DBTenant
	Service *Service
}

// GetTopology returns the service topology graph, optionally filtered.
func (h *Handler) GetTopology(c *gin.Context) {
	service := c.Query("service")
	modulecommon.HandleRangeQuery(c, h.GetTenant, "Failed to build service topology", func(ctx context.Context, teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetTopology(ctx, teamID, startMs, endMs, service)
	})
}
