package hosts

import (
	"context"

	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type Handler struct {
	modulecommon.DBTenant
	Service *Service
}

// GetHosts retrieves the host saturation list, optionally filtered by service.
func (h *Handler) GetHosts(c *gin.Context) {
	modulecommon.HandleRangeQuery(c, h.GetTenant, "Failed to query hosts", func(ctx context.Context, teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetHosts(ctx, teamID, startMs, endMs, c.Query("service"))
	})
}
