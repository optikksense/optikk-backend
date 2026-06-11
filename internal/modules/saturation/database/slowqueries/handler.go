package slowqueries

import (
	"context"

	"github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/filter"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type Handler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *Handler) GetSlowQueryPatterns(c *gin.Context) {
	modulecommon.HandleRangeQuery(c, h.GetTenant, "Failed to query slow query patterns", func(ctx context.Context, teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetSlowQueryPatterns(ctx, teamID, startMs, endMs, filter.ParseFilters(c), filter.ParseLimit(c, 20))
	})
}
