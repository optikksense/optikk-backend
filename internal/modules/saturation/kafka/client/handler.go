package client

import (
	"context"

	"github.com/Optikk-Org/optikk-backend/internal/modules/saturation/kafka/internal/shared"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type Handler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *Handler) GetE2ELatency(c *gin.Context) {
	shared.HandleRangeQuery(c, h.GetTenant, "Failed to query end-to-end latency", func(ctx context.Context, teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetE2ELatency(ctx, teamID, startMs, endMs)
	})
}
