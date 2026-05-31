package consumer

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

func (h *Handler) GetConsumeRateByTopic(c *gin.Context) {
	shared.HandleRangeQuery(c, h.GetTenant, "Failed to query consume rate by topic", func(ctx context.Context, teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetConsumeRateByTopic(ctx, teamID, startMs, endMs)
	})
}

func (h *Handler) GetConsumerLagByGroup(c *gin.Context) {
	shared.HandleRangeQuery(c, h.GetTenant, "Failed to query consumer lag by group", func(ctx context.Context, teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetConsumerLagByGroup(ctx, teamID, startMs, endMs)
	})
}
