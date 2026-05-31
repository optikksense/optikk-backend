package producer

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

func (h *Handler) GetProduceRateByTopic(c *gin.Context) {
	shared.HandleRangeQuery(c, h.GetTenant, "Failed to query produce rate by topic", func(ctx context.Context, teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetProduceRateByTopic(ctx, teamID, startMs, endMs)
	})
}
