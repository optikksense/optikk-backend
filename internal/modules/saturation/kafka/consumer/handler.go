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

func (h *Handler) GetReceiveLatencyByTopic(c *gin.Context) {
	shared.HandleRangeQuery(c, h.GetTenant, "Failed to query receive latency by topic", func(ctx context.Context, teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetReceiveLatencyByTopic(ctx, teamID, startMs, endMs)
	})
}

func (h *Handler) GetConsumeRateByGroup(c *gin.Context) {
	shared.HandleRangeQuery(c, h.GetTenant, "Failed to query consume rate by group", func(ctx context.Context, teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetConsumeRateByGroup(ctx, teamID, startMs, endMs)
	})
}

func (h *Handler) GetProcessRateByGroup(c *gin.Context) {
	shared.HandleRangeQuery(c, h.GetTenant, "Failed to query process rate by group", func(ctx context.Context, teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetProcessRateByGroup(ctx, teamID, startMs, endMs)
	})
}

func (h *Handler) GetProcessLatencyByGroup(c *gin.Context) {
	shared.HandleRangeQuery(c, h.GetTenant, "Failed to query process latency by group", func(ctx context.Context, teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetProcessLatencyByGroup(ctx, teamID, startMs, endMs)
	})
}

func (h *Handler) GetConsumeErrors(c *gin.Context) {
	shared.HandleRangeQuery(c, h.GetTenant, "Failed to query consume errors", func(ctx context.Context, teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetConsumeErrors(ctx, teamID, startMs, endMs)
	})
}

func (h *Handler) GetProcessErrors(c *gin.Context) {
	shared.HandleRangeQuery(c, h.GetTenant, "Failed to query process errors", func(ctx context.Context, teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetProcessErrors(ctx, teamID, startMs, endMs)
	})
}

func (h *Handler) GetConsumerLagByGroup(c *gin.Context) {
	shared.HandleRangeQuery(c, h.GetTenant, "Failed to query consumer lag by group", func(ctx context.Context, teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetConsumerLagByGroup(ctx, teamID, startMs, endMs)
	})
}

func (h *Handler) GetConsumerLagPerPartition(c *gin.Context) {
	shared.HandleRangeQuery(c, h.GetTenant, "Failed to query consumer lag per partition", func(ctx context.Context, teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetConsumerLagPerPartition(ctx, teamID, startMs, endMs)
	})
}

func (h *Handler) GetRebalanceSignals(c *gin.Context) {
	shared.HandleRangeQuery(c, h.GetTenant, "Failed to query rebalance signals", func(ctx context.Context, teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetRebalanceSignals(ctx, teamID, startMs, endMs)
	})
}
