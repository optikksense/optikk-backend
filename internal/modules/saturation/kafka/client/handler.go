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

func (h *Handler) GetKafkaSummaryStats(c *gin.Context) {
	shared.HandleRangeQuery(c, h.GetTenant, "Failed to query kafka summary stats", func(ctx context.Context, teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetKafkaSummaryStats(ctx, teamID, startMs, endMs)
	})
}

func (h *Handler) GetE2ELatency(c *gin.Context) {
	shared.HandleRangeQuery(c, h.GetTenant, "Failed to query end-to-end latency", func(ctx context.Context, teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetE2ELatency(ctx, teamID, startMs, endMs)
	})
}

func (h *Handler) GetBrokerConnections(c *gin.Context) {
	shared.HandleRangeQuery(c, h.GetTenant, "Failed to query broker connections", func(ctx context.Context, teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetBrokerConnections(ctx, teamID, startMs, endMs)
	})
}

func (h *Handler) GetClientOperationDuration(c *gin.Context) {
	shared.HandleRangeQuery(c, h.GetTenant, "Failed to query client operation duration", func(ctx context.Context, teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetClientOperationDuration(ctx, teamID, startMs, endMs)
	})
}

func (h *Handler) GetClientOpErrors(c *gin.Context) {
	shared.HandleRangeQuery(c, h.GetTenant, "Failed to query client operation errors", func(ctx context.Context, teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetClientOpErrors(ctx, teamID, startMs, endMs)
	})
}
