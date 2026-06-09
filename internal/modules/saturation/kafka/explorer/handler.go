package explorer

import (
	"context"

	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type Handler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *Handler) GetTopicThroughput(c *gin.Context) {
	topic := c.Query("topic")
	modulecommon.HandleRangeQuery(c, h.GetTenant, "Failed to query topic throughput", func(ctx context.Context, teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetTopicThroughput(ctx, teamID, startMs, endMs, topic)
	})
}

func (h *Handler) GetTopicLag(c *gin.Context) {
	topic := c.Query("topic")
	modulecommon.HandleRangeQuery(c, h.GetTenant, "Failed to query topic lag", func(ctx context.Context, teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetTopicLag(ctx, teamID, startMs, endMs, topic)
	})
}

func (h *Handler) GetTopicConsumers(c *gin.Context) {
	topic := c.Query("topic")
	modulecommon.HandleRangeQuery(c, h.GetTenant, "Failed to query topic consumers", func(ctx context.Context, teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetTopicConsumers(ctx, teamID, startMs, endMs, topic)
	})
}

func (h *Handler) GetGroupPartitions(c *gin.Context) {
	group := c.Query("group")
	modulecommon.HandleRangeQuery(c, h.GetTenant, "Failed to query group partitions", func(ctx context.Context, teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetGroupPartitions(ctx, teamID, startMs, endMs, group)
	})
}

func (h *Handler) GetGroupCommits(c *gin.Context) {
	group := c.Query("group")
	modulecommon.HandleRangeQuery(c, h.GetTenant, "Failed to query group commits", func(ctx context.Context, teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetGroupCommits(ctx, teamID, startMs, endMs, group)
	})
}

func (h *Handler) GetGroupFetches(c *gin.Context) {
	group := c.Query("group")
	modulecommon.HandleRangeQuery(c, h.GetTenant, "Failed to query group fetches", func(ctx context.Context, teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetGroupFetches(ctx, teamID, startMs, endMs, group)
	})
}

func (h *Handler) GetGroupHealth(c *gin.Context) {
	group := c.Query("group")
	modulecommon.HandleRangeQuery(c, h.GetTenant, "Failed to query group health", func(ctx context.Context, teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetGroupHealth(ctx, teamID, startMs, endMs, group)
	})
}
