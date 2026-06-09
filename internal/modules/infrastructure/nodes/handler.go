package nodes

import (
	"context"

	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type NodeHandler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *NodeHandler) GetInfrastructureNodes(c *gin.Context) {
	modulecommon.HandleRangeQuery(c, h.GetTenant, "Failed to query node health", func(ctx context.Context, teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetInfrastructureNodes(ctx, teamID, startMs, endMs)
	})
}

func (h *NodeHandler) GetInfrastructureNodeSummary(c *gin.Context) {
	modulecommon.HandleRangeQuery(c, h.GetTenant, "Failed to query node summary", func(ctx context.Context, teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetInfrastructureNodeSummary(ctx, teamID, startMs, endMs)
	})
}

func (h *NodeHandler) GetInfrastructureNodeServices(c *gin.Context) {
	host := c.Param("host")
	modulecommon.HandleRangeQuery(c, h.GetTenant, "Failed to query node services", func(ctx context.Context, teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetInfrastructureNodeServices(ctx, teamID, host, startMs, endMs)
	})
}
