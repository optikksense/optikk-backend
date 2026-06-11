package cpu

import (
	"context"

	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type CPUHandler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *CPUHandler) GetAvgCPU(c *gin.Context) {
	modulecommon.HandleRangeQuery(c, h.GetTenant, "Failed to query avg CPU", func(ctx context.Context, teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetAvgCPU(ctx, teamID, startMs, endMs)
	})
}

func (h *CPUHandler) GetCPUByInstance(c *gin.Context) {
	modulecommon.HandleRangeQuery(c, h.GetTenant, "Failed to query CPU by instance", func(ctx context.Context, teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetCPUByInstance(ctx, teamID, startMs, endMs)
	})
}
