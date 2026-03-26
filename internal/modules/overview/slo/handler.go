package slo

import (
	"net/http"

	"github.com/observability/observability-backend-go/internal/contracts/errorcode"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"

	"github.com/gin-gonic/gin"
)

type SLOHandler struct {
	modulecommon.DBTenant
	Service Service
}

func (h *SLOHandler) GetSloSli(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	serviceName := c.Query("serviceName")
	if serviceName == "" {
		serviceName = c.Query("service")
	}
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	resp, err := h.Service.GetSloSli(teamID, startMs, endMs, serviceName)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query overview SLO status", err)
		return
	}

	modulecommon.RespondOK(c, resp)
}

func (h *SLOHandler) GetSloStats(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	serviceName := c.Query("serviceName")
	if serviceName == "" {
		serviceName = c.Query("service")
	}
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	resp, err := h.Service.GetSloSli(teamID, startMs, endMs, serviceName)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query overview SLO status", err)
		return
	}

	modulecommon.RespondOK(c, resp.Summary)
}

func (h *SLOHandler) GetBurnDown(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	serviceName := c.Query("serviceName")
	if serviceName == "" {
		serviceName = c.Query("service")
	}
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	points, err := h.Service.GetBurnDown(teamID, startMs, endMs, serviceName)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query SLO burn-down", err)
		return
	}
	modulecommon.RespondOK(c, points)
}

func (h *SLOHandler) GetBurnRate(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	serviceName := c.Query("serviceName")
	if serviceName == "" {
		serviceName = c.Query("service")
	}
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	rate, err := h.Service.GetBurnRate(teamID, startMs, endMs, serviceName)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query SLO burn rate", err)
		return
	}
	modulecommon.RespondOK(c, rate)
}
