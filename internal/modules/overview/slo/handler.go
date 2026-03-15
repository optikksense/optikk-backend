package slo

import (
	"net/http"

	"github.com/gin-gonic/gin"
	. "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

type SLOHandler struct {
	modulecommon.DBTenant
	Service Service
}

func (h *SLOHandler) GetSloSli(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	serviceName := c.Query("serviceName")
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	resp, err := h.Service.GetSloSli(teamID, startMs, endMs, serviceName)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query overview SLO status")
		return
	}

	RespondOK(c, resp)
}

func (h *SLOHandler) GetSloStats(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	serviceName := c.Query("serviceName")
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	resp, err := h.Service.GetSloSli(teamID, startMs, endMs, serviceName)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query overview SLO status")
		return
	}

	RespondOK(c, resp.Summary)
}

func (h *SLOHandler) GetBurnDown(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	serviceName := c.Query("serviceName")
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	points, err := h.Service.GetBurnDown(teamID, startMs, endMs, serviceName)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query SLO burn-down")
		return
	}
	RespondOK(c, points)
}

func (h *SLOHandler) GetBurnRate(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	serviceName := c.Query("serviceName")
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	rate, err := h.Service.GetBurnRate(teamID, startMs, endMs, serviceName)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query SLO burn rate")
		return
	}
	RespondOK(c, rate)
}
