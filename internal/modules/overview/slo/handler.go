package slo

import (
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts/errorcode"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"

	"github.com/gin-gonic/gin"
)

type SLOHandler struct {
	modulecommon.DBTenant
	Service Service
}

func (h *SLOHandler) GetSloSli(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	serviceName := c.Query("serviceName")

	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	resp, err := h.Service.GetSloSli(c.Request.Context(), teamID, startMs, endMs, serviceName)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query overview SLO status", err)
		return
	}

	modulecommon.RespondOK(c, resp)
}

func (h *SLOHandler) GetSloStats(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	serviceName := c.Query("serviceName")

	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	resp, err := h.Service.GetSloSli(c.Request.Context(), teamID, startMs, endMs, serviceName)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query overview SLO status", err)
		return
	}

	modulecommon.RespondOK(c, resp.Summary)
}

func (h *SLOHandler) GetBurnDown(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	serviceName := c.Query("serviceName")

	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	points, err := h.Service.GetBurnDown(c.Request.Context(), teamID, startMs, endMs, serviceName)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query SLO burn-down", err)
		return
	}
	modulecommon.RespondOK(c, points)
}

func (h *SLOHandler) GetBurnRate(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	serviceName := c.Query("serviceName")

	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	rate, err := h.Service.GetBurnRate(c.Request.Context(), teamID, startMs, endMs, serviceName)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query SLO burn rate", err)
		return
	}
	modulecommon.RespondOK(c, rate)
}
