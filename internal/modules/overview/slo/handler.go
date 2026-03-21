package slo

import (
	"net/http"

	"github.com/observability/observability-backend-go/internal/contracts/errorcode"

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
	if serviceName == "" {
		serviceName = c.Query("service")
	}
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	resp, err := h.Service.GetSloSli(teamID, startMs, endMs, serviceName)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query overview SLO status", err)
		return
	}

	RespondOK(c, resp)
}

func (h *SLOHandler) GetSloStats(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	serviceName := c.Query("serviceName")
	if serviceName == "" {
		serviceName = c.Query("service")
	}
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	resp, err := h.Service.GetSloSli(teamID, startMs, endMs, serviceName)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query overview SLO status", err)
		return
	}

	RespondOK(c, resp.Summary)
}

func (h *SLOHandler) GetBurnDown(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	serviceName := c.Query("serviceName")
	if serviceName == "" {
		serviceName = c.Query("service")
	}
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	points, err := h.Service.GetBurnDown(teamID, startMs, endMs, serviceName)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query SLO burn-down", err)
		return
	}
	RespondOK(c, points)
}

func (h *SLOHandler) GetBurnRate(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	serviceName := c.Query("serviceName")
	if serviceName == "" {
		serviceName = c.Query("service")
	}
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	rate, err := h.Service.GetBurnRate(teamID, startMs, endMs, serviceName)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query SLO burn rate", err)
		return
	}
	RespondOK(c, rate)
}
