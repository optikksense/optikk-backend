package slo

import (
	"net/http"

	"github.com/gin-gonic/gin"
	. "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

// SLOHandler handles overview SLO endpoints.
type SLOHandler struct {
	modulecommon.DBTenant
	Service Service
}

// GetSloSli returns the SLO dashboard payload.
func (h *SLOHandler) GetSloSli(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	serviceName := c.Query("serviceName")
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	resp, err := h.Service.GetSloSli(teamUUID, startMs, endMs, serviceName)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query overview SLO status")
		return
	}

	RespondOK(c, resp)
}

// GetSloStats returns a flat scalar summary of the current SLO status.
// Intended for stat-card components that need individual scalar fields.
func (h *SLOHandler) GetSloStats(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	serviceName := c.Query("serviceName")
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	resp, err := h.Service.GetSloSli(teamUUID, startMs, endMs, serviceName)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query overview SLO status")
		return
	}

	RespondOK(c, resp.Summary)
}
