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
	startMs, endMs := ParseRange(c, 7*24*60*60*1000)

	resp, err := h.Service.GetSloSli(teamUUID, startMs, endMs, serviceName)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query overview SLO status")
		return
	}

	RespondOK(c, resp)
}
