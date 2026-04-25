package tracedetail

import (
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/shared/errorcode"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

// BundleHandler backs GET /traces/:traceId/bundle — the aggregated always-on
// read path so the FE does one round-trip per mount instead of five.
type BundleHandler struct {
	modulecommon.DBTenant
	Service *BundleService
}

func (h *BundleHandler) GetBundle(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	traceID := c.Param("traceId")
	resp, err := h.Service.Bundle(c.Request.Context(), teamID, traceID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to assemble trace bundle", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}
