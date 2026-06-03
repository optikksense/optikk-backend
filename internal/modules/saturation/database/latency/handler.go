package latency

import (
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/shared/errorcode"

	shared "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/internal/shared"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type Handler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *Handler) GetLatencyBySystem(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetLatencyBySystem(c.Request.Context(), teamID, startMs, endMs, shared.ParseFilters(c))
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query latency by system", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}
