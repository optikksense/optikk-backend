package hosts

import (
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/shared/errorcode"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type Handler struct {
	modulecommon.DBTenant
	Service *Service
}

// GetHosts is GET /api/v1/infrastructure/hosts. With no ?service= it returns the
// fleet-wide host saturation list; with ?service=<name> it returns the hosts
// running that service, enriched with RED traffic.
func (h *Handler) GetHosts(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetHosts(c.Request.Context(), teamID, startMs, endMs, c.Query("service"))
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query hosts", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}
