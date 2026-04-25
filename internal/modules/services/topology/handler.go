package topology

import (
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/shared/errorcode"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

// Handler serves the runtime service topology API.
type Handler struct {
	modulecommon.DBTenant
	Service Service
}

// GetTopology is GET /services/topology.
// Optional ?service=<name> prunes the result to the 1-hop neighborhood of that service.
func (h *Handler) GetTopology(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	service := c.Query("service")

	out, err := h.Service.GetTopology(c.Request.Context(), teamID, startMs, endMs, service)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to build service topology", err)
		return
	}
	modulecommon.RespondOK(c, out)
}
