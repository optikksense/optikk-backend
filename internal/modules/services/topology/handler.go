package topology

import (
	"net/http"

	"github.com/gin-gonic/gin"
	. "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

// TopologyHandler handles services topology endpoints.
type TopologyHandler struct {
	modulecommon.DBTenant
	Service Service
}

// GetTopology returns the complete service topology graph payload.
func (h *TopologyHandler) GetTopology(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)

	resp, err := h.Service.GetTopology(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query topology")
		return
	}

	RespondOK(c, resp)
}
