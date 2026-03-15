package explorer

import (
	"net/http"

	"github.com/gin-gonic/gin"
	. "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

// Handler serves the data explorer endpoint.
type Handler struct {
	modulecommon.DBTenant
	Service *Service
}

// Explore handles GET /explorer.
func (h *Handler) Explore(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	req := ExploreRequest{
		SignalType:      c.DefaultQuery("signalType", "span"),
		Aggregation:     c.DefaultQuery("aggregation", "count"),
		GroupBy:         c.Query("groupBy"),
		FilterService:   c.Query("service"),
		FilterOperation: c.Query("operation"),
		Mode:            c.DefaultQuery("mode", "timeseries"),
	}

	result, err := h.Service.Explore(teamID, startMs, endMs, req)
	if err != nil {
		RespondError(c, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	RespondOK(c, result)
}
