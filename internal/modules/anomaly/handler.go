package anomaly

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	. "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

// Handler exposes anomaly detection HTTP endpoints.
type Handler struct {
	modulecommon.DBTenant
	Service Service
}

// GetBaseline returns time-series data with baseline bounds and anomaly flags.
func (h *Handler) GetBaseline(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	sensitivity := 2.0
	if raw := c.Query("sensitivity"); raw != "" {
		if v, err := strconv.ParseFloat(raw, 64); err == nil && v > 0 {
			sensitivity = v
		}
	}

	req := BaselineRequest{
		Metric:      c.Query("metric"),
		ServiceName: c.Query("serviceName"),
		Sensitivity: sensitivity,
	}

	points, err := h.Service.GetBaseline(teamID, startMs, endMs, req)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to compute anomaly baseline")
		return
	}

	RespondOK(c, points)
}
