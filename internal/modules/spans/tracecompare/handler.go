package tracecompare

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/observability/observability-backend-go/internal/modules/common"
)

type Handler struct {
	getTenant common.GetTenantFunc
	svc       *Service
}

func NewHandler(getTenant common.GetTenantFunc, svc *Service) *Handler {
	return &Handler{getTenant: getTenant, svc: svc}
}

// GetTraceComparison handles GET /v1/traces/compare?traceA=...&traceB=...
func (h *Handler) GetTraceComparison(c *gin.Context) {
	teamID := h.getTenant(c).TeamID
	traceA := c.Query("traceA")
	traceB := c.Query("traceB")

	if traceA == "" || traceB == "" {
		common.RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "traceA and traceB query params are required")
		return
	}

	result, err := h.svc.Compare(teamID, traceA, traceB)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to compare traces: "+err.Error())
		return
	}
	common.RespondOK(c, result)
}
