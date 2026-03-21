package tracecompare

import (
	"net/http"

	"github.com/observability/observability-backend-go/internal/contracts/errorcode"

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
		common.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "traceA and traceB query params are required")
		return
	}

	result, err := h.svc.Compare(teamID, traceA, traceB)
	if err != nil {
		common.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to compare traces: "+err.Error(), err)
		return
	}
	common.RespondOK(c, result)
}
