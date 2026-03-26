package tracecompare

import (
	"net/http"

	"github.com/observability/observability-backend-go/internal/contracts/errorcode"

	"github.com/gin-gonic/gin"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

type Handler struct {
	getTenant modulecommon.GetTenantFunc
	svc       *Service
}

func NewHandler(getTenant modulecommon.GetTenantFunc, svc *Service) *Handler {
	return &Handler{getTenant: getTenant, svc: svc}
}

// GetTraceComparison handles GET /v1/traces/compare?traceA=...&traceB=...
func (h *Handler) GetTraceComparison(c *gin.Context) {
	teamID := h.getTenant(c).TeamID
	traceA := c.Query("traceA")
	traceB := c.Query("traceB")

	if traceA == "" || traceB == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "traceA and traceB query params are required")
		return
	}

	result, err := h.svc.Compare(teamID, traceA, traceB)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to compare traces: "+err.Error(), err)
		return
	}
	modulecommon.RespondOK(c, result)
}
