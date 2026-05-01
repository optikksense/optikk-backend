package trace_logs

import (
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/shared/errorcode"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

const (
	defaultLimit = 1000
	maxLimit     = 5000
)

type Handler struct {
	modulecommon.DBTenant
	svc *Service
}

func NewHandler(getTenant modulecommon.GetTenantFunc, svc *Service) *Handler {
	return &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		svc:      svc,
	}
}

// GetByTrace powers GET /api/v1/logs/trace/:traceID — all logs for a trace.
func (h *Handler) GetByTrace(c *gin.Context) {
	traceID := c.Param("traceID")
	if traceID == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "trace id required")
		return
	}
	limit := modulecommon.ParseIntParam(c, "limit", defaultLimit)
	if limit <= 0 {
		limit = defaultLimit
	}
	if limit > maxLimit {
		limit = maxLimit
	}
	logs, err := h.svc.GetByTraceID(c.Request.Context(), h.GetTenant(c).TeamID, traceID, limit)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to fetch logs by trace", err)
		return
	}
	modulecommon.RespondOK(c, logs)
}
