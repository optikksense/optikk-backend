package slowqueries

import (
	"net/http"

	"github.com/observability/observability-backend-go/internal/contracts/errorcode"

	"github.com/gin-gonic/gin"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
	shared "github.com/observability/observability-backend-go/internal/modules/database/internal/shared"
)

type Handler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *Handler) GetSlowQueryPatterns(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetSlowQueryPatterns(c.Request.Context(), teamID, startMs, endMs, shared.ParseFilters(c), shared.ParseLimit(c, 20))
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query slow query patterns", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetSlowestCollections(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetSlowestCollections(c.Request.Context(), teamID, startMs, endMs, shared.ParseFilters(c))
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query slowest collections", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetSlowQueryRate(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetSlowQueryRate(c.Request.Context(), teamID, startMs, endMs, shared.ParseFilters(c), shared.ParseThreshold(c, 100))
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query slow query rate", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetP99ByQueryText(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetP99ByQueryText(c.Request.Context(), teamID, startMs, endMs, shared.ParseFilters(c), shared.ParseLimit(c, 10))
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query p99 by query text", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}
