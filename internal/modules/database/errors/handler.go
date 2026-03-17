package errors

import (
	"net/http"

	"github.com/gin-gonic/gin"
	common "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
	shared "github.com/observability/observability-backend-go/internal/modules/database/internal/shared"
)

type Handler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *Handler) GetErrorsBySystem(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetErrorsBySystem(c.Request.Context(), teamID, startMs, endMs, shared.ParseFilters(c))
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query errors by system")
		return
	}
	common.RespondOK(c, resp)
}

func (h *Handler) GetErrorsByOperation(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetErrorsByOperation(c.Request.Context(), teamID, startMs, endMs, shared.ParseFilters(c))
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query errors by operation")
		return
	}
	common.RespondOK(c, resp)
}

func (h *Handler) GetErrorsByErrorType(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetErrorsByErrorType(c.Request.Context(), teamID, startMs, endMs, shared.ParseFilters(c))
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query errors by error type")
		return
	}
	common.RespondOK(c, resp)
}

func (h *Handler) GetErrorsByCollection(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetErrorsByCollection(c.Request.Context(), teamID, startMs, endMs, shared.ParseFilters(c))
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query errors by collection")
		return
	}
	common.RespondOK(c, resp)
}

func (h *Handler) GetErrorsByResponseStatus(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetErrorsByResponseStatus(c.Request.Context(), teamID, startMs, endMs, shared.ParseFilters(c))
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query errors by response status")
		return
	}
	common.RespondOK(c, resp)
}

func (h *Handler) GetErrorRatio(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetErrorRatio(c.Request.Context(), teamID, startMs, endMs, shared.ParseFilters(c))
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query error ratio")
		return
	}
	common.RespondOK(c, resp)
}
