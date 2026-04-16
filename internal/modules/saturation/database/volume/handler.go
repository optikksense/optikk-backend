package volume

import (
	"net/http"

	errorcode "github.com/Optikk-Org/optikk-backend/internal/shared/contracts"

	shared "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/internal/shared"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type Handler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *Handler) GetOpsBySystem(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetOpsBySystem(c.Request.Context(), teamID, startMs, endMs, shared.ParseFilters(c))
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query ops by system", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetOpsByOperation(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetOpsByOperation(c.Request.Context(), teamID, startMs, endMs, shared.ParseFilters(c))
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query ops by operation", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetOpsByCollection(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetOpsByCollection(c.Request.Context(), teamID, startMs, endMs, shared.ParseFilters(c))
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query ops by collection", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetReadVsWrite(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetReadVsWrite(c.Request.Context(), teamID, startMs, endMs, shared.ParseFilters(c))
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query read vs write ratio", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetOpsByNamespace(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetOpsByNamespace(c.Request.Context(), teamID, startMs, endMs, shared.ParseFilters(c))
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query ops by namespace", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}
