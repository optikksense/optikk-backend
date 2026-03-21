package latency

import (
	"net/http"

	"github.com/observability/observability-backend-go/internal/contracts/errorcode"

	"github.com/gin-gonic/gin"
	common "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
	shared "github.com/observability/observability-backend-go/internal/modules/database/internal/shared"
)

type Handler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *Handler) GetLatencyBySystem(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetLatencyBySystem(c.Request.Context(), teamID, startMs, endMs, shared.ParseFilters(c))
	if err != nil {
		common.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query latency by system", err)
		return
	}
	common.RespondOK(c, resp)
}

func (h *Handler) GetLatencyByOperation(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetLatencyByOperation(c.Request.Context(), teamID, startMs, endMs, shared.ParseFilters(c))
	if err != nil {
		common.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query latency by operation", err)
		return
	}
	common.RespondOK(c, resp)
}

func (h *Handler) GetLatencyByCollection(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetLatencyByCollection(c.Request.Context(), teamID, startMs, endMs, shared.ParseFilters(c))
	if err != nil {
		common.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query latency by collection", err)
		return
	}
	common.RespondOK(c, resp)
}

func (h *Handler) GetLatencyByNamespace(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetLatencyByNamespace(c.Request.Context(), teamID, startMs, endMs, shared.ParseFilters(c))
	if err != nil {
		common.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query latency by namespace", err)
		return
	}
	common.RespondOK(c, resp)
}

func (h *Handler) GetLatencyByServer(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetLatencyByServer(c.Request.Context(), teamID, startMs, endMs, shared.ParseFilters(c))
	if err != nil {
		common.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query latency by server", err)
		return
	}
	common.RespondOK(c, resp)
}

func (h *Handler) GetLatencyHeatmap(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetLatencyHeatmap(c.Request.Context(), teamID, startMs, endMs, shared.ParseFilters(c))
	if err != nil {
		common.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query latency heatmap", err)
		return
	}
	common.RespondOK(c, resp)
}
