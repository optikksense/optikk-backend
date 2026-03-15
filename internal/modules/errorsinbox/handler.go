package errorsinbox

import (
	"net/http"
	"strconv"

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

func (h *Handler) GetStatus(c *gin.Context) {
	tenant := h.getTenant(c)
	groupID := c.Param("groupId")
	if groupID == "" {
		common.RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "groupId is required")
		return
	}

	status, err := h.svc.GetStatus(tenant.TeamID, groupID)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to get error group status")
		return
	}
	if status == nil {
		common.RespondOK(c, map[string]any{"groupId": groupID, "status": "new"})
		return
	}
	common.RespondOK(c, status)
}

func (h *Handler) ListByStatus(c *gin.Context) {
	tenant := h.getTenant(c)
	status := c.DefaultQuery("status", "new")
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "100"))

	items, err := h.svc.ListByStatus(tenant.TeamID, status, limit)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to list error groups")
		return
	}
	common.RespondOK(c, items)
}

func (h *Handler) UpdateStatus(c *gin.Context) {
	tenant := h.getTenant(c)
	groupID := c.Param("groupId")
	if groupID == "" {
		common.RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "groupId is required")
		return
	}

	var req UpdateStatusRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		common.RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "Invalid request: "+err.Error())
		return
	}

	result, err := h.svc.UpdateStatus(tenant.TeamID, tenant.UserID, groupID, req)
	if err != nil {
		common.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
		return
	}
	common.RespondOK(c, result)
}

func (h *Handler) BulkUpdate(c *gin.Context) {
	tenant := h.getTenant(c)
	var req BulkUpdateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		common.RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "Invalid request: "+err.Error())
		return
	}

	if err := h.svc.BulkUpdate(tenant.TeamID, tenant.UserID, req); err != nil {
		common.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
		return
	}
	common.RespondOK(c, map[string]string{"status": "updated"})
}

func (h *Handler) GetSummary(c *gin.Context) {
	tenant := h.getTenant(c)
	counts, err := h.svc.GetSummary(tenant.TeamID)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to get summary")
		return
	}
	common.RespondOK(c, counts)
}
