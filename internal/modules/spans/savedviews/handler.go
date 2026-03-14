package savedviews

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

func (h *Handler) Create(c *gin.Context) {
	tenant := h.getTenant(c)
	var req CreateViewRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		common.RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "Invalid request: "+err.Error())
		return
	}

	view, err := h.svc.Create(tenant.TeamID, tenant.UserID, req)
	if err != nil {
		common.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
		return
	}
	c.JSON(http.StatusCreated, view)
}

func (h *Handler) List(c *gin.Context) {
	tenant := h.getTenant(c)
	views, err := h.svc.List(tenant.TeamID, tenant.UserID)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to list saved views")
		return
	}
	common.RespondOK(c, views)
}

func (h *Handler) GetByID(c *gin.Context) {
	tenant := h.getTenant(c)
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		common.RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "Invalid view ID")
		return
	}

	view, err := h.svc.GetByID(tenant.TeamID, id)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to get saved view")
		return
	}
	if view == nil {
		common.RespondError(c, http.StatusNotFound, "NOT_FOUND", "Saved view not found")
		return
	}
	common.RespondOK(c, view)
}

func (h *Handler) Update(c *gin.Context) {
	tenant := h.getTenant(c)
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		common.RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "Invalid view ID")
		return
	}

	var req UpdateViewRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		common.RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "Invalid request: "+err.Error())
		return
	}

	view, err := h.svc.Update(tenant.TeamID, tenant.UserID, id, req)
	if err != nil {
		common.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
		return
	}
	common.RespondOK(c, view)
}

func (h *Handler) Delete(c *gin.Context) {
	tenant := h.getTenant(c)
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		common.RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "Invalid view ID")
		return
	}

	if err := h.svc.Delete(tenant.TeamID, tenant.UserID, id); err != nil {
		common.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
		return
	}
	common.RespondOK(c, map[string]string{"status": "deleted"})
}
