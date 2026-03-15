package dashboards

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
	var req CreateDashboardRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		common.RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "Invalid request: "+err.Error())
		return
	}
	dash, err := h.svc.CreateDashboard(tenant.TeamID, tenant.UserID, req)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create dashboard")
		return
	}
	c.JSON(http.StatusCreated, dash)
}

func (h *Handler) List(c *gin.Context) {
	tenant := h.getTenant(c)
	dashboards, err := h.svc.ListDashboards(tenant.TeamID, tenant.UserID)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to list dashboards")
		return
	}
	common.RespondOK(c, dashboards)
}

func (h *Handler) GetByID(c *gin.Context) {
	tenant := h.getTenant(c)
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		common.RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "Invalid dashboard ID")
		return
	}
	dash, err := h.svc.GetDashboard(tenant.TeamID, id)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to get dashboard")
		return
	}
	if dash == nil {
		common.RespondError(c, http.StatusNotFound, "NOT_FOUND", "Dashboard not found")
		return
	}
	common.RespondOK(c, dash)
}

func (h *Handler) Update(c *gin.Context) {
	tenant := h.getTenant(c)
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		common.RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "Invalid dashboard ID")
		return
	}
	var req UpdateDashboardRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		common.RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "Invalid request: "+err.Error())
		return
	}
	dash, err := h.svc.UpdateDashboard(tenant.TeamID, tenant.UserID, id, req)
	if err != nil {
		common.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
		return
	}
	common.RespondOK(c, dash)
}

func (h *Handler) Delete(c *gin.Context) {
	tenant := h.getTenant(c)
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		common.RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "Invalid dashboard ID")
		return
	}
	if err := h.svc.DeleteDashboard(tenant.TeamID, tenant.UserID, id); err != nil {
		common.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
		return
	}
	common.RespondOK(c, map[string]string{"status": "deleted"})
}

func (h *Handler) AddWidget(c *gin.Context) {
	tenant := h.getTenant(c)
	dashID, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		common.RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "Invalid dashboard ID")
		return
	}
	var req CreateWidgetRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		common.RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "Invalid request: "+err.Error())
		return
	}
	widget, err := h.svc.AddWidget(tenant.TeamID, dashID, req)
	if err != nil {
		common.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
		return
	}
	c.JSON(http.StatusCreated, widget)
}

func (h *Handler) UpdateWidget(c *gin.Context) {
	tenant := h.getTenant(c)
	widgetID, err := strconv.ParseInt(c.Param("widgetId"), 10, 64)
	if err != nil {
		common.RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "Invalid widget ID")
		return
	}
	var req UpdateWidgetRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		common.RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "Invalid request: "+err.Error())
		return
	}
	widget, err := h.svc.UpdateWidget(tenant.TeamID, widgetID, req)
	if err != nil {
		common.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
		return
	}
	common.RespondOK(c, widget)
}

func (h *Handler) DeleteWidget(c *gin.Context) {
	tenant := h.getTenant(c)
	widgetID, err := strconv.ParseInt(c.Param("widgetId"), 10, 64)
	if err != nil {
		common.RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "Invalid widget ID")
		return
	}
	if err := h.svc.DeleteWidget(tenant.TeamID, widgetID); err != nil {
		common.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
		return
	}
	common.RespondOK(c, map[string]string{"status": "deleted"})
}

func (h *Handler) Duplicate(c *gin.Context) {
	tenant := h.getTenant(c)
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		common.RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "Invalid dashboard ID")
		return
	}
	dash, err := h.svc.DuplicateDashboard(tenant.TeamID, tenant.UserID, id)
	if err != nil {
		common.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
		return
	}
	c.JSON(http.StatusCreated, dash)
}
