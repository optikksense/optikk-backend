package workloads

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
	var req CreateWorkloadRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		common.RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "Invalid request: "+err.Error())
		return
	}

	workload, err := h.svc.Create(tenant.TeamID, tenant.UserID, req)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create workload")
		return
	}
	c.JSON(http.StatusCreated, workload)
}

func (h *Handler) List(c *gin.Context) {
	tenant := h.getTenant(c)
	workloads, err := h.svc.List(tenant.TeamID)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to list workloads")
		return
	}
	common.RespondOK(c, workloads)
}

func (h *Handler) GetByID(c *gin.Context) {
	tenant := h.getTenant(c)
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		common.RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "Invalid workload ID")
		return
	}

	workload, err := h.svc.Get(tenant.TeamID, id)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to get workload")
		return
	}
	if workload == nil {
		common.RespondError(c, http.StatusNotFound, "NOT_FOUND", "Workload not found")
		return
	}
	common.RespondOK(c, workload)
}

func (h *Handler) Update(c *gin.Context) {
	tenant := h.getTenant(c)
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		common.RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "Invalid workload ID")
		return
	}

	var req UpdateWorkloadRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		common.RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "Invalid request: "+err.Error())
		return
	}

	workload, err := h.svc.Update(tenant.TeamID, tenant.UserID, id, req)
	if err != nil {
		common.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
		return
	}
	common.RespondOK(c, workload)
}

func (h *Handler) Delete(c *gin.Context) {
	tenant := h.getTenant(c)
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		common.RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "Invalid workload ID")
		return
	}

	if err := h.svc.Delete(tenant.TeamID, tenant.UserID, id); err != nil {
		common.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
		return
	}
	common.RespondOK(c, map[string]string{"status": "deleted"})
}
