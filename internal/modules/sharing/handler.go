package sharing

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

func (h *Handler) CreateLink(c *gin.Context) {
	tenant := h.getTenant(c)
	var req CreateShareRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		common.RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "Invalid request: "+err.Error())
		return
	}

	link, err := h.svc.CreateLink(tenant.TeamID, tenant.UserID, req)
	if err != nil {
		common.RespondErrorWithCause(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create shared link", err)
		return
	}
	c.JSON(http.StatusCreated, link)
}

func (h *Handler) ListLinks(c *gin.Context) {
	tenant := h.getTenant(c)
	links, err := h.svc.ListLinks(tenant.TeamID)
	if err != nil {
		common.RespondErrorWithCause(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to list shared links", err)
		return
	}
	common.RespondOK(c, links)
}

func (h *Handler) ResolveLink(c *gin.Context) {
	token := c.Param("token")
	if token == "" {
		common.RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "Token is required")
		return
	}

	link, err := h.svc.ResolveLink(token)
	if err != nil {
		common.RespondError(c, http.StatusNotFound, "NOT_FOUND", err.Error())
		return
	}
	common.RespondOK(c, link)
}

func (h *Handler) DeleteLink(c *gin.Context) {
	tenant := h.getTenant(c)
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		common.RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "Invalid link ID")
		return
	}

	if err := h.svc.DeleteLink(tenant.TeamID, tenant.UserID, id); err != nil {
		common.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
		return
	}
	common.RespondOK(c, map[string]string{"status": "deleted"})
}

func (h *Handler) ExportCSV(c *gin.Context) {
	var req ExportRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		common.RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "Invalid request: "+err.Error())
		return
	}

	data, err := h.svc.ExportCSV(req.Columns, req.Data)
	if err != nil {
		common.RespondErrorWithCause(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to export CSV", err)
		return
	}

	c.Header("Content-Disposition", "attachment; filename=export.csv")
	c.Data(http.StatusOK, "text/csv", data)
}
