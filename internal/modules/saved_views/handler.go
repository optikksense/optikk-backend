package savedviews

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/Optikk-Org/optikk-backend/internal/shared/errorcode"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
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

func (h *Handler) List(c *gin.Context) {
	tenant := h.GetTenant(c)
	scope := c.Query("scope")
	views, err := h.svc.List(c.Request.Context(), tenant.TeamID, scope)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to list saved views", err)
		return
	}
	modulecommon.RespondOK(c, views)
}

func (h *Handler) Create(c *gin.Context) {
	var req CreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Invalid request body")
		return
	}
	tenant := h.GetTenant(c)
	view, err := h.svc.Create(c.Request.Context(), tenant.TeamID, tenant.UserID, req)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.Validation, "Unable to create saved view", err)
		return
	}
	modulecommon.RespondOK(c, view)
}

func (h *Handler) Delete(c *gin.Context) {
	id, err := modulecommon.ExtractIDParam(c, "id")
	if err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Invalid saved view id")
		return
	}
	tenant := h.GetTenant(c)
	if err := h.svc.Delete(c.Request.Context(), tenant.TeamID, tenant.UserID, id); err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusNotFound, errorcode.NotFound, "Saved view not found", err)
		return
	}
	modulecommon.RespondOK(c, gin.H{"deleted": id})
}
