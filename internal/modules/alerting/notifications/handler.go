package notifications

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/Optikk-Org/optikk-backend/internal/shared/errorcode"
	httputil "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
)

// Handler shapes Gin handlers for the notifications module's HTTP routes.
type Handler struct {
	httputil.DBTenant
	Service *Service
}

func NewHandler(getTenant httputil.GetTenantFunc, service *Service) *Handler {
	return &Handler{
		DBTenant: httputil.DBTenant{GetTenant: getTenant},
		Service:  service,
	}
}

// Channels ------------------------------------------------------------------

func (h *Handler) ListChannels(c *gin.Context) {
	t := h.GetTenant(c)
	res, err := h.Service.ListChannels(c.Request.Context(), t.TeamID)
	if err != nil {
		respondServiceError(c, err)
		return
	}
	httputil.RespondOK(c, res)
}

func (h *Handler) GetChannel(c *gin.Context) {
	t := h.GetTenant(c)
	id, ok := parseIDParam(c)
	if !ok {
		return
	}
	res, err := h.Service.GetChannel(c.Request.Context(), t.TeamID, id)
	if err != nil {
		respondServiceError(c, err)
		return
	}
	httputil.RespondOK(c, res)
}

func (h *Handler) CreateChannel(c *gin.Context) {
	t := h.GetTenant(c)
	var req CreateChannelRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		httputil.RespondError(c, http.StatusBadRequest, errorcode.Validation, err.Error())
		return
	}
	res, err := h.Service.CreateChannel(c.Request.Context(), t.TeamID, req)
	if err != nil {
		respondServiceError(c, err)
		return
	}
	httputil.RespondOK(c, res)
}

func (h *Handler) UpdateChannel(c *gin.Context) {
	t := h.GetTenant(c)
	id, ok := parseIDParam(c)
	if !ok {
		return
	}
	var req UpdateChannelRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		httputil.RespondError(c, http.StatusBadRequest, errorcode.Validation, err.Error())
		return
	}
	res, err := h.Service.UpdateChannel(c.Request.Context(), t.TeamID, id, req)
	if err != nil {
		respondServiceError(c, err)
		return
	}
	httputil.RespondOK(c, res)
}

func (h *Handler) DeleteChannel(c *gin.Context) {
	t := h.GetTenant(c)
	id, ok := parseIDParam(c)
	if !ok {
		return
	}
	if err := h.Service.DeleteChannel(c.Request.Context(), t.TeamID, id); err != nil {
		respondServiceError(c, err)
		return
	}
	httputil.RespondOK(c, gin.H{"deleted": id})
}

func (h *Handler) TestChannel(c *gin.Context) {
	t := h.GetTenant(c)
	id, ok := parseIDParam(c)
	if !ok {
		return
	}
	res, err := h.Service.TestChannel(c.Request.Context(), t.TeamID, id)
	if err != nil {
		respondServiceError(c, err)
		return
	}
	httputil.RespondOK(c, res)
}

// Policies ------------------------------------------------------------------

func (h *Handler) ListPolicies(c *gin.Context) {
	t := h.GetTenant(c)
	res, err := h.Service.ListPolicies(c.Request.Context(), t.TeamID)
	if err != nil {
		respondServiceError(c, err)
		return
	}
	httputil.RespondOK(c, res)
}

func (h *Handler) CreatePolicy(c *gin.Context) {
	t := h.GetTenant(c)
	var req CreatePolicyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		httputil.RespondError(c, http.StatusBadRequest, errorcode.Validation, err.Error())
		return
	}
	res, err := h.Service.CreatePolicy(c.Request.Context(), t.TeamID, req)
	if err != nil {
		respondServiceError(c, err)
		return
	}
	httputil.RespondOK(c, res)
}

func (h *Handler) UpdatePolicy(c *gin.Context) {
	t := h.GetTenant(c)
	id, ok := parseIDParam(c)
	if !ok {
		return
	}
	var req UpdatePolicyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		httputil.RespondError(c, http.StatusBadRequest, errorcode.Validation, err.Error())
		return
	}
	res, err := h.Service.UpdatePolicy(c.Request.Context(), t.TeamID, id, req)
	if err != nil {
		respondServiceError(c, err)
		return
	}
	httputil.RespondOK(c, res)
}

func (h *Handler) DeletePolicy(c *gin.Context) {
	t := h.GetTenant(c)
	id, ok := parseIDParam(c)
	if !ok {
		return
	}
	if err := h.Service.DeletePolicy(c.Request.Context(), t.TeamID, id); err != nil {
		respondServiceError(c, err)
		return
	}
	httputil.RespondOK(c, gin.H{"deleted": id})
}

// Templates -----------------------------------------------------------------

func (h *Handler) ListTemplates(c *gin.Context) {
	t := h.GetTenant(c)
	res, err := h.Service.ListTemplates(c.Request.Context(), t.TeamID)
	if err != nil {
		respondServiceError(c, err)
		return
	}
	httputil.RespondOK(c, res)
}

func (h *Handler) CreateTemplate(c *gin.Context) {
	t := h.GetTenant(c)
	var req CreateTemplateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		httputil.RespondError(c, http.StatusBadRequest, errorcode.Validation, err.Error())
		return
	}
	res, err := h.Service.CreateTemplate(c.Request.Context(), t.TeamID, req)
	if err != nil {
		respondServiceError(c, err)
		return
	}
	httputil.RespondOK(c, res)
}

func (h *Handler) UpdateTemplate(c *gin.Context) {
	t := h.GetTenant(c)
	id, ok := parseIDParam(c)
	if !ok {
		return
	}
	var req UpdateTemplateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		httputil.RespondError(c, http.StatusBadRequest, errorcode.Validation, err.Error())
		return
	}
	res, err := h.Service.UpdateTemplate(c.Request.Context(), t.TeamID, id, req)
	if err != nil {
		respondServiceError(c, err)
		return
	}
	httputil.RespondOK(c, res)
}

func (h *Handler) DeleteTemplate(c *gin.Context) {
	t := h.GetTenant(c)
	id, ok := parseIDParam(c)
	if !ok {
		return
	}
	if err := h.Service.DeleteTemplate(c.Request.Context(), t.TeamID, id); err != nil {
		respondServiceError(c, err)
		return
	}
	httputil.RespondOK(c, gin.H{"deleted": id})
}

// Integrations --------------------------------------------------------------

func (h *Handler) ListIntegrations(c *gin.Context) {
	t := h.GetTenant(c)
	res, err := h.Service.ListIntegrations(c.Request.Context(), t.TeamID)
	if err != nil {
		respondServiceError(c, err)
		return
	}
	httputil.RespondOK(c, res)
}

// Shared helpers ------------------------------------------------------------

func parseIDParam(c *gin.Context) (int64, bool) {
	raw := c.Param("id")
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || id <= 0 {
		httputil.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "invalid id")
		return 0, false
	}
	return id, true
}

func respondServiceError(c *gin.Context, err error) {
	var ve ErrValidation
	switch {
	case errors.Is(err, ErrNotFound):
		httputil.RespondError(c, http.StatusNotFound, errorcode.NotFound, "resource not found")
	case errors.Is(err, ErrChannelInUse):
		httputil.RespondError(c, http.StatusConflict, errorcode.Conflict, "channel is in use by one or more monitors")
	case errors.As(err, &ve):
		httputil.RespondError(c, http.StatusBadRequest, errorcode.Validation, ve.Msg)
	default:
		httputil.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "request failed", err)
	}
}
