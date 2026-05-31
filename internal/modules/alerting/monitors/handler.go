package monitors

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/shared/query"
	"github.com/Optikk-Org/optikk-backend/internal/shared/errorcode"
	httputil "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
)

// Handler shapes Gin handlers for the monitors module's HTTP routes.
// One handler method per route; none of them carry business logic — they parse
// + delegate + respond. Queries is the per-type CH backend registry used by
// the test, series, and (transitively) status-timeline routes.
type Handler struct {
	httputil.DBTenant
	Service *Service
	Queries query.Registry
}

func NewHandler(getTenant httputil.GetTenantFunc, service *Service, queries query.Registry) *Handler {
	return &Handler{
		DBTenant: httputil.DBTenant{GetTenant: getTenant},
		Service:  service,
		Queries:  queries,
	}
}

func (h *Handler) List(c *gin.Context) {
	tenant := h.GetTenant(c)
	q := ListQuery{
		Status:   c.Query("status"),
		Type:     c.Query("type"),
		Priority: c.Query("priority"),
		Search:   c.Query("q"),
		Limit:    httputil.ParseIntParam(c, "limit", 50),
		Offset:   httputil.ParseIntParam(c, "offset", 0),
	}
	if mv := c.Query("muted"); mv != "" {
		b := mv == "true" || mv == "1"
		q.Muted = &b
	}
	res, err := h.Service.List(c.Request.Context(), tenant.TeamID, q)
	if err != nil {
		httputil.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.QueryFailed, "failed to list monitors", err)
		return
	}
	httputil.RespondOK(c, res)
}

func (h *Handler) Get(c *gin.Context) {
	tenant := h.GetTenant(c)
	id, ok := parseIDParam(c)
	if !ok {
		return
	}
	res, err := h.Service.GetByID(c.Request.Context(), tenant.TeamID, id)
	if err != nil {
		respondServiceError(c, err)
		return
	}
	httputil.RespondOK(c, res)
}

func (h *Handler) Create(c *gin.Context) {
	tenant := h.GetTenant(c)
	var req CreateMonitorRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		httputil.RespondError(c, http.StatusBadRequest, errorcode.Validation, err.Error())
		return
	}
	res, err := h.Service.Create(c.Request.Context(), tenant.TeamID, tenant.UserID, req)
	if err != nil {
		respondServiceError(c, err)
		return
	}
	httputil.RespondOK(c, res)
}

func (h *Handler) Update(c *gin.Context) {
	tenant := h.GetTenant(c)
	id, ok := parseIDParam(c)
	if !ok {
		return
	}
	var req UpdateMonitorRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		httputil.RespondError(c, http.StatusBadRequest, errorcode.Validation, err.Error())
		return
	}
	res, err := h.Service.Update(c.Request.Context(), tenant.TeamID, tenant.UserID, id, req)
	if err != nil {
		respondServiceError(c, err)
		return
	}
	httputil.RespondOK(c, res)
}

func (h *Handler) Delete(c *gin.Context) {
	tenant := h.GetTenant(c)
	id, ok := parseIDParam(c)
	if !ok {
		return
	}
	if err := h.Service.Delete(c.Request.Context(), tenant.TeamID, id); err != nil {
		respondServiceError(c, err)
		return
	}
	httputil.RespondOK(c, gin.H{"deleted": id})
}

func parseIDParam(c *gin.Context) (int64, bool) {
	raw := c.Param("id")
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || id <= 0 {
		httputil.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "invalid monitor id")
		return 0, false
	}
	return id, true
}

func respondServiceError(c *gin.Context, err error) {
	var ve ErrValidation
	switch {
	case errors.Is(err, ErrNotFound):
		httputil.RespondError(c, http.StatusNotFound, errorcode.NotFound, "monitor not found")
	case errors.As(err, &ve):
		httputil.RespondError(c, http.StatusBadRequest, errorcode.Validation, ve.Msg)
	default:
		httputil.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "monitor request failed", err)
	}
}
