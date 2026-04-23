package errors

import (
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts/errorcode"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type Handler struct {
	modulecommon.DBTenant
	svc *Service
}

func NewHandler(getTenant modulecommon.GetTenantFunc, svc *Service) *Handler {
	return &Handler{DBTenant: modulecommon.DBTenant{GetTenant: getTenant}, svc: svc}
}

func (h *Handler) ErrorGroups(c *gin.Context) {
	var req ErrorGroupsRequest
	if err := c.ShouldBindQuery(&req); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Invalid query params")
		return
	}
	resp, err := h.svc.ErrorGroups(c.Request.Context(), h.GetTenant(c).TeamID, req)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to fetch error groups", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) ServiceErrorGroups(c *gin.Context) {
	var req ErrorGroupsRequest
	if err := c.ShouldBindQuery(&req); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Invalid query params")
		return
	}
	req.ServiceName = c.Param("serviceName")
	resp, err := h.svc.ErrorGroups(c.Request.Context(), h.GetTenant(c).TeamID, req)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to fetch service error groups", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) Timeseries(c *gin.Context) {
	var req TimeseriesRequest
	if err := c.ShouldBindQuery(&req); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Invalid query params")
		return
	}
	resp, err := h.svc.Timeseries(c.Request.Context(), h.GetTenant(c).TeamID, req)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to fetch error timeseries", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) ServiceTimeseries(c *gin.Context) {
	var req TimeseriesRequest
	if err := c.ShouldBindQuery(&req); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Invalid query params")
		return
	}
	req.ServiceName = c.Param("serviceName")
	resp, err := h.svc.Timeseries(c.Request.Context(), h.GetTenant(c).TeamID, req)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to fetch service error timeseries", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}
