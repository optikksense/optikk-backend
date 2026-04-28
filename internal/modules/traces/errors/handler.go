package errors

import (
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/filter"
	"github.com/Optikk-Org/optikk-backend/internal/shared/errorcode"
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
	h.runErrorGroups(c, "")
}

func (h *Handler) ServiceErrorGroups(c *gin.Context) {
	h.runErrorGroups(c, c.Param("serviceName"))
}

func (h *Handler) Timeseries(c *gin.Context) {
	h.runTimeseries(c, "")
}

func (h *Handler) ServiceTimeseries(c *gin.Context) {
	h.runTimeseries(c, c.Param("serviceName"))
}

func (h *Handler) runErrorGroups(c *gin.Context, serviceFromPath string) {
	var req ErrorGroupsRequest
	if err := c.ShouldBindQuery(&req); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Invalid query params")
		return
	}
	req.ServiceName = serviceFromPath
	filters, err := h.filtersFromGroups(c, req)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.Validation, "Invalid filters", err)
		return
	}
	resp, err := h.svc.ErrorGroups(c.Request.Context(), filters, req.Limit)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to fetch error groups", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) runTimeseries(c *gin.Context, serviceFromPath string) {
	var req TimeseriesRequest
	if err := c.ShouldBindQuery(&req); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Invalid query params")
		return
	}
	req.ServiceName = serviceFromPath
	filters, err := h.filtersFromTimeseries(c, req)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.Validation, "Invalid filters", err)
		return
	}
	resp, err := h.svc.Timeseries(c.Request.Context(), filters)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to fetch error timeseries", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) filtersFromGroups(c *gin.Context, req ErrorGroupsRequest) (filter.Filters, error) {
	f := filter.Filters{
		TeamID:   h.GetTenant(c).TeamID,
		StartMs:  req.StartMs,
		EndMs:    req.EndMs,
		Services: append([]string{}, req.Services...),
	}
	if req.ServiceName != "" {
		f.Services = append(f.Services, req.ServiceName)
	}
	return f, f.Validate()
}

func (h *Handler) filtersFromTimeseries(c *gin.Context, req TimeseriesRequest) (filter.Filters, error) {
	f := filter.Filters{
		TeamID:  h.GetTenant(c).TeamID,
		StartMs: req.StartMs,
		EndMs:   req.EndMs,
	}
	if req.ServiceName != "" {
		f.Services = []string{req.ServiceName}
	}
	return f, f.Validate()
}
