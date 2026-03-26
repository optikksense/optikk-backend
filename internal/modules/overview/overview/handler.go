package overview

import (
	"net/http"

	"github.com/observability/observability-backend-go/internal/contracts/errorcode"

	"github.com/gin-gonic/gin"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

type OverviewHandler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *OverviewHandler) GetRequestRate(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	serviceName := c.Query("serviceName")
	if serviceName == "" {
		serviceName = c.Query("service")
	}

	resp, err := modulecommon.WithComparison(c, startMs, endMs, func(s, e int64) (any, error) {
		return h.Service.GetRequestRate(c.Request.Context(), teamID, s, e, serviceName)
	})
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query overview request rate", err)
		return
	}

	modulecommon.RespondOK(c, resp)
}

func (h *OverviewHandler) GetErrorRate(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	serviceName := c.Query("serviceName")
	if serviceName == "" {
		serviceName = c.Query("service")
	}

	resp, err := modulecommon.WithComparison(c, startMs, endMs, func(s, e int64) (any, error) {
		return h.Service.GetErrorRate(c.Request.Context(), teamID, s, e, serviceName)
	})
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query overview error rate", err)
		return
	}

	modulecommon.RespondOK(c, resp)
}

func (h *OverviewHandler) GetP95Latency(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	serviceName := c.Query("serviceName")
	if serviceName == "" {
		serviceName = c.Query("service")
	}

	resp, err := modulecommon.WithComparison(c, startMs, endMs, func(s, e int64) (any, error) {
		return h.Service.GetP95Latency(c.Request.Context(), teamID, s, e, serviceName)
	})
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query overview p95 latency", err)
		return
	}

	modulecommon.RespondOK(c, resp)
}

func (h *OverviewHandler) GetServices(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	rows, err := h.Service.GetServices(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query overview services", err)
		return
	}

	modulecommon.RespondOK(c, rows)
}

func (h *OverviewHandler) GetTopEndpoints(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	serviceName := c.Query("serviceName")
	if serviceName == "" {
		serviceName = c.Query("service")
	}

	rows, err := h.Service.GetTopEndpoints(c.Request.Context(), teamID, startMs, endMs, serviceName)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query overview top endpoints", err)
		return
	}

	modulecommon.RespondOK(c, rows)
}

func (h *OverviewHandler) GetEndpointTimeSeries(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	serviceName := c.Query("serviceName")
	if serviceName == "" {
		serviceName = c.Query("service")
	}

	rows, err := h.Service.GetEndpointTimeSeries(c.Request.Context(), teamID, startMs, endMs, serviceName)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query overview endpoint time series", err)
		return
	}

	modulecommon.RespondOK(c, rows)
}
