package overview

import (
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts/errorcode"

	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
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

	rows, err := h.Service.GetTopEndpoints(c.Request.Context(), teamID, startMs, endMs, serviceName)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query overview top endpoints", err)
		return
	}

	modulecommon.RespondOK(c, rows)
}

func (h *OverviewHandler) GetSummary(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	summary, err := h.Service.GetSummary(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query overview summary", err)
		return
	}

	modulecommon.RespondOK(c, summary)
}

func (h *OverviewHandler) GetBatchSummary(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	batch, err := h.Service.GetBatchSummary(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query overview batch summary", err)
		return
	}

	modulecommon.RespondOK(c, batch)
}
