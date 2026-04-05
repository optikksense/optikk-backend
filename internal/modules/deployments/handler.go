package deployments

import (
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts/errorcode"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

// Handler serves deployment APIs.
type Handler struct {
	modulecommon.DBTenant
	Service Service
}

func parseServiceName(c *gin.Context) (string, bool) {
	name := c.Query("serviceName")
	if name == "" {
		name = c.Query("service")
	}
	if name == "" {
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.Validation, "serviceName is required", nil)
		return "", false
	}
	return name, true
}

// ListDeployments is GET /deployments/list.
func (h *Handler) ListDeployments(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	serviceName, ok := parseServiceName(c)
	if !ok {
		return
	}
	out, err := h.Service.ListDeployments(c.Request.Context(), teamID, serviceName, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "failed to list deployments", err)
		return
	}
	modulecommon.RespondOK(c, out)
}

// GetVersionTraffic is GET /deployments/timeline.
func (h *Handler) GetVersionTraffic(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	serviceName, ok := parseServiceName(c)
	if !ok {
		return
	}
	out, err := h.Service.GetVersionTraffic(c.Request.Context(), teamID, serviceName, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "failed to load version traffic", err)
		return
	}
	modulecommon.RespondOK(c, out)
}

// GetDeploymentImpact is GET /deployments/impact.
func (h *Handler) GetDeploymentImpact(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	serviceName, ok := parseServiceName(c)
	if !ok {
		return
	}
	out, err := h.Service.GetDeploymentImpact(c.Request.Context(), teamID, serviceName, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "failed to compute deployment impact", err)
		return
	}
	modulecommon.RespondOK(c, out)
}

// GetActiveVersion is GET /deployments/active-version.
func (h *Handler) GetActiveVersion(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	serviceName, ok := parseServiceName(c)
	if !ok {
		return
	}
	out, err := h.Service.GetActiveVersion(c.Request.Context(), teamID, serviceName, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "failed to resolve active version", err)
		return
	}
	modulecommon.RespondOK(c, out)
}
