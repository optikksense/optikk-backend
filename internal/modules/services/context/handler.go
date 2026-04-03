package servicecontext

import (
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts/errorcode"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type Handler struct {
	modulecommon.DBTenant
	Service *Service
}

func NewHandler(getTenant modulecommon.GetTenantFunc, service *Service) *Handler {
	return &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  service,
	}
}

func (h *Handler) GetServicesCatalog(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	rows, err := h.Service.GetServicesCatalog(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to load services catalog", err)
		return
	}
	modulecommon.RespondOK(c, rows)
}

func (h *Handler) GetServiceCatalog(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	serviceName := c.Param("serviceName")
	row, err := h.Service.GetServiceCatalog(c.Request.Context(), teamID, startMs, endMs, serviceName)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to load service catalog", err)
		return
	}
	modulecommon.RespondOK(c, row)
}

func (h *Handler) GetServiceOwnership(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	serviceName := c.Param("serviceName")
	row, err := h.Service.GetServiceOwnership(c.Request.Context(), teamID, startMs, endMs, serviceName)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to load service ownership", err)
		return
	}
	modulecommon.RespondOK(c, row)
}

func (h *Handler) GetServiceMonitors(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	serviceName := c.Param("serviceName")
	row, err := h.Service.GetServiceMonitors(c.Request.Context(), teamID, startMs, endMs, serviceName)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to load service monitors", err)
		return
	}
	modulecommon.RespondOK(c, row)
}

func (h *Handler) GetServiceDeployments(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	rows, err := h.Service.GetServiceDeployments(teamID, c.Param("serviceName"))
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to load service deployments", err)
		return
	}
	modulecommon.RespondOK(c, rows)
}

func (h *Handler) GetServiceIncidents(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	rows, err := h.Service.GetServiceIncidents(teamID, c.Param("serviceName"))
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to load service incidents", err)
		return
	}
	modulecommon.RespondOK(c, rows)
}

func (h *Handler) GetServiceChangeEvents(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	rows, err := h.Service.GetServiceChangeEvents(teamID, c.Param("serviceName"))
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to load service change events", err)
		return
	}
	modulecommon.RespondOK(c, rows)
}

func (h *Handler) GetServiceScorecard(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	serviceName := c.Param("serviceName")
	row, err := h.Service.GetServiceScorecard(c.Request.Context(), teamID, startMs, endMs, serviceName)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to load service scorecard", err)
		return
	}
	modulecommon.RespondOK(c, row)
}
