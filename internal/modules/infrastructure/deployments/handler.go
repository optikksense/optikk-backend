package deployments

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	types "github.com/observability/observability-backend-go/internal/contracts"
	. "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

// DeploymentHandler handles deployment tracking API endpoints.
type DeploymentHandler struct {
	modulecommon.DBTenant
	Service Service
}

// GetDeployments — paginated list of deployments with optional filters.
func (h *DeploymentHandler) GetDeployments(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 7*24*60*60*1000)
	serviceName := c.Query("serviceName")
	environment := c.Query("environment")
	limit := ParseIntParam(c, "limit", 50)
	offset := ParseIntParam(c, "offset", 0)

	rows, total, err := h.Service.GetDeployments(teamUUID, startMs, endMs, serviceName, environment, limit, offset)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to load deployments")
		return
	}

	RespondOK(c, map[string]any{
		"deployments": rows,
		"total":       total,
		"limit":       limit,
		"offset":      offset,
	})
}

// GetDeploymentEvents — lightweight event list for timeline overlays.
func (h *DeploymentHandler) GetDeploymentEvents(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)
	serviceName := c.Query("serviceName")

	rows, err := h.Service.GetDeploymentEvents(teamUUID, startMs, endMs, serviceName)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to load deploy events")
		return
	}
	RespondOK(c, rows)
}

// GetDeploymentDiff — before/after performance comparison around a deploy.
func (h *DeploymentHandler) GetDeploymentDiff(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	deployID := c.Param("deployId")
	windowMinutes := ParseIntParam(c, "windowMinutes", 30)

	diff, err := h.Service.GetDeploymentDiff(teamUUID, deployID, windowMinutes)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to calculate deployment diff")
		return
	}
	if diff == nil {
		RespondOK(c, map[string]any{})
		return
	}

	RespondOK(c, diff)
}

// CreateDeployment — record a new deployment event.
func (h *DeploymentHandler) CreateDeployment(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	var req types.DeploymentCreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid payload")
		return
	}
	deployID := uuid.NewString()

	err := h.Service.CreateDeployment(teamUUID, deployID, req)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create deployment")
		return
	}

	dur := 0
	if req.DurationSeconds != nil {
		dur = *req.DurationSeconds
	}

	RespondOK(c, map[string]any{
		"team_id":          teamUUID,
		"deploy_id":        deployID,
		"service_name":     req.ServiceName,
		"version":          req.Version,
		"environment":      DefaultString(req.Environment, "production"),
		"deployed_by":      DefaultString(req.DeployedBy, ""),
		"status":           DefaultString(req.Status, "success"),
		"commit_sha":       req.CommitSHA,
		"duration_seconds": dur,
		"attributes":       req.Attributes,
		"message":          "Deployment recorded",
	})
}
