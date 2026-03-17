package team

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
	usershared "github.com/observability/observability-backend-go/internal/modules/user/internal/shared"
	appvalidation "github.com/observability/observability-backend-go/internal/platform/validation"
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

func (h *Handler) GetTeams(c *gin.Context) {
	teams, err := h.Service.GetTeams(h.GetTenant(c).TeamID)
	if err != nil {
		usershared.RespondServiceError(c, err, "Failed to load teams")
		return
	}
	modulecommon.RespondOK(c, teams)
}

func (h *Handler) GetMyTeams(c *gin.Context) {
	teams, err := h.Service.GetMyTeams(h.GetTenant(c).UserID)
	if err != nil {
		usershared.RespondServiceError(c, err, "Failed to load teams")
		return
	}
	modulecommon.RespondOK(c, teams)
}

func (h *Handler) GetTeamByID(c *gin.Context) {
	teamID, err := modulecommon.ExtractIDParam(c, "id")
	if err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid team id")
		return
	}

	team, err := h.Service.GetTeamByID(teamID)
	if err != nil {
		usershared.RespondServiceError(c, err, "Team not found")
		return
	}
	modulecommon.RespondOK(c, team)
}

func (h *Handler) GetTeamBySlug(c *gin.Context) {
	team, err := h.Service.GetTeamBySlug(h.GetTenant(c).TeamID, c.Param("slug"))
	if err != nil {
		usershared.RespondServiceError(c, err, "Team not found")
		return
	}
	modulecommon.RespondOK(c, team)
}

func (h *Handler) CreateTeam(c *gin.Context) {
	var req CreateTeamRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	req.TeamName = strings.TrimSpace(req.TeamName)
	req.OrgName = strings.TrimSpace(req.OrgName)
	if err := appvalidation.Struct(req); err != nil {
		usershared.RespondServiceError(c, usershared.NewValidationError("team name and organization name are required", nil), "Unable to create team")
		return
	}

	team, err := h.Service.CreateTeam(req)
	if err != nil {
		usershared.RespondServiceError(c, err, "Unable to create team")
		return
	}
	modulecommon.RespondOK(c, team)
}

func (h *Handler) AddUserToTeam(c *gin.Context) {
	userID, err := modulecommon.ExtractIDParam(c, "userId")
	if err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid userId")
		return
	}
	teamID, err := modulecommon.ExtractIDParam(c, "teamId")
	if err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid teamId")
		return
	}

	role := strings.TrimSpace(c.Query("role"))
	if role == "" {
		role = "member"
	}

	if err := h.Service.AddUserToTeam(userID, teamID, role); err != nil {
		usershared.RespondServiceError(c, err, "Unable to add user to team")
		return
	}
	modulecommon.RespondOK(c, nil)
}

func (h *Handler) RemoveUserFromTeam(c *gin.Context) {
	userID, err := modulecommon.ExtractIDParam(c, "userId")
	if err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid userId")
		return
	}
	teamID, err := modulecommon.ExtractIDParam(c, "teamId")
	if err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid teamId")
		return
	}

	if err := h.Service.RemoveUserFromTeam(userID, teamID); err != nil {
		usershared.RespondServiceError(c, err, "Unable to remove user from team")
		return
	}
	modulecommon.RespondOK(c, nil)
}
