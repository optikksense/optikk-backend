package identity

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	types "github.com/observability/observability-backend-go/internal/contracts"
	identityservice "github.com/observability/observability-backend-go/modules/user/service"
	apphandlers "github.com/observability/observability-backend-go/internal/platform/handlers"
)

// UserHandler handles user/team/profile endpoints for the identity module.
type UserHandler struct {
	service   identityservice.UserService
	getTenant apphandlers.GetTenantFunc
}

func NewUserHandler(getTenant apphandlers.GetTenantFunc, service identityservice.UserService) *UserHandler {
	return &UserHandler{
		service:   service,
		getTenant: getTenant,
	}
}

func (h *UserHandler) GetCurrentUser(c *gin.Context) {
	user, err := h.service.GetCurrentUser(h.getTenant(c).UserID)
	if err != nil {
		respondServiceError(c, err, "User not found")
		return
	}
	apphandlers.RespondOK(c, user)
}

func (h *UserHandler) GetUsers(c *gin.Context) {
	tenant := h.getTenant(c)
	limit := apphandlers.ParseIntParam(c, "limit", 100)
	offset := apphandlers.ParseIntParam(c, "offset", 0)

	users, err := h.service.GetUsers(tenant.OrganizationID, limit, offset)
	if err != nil {
		respondServiceError(c, err, "Failed to load users")
		return
	}
	apphandlers.RespondOK(c, users)
}

func (h *UserHandler) GetUserByID(c *gin.Context) {
	id, err := apphandlers.ExtractIDParam(c, "id")
	if err != nil {
		apphandlers.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid user id")
		return
	}

	user, err := h.service.GetUserByID(id)
	if err != nil {
		respondServiceError(c, err, "User not found")
		return
	}
	apphandlers.RespondOK(c, user)
}

func (h *UserHandler) CreateUser(c *gin.Context) {
	var req types.UserRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		apphandlers.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	user, err := h.service.CreateUser(identityservice.CreateUserInput{
		OrganizationID: h.getTenant(c).OrganizationID,
		Email:          req.Email,
		Name:           req.Name,
		Role:           req.Role,
		Password:       req.Password,
	})
	if err != nil {
		respondServiceError(c, err, "Unable to create user")
		return
	}
	apphandlers.RespondOK(c, user)
}

type SignupRequest struct {
	Email    string `json:"email" example:"user@example.com"`
	Name     string `json:"name" example:"John Doe"`
	Password string `json:"password" example:"securePassword123"`
	TeamName string `json:"teamName" example:"My Team"`
	OrgID    *int64 `json:"orgId,omitempty" example:"1"`
	TeamID   *int64 `json:"teamId,omitempty" example:"5"`
}

type SignupResponse struct {
	UserID int64  `json:"user_id" example:"123"`
	Email  string `json:"email" example:"user@example.com"`
	TeamID int64  `json:"team_id" example:"5"`
	APIKey string `json:"api_key" example:"sk_live_abc123def456"`
}

// @Summary User Signup
// @Description Create a new user account with optional team
// @Tags Auth
// @Accept json
// @Produce json
// @Param request body SignupRequest true "Signup Request"
// @Success 200 {object} SignupResponse
// @Failure 400 {object} map[string]interface{}
// @Failure 500 {object} map[string]interface{}
// @Router /api/signup [post]
func (h *UserHandler) Signup(c *gin.Context) {
	var req SignupRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		apphandlers.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	resp, err := h.service.Signup(identityservice.SignupInput{
		Email:              req.Email,
		Name:               req.Name,
		Password:           req.Password,
		TeamName:           req.TeamName,
		OrganizationID:     req.OrgID,
		TenantOrganization: h.getTenant(c).OrganizationID,
	})
	if err != nil {
		respondServiceError(c, err, "Unable to create account")
		return
	}
	apphandlers.RespondOK(c, resp)
}

func (h *UserHandler) AddUserToTeam(c *gin.Context) {
	userID, err := apphandlers.ExtractIDParam(c, "userId")
	if err != nil {
		apphandlers.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid userId")
		return
	}
	teamID, err := apphandlers.ExtractIDParam(c, "teamId")
	if err != nil {
		apphandlers.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid teamId")
		return
	}

	if err := h.service.AddUserToTeam(userID, teamID, c.Query("role")); err != nil {
		respondServiceError(c, err, "Unable to add user to team")
		return
	}
	apphandlers.RespondOK(c, nil)
}

func (h *UserHandler) RemoveUserFromTeam(c *gin.Context) {
	userID, err := apphandlers.ExtractIDParam(c, "userId")
	if err != nil {
		apphandlers.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid userId")
		return
	}
	teamID, err := apphandlers.ExtractIDParam(c, "teamId")
	if err != nil {
		apphandlers.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid teamId")
		return
	}

	if err := h.service.RemoveUserFromTeam(userID, teamID); err != nil {
		respondServiceError(c, err, "Unable to remove user from team")
		return
	}
	apphandlers.RespondOK(c, nil)
}

func (h *UserHandler) GetTeams(c *gin.Context) {
	teams, err := h.service.GetTeams(h.getTenant(c).OrganizationID)
	if err != nil {
		respondServiceError(c, err, "Failed to load teams")
		return
	}
	apphandlers.RespondOK(c, teams)
}

func (h *UserHandler) GetMyTeams(c *gin.Context) {
	teams, err := h.service.GetMyTeams(h.getTenant(c).UserID)
	if err != nil {
		respondServiceError(c, err, "Failed to load teams")
		return
	}
	apphandlers.RespondOK(c, teams)
}

func (h *UserHandler) GetTeamByID(c *gin.Context) {
	id, err := apphandlers.ExtractIDParam(c, "id")
	if err != nil {
		apphandlers.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid team id")
		return
	}

	team, err := h.service.GetTeamByID(id)
	if err != nil {
		respondServiceError(c, err, "Team not found")
		return
	}
	apphandlers.RespondOK(c, team)
}

func (h *UserHandler) GetTeamBySlug(c *gin.Context) {
	team, err := h.service.GetTeamBySlug(h.getTenant(c).OrganizationID, c.Param("slug"))
	if err != nil {
		respondServiceError(c, err, "Team not found")
		return
	}
	apphandlers.RespondOK(c, team)
}

func (h *UserHandler) CreateTeam(c *gin.Context) {
	team, err := h.service.CreateTeam(identityservice.CreateTeamInput{
		OrganizationID: h.getTenant(c).OrganizationID,
		Name:           strings.TrimSpace(c.Query("name")),
		Slug:           strings.TrimSpace(c.Query("slug")),
		Description:    strings.TrimSpace(c.Query("description")),
		Color:          c.Query("color"),
	})
	if err != nil {
		respondServiceError(c, err, "Unable to create team")
		return
	}
	apphandlers.RespondOK(c, team)
}

func (h *UserHandler) GetProfile(c *gin.Context) {
	profile, err := h.service.GetProfile(h.getTenant(c).UserID)
	if err != nil {
		respondServiceError(c, err, "User not found")
		return
	}
	apphandlers.RespondOK(c, profile)
}

func (h *UserHandler) UpdateProfile(c *gin.Context) {
	var req types.SettingsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		apphandlers.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	profile, err := h.service.UpdateProfile(identityservice.UpdateProfileInput{
		UserID:    h.getTenant(c).UserID,
		Name:      req.Name,
		AvatarURL: req.AvatarURL,
	})
	if err != nil {
		respondServiceError(c, err, "Unable to update profile")
		return
	}
	apphandlers.RespondOK(c, profile)
}
