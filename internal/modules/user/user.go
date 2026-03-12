package user

import (
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/crypto/bcrypt"

	types "github.com/observability/observability-backend-go/internal/contracts"
	dbutil "github.com/observability/observability-backend-go/internal/database"
	configdefaults "github.com/observability/observability-backend-go/internal/defaultconfig"
	apphandlers "github.com/observability/observability-backend-go/internal/modules/common"
)

type UserHandler struct {
	store     *Store
	getTenant apphandlers.GetTenantFunc
	registry  *configdefaults.Registry
}

func NewUserHandler(getTenant apphandlers.GetTenantFunc, store *Store, registry *configdefaults.Registry) *UserHandler {
	return &UserHandler{
		store:     store,
		getTenant: getTenant,
		registry:  registry,
	}
}

func (h *UserHandler) GetCurrentUser(c *gin.Context) {
	userID := h.getTenant(c).UserID
	user, err := buildUserResponseByID(h.store, userID)
	if err != nil {
		respondServiceError(c, newNotFoundError("User not found", err), "User not found")
		return
	}
	apphandlers.RespondOK(c, user)
}

func (h *UserHandler) GetUsers(c *gin.Context) {
	tenant := h.getTenant(c)
	teamID := tenant.TeamID
	limit := apphandlers.ParseIntParam(c, "limit", 100)
	offset := apphandlers.ParseIntParam(c, "offset", 0)

	if limit <= 0 || limit > 500 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}

	team, err := h.store.FindTeamByID(teamID)
	if err != nil {
		respondServiceError(c, newInternalError("Failed to load team", err), "Failed to load users")
		return
	}
	orgName := dbutil.StringFromAny(team["org_name"])

	orgTeams, err := h.store.ListActiveTeamsByOrganization(orgName)
	if err != nil {
		respondServiceError(c, newInternalError("Failed to load teams", err), "Failed to load users")
		return
	}

	allTeamIDs := make([]int64, 0, len(orgTeams))
	for _, t := range orgTeams {
		if tid := dbutil.Int64FromAny(t["id"]); tid > 0 {
			allTeamIDs = append(allTeamIDs, tid)
		}
	}

	users, err := h.store.ListActiveUsersByTeamIDs(allTeamIDs, limit, offset)
	if err != nil {
		respondServiceError(c, newInternalError("Failed to load users", err), "Failed to load users")
		return
	}

	out := make([]map[string]any, 0, len(users))
	for _, u := range users {
		teamsJSON := dbutil.StringFromAny(u["teams"])
		memberships, _ := parseTeamsJSON(teamsJSON)

		userTeams := make([]map[string]any, 0, len(memberships))
		for _, m := range memberships {
			userTeams = append(userTeams, map[string]any{
				"teamId": m.TeamID,
				"role":   m.Role,
			})
		}

		out = append(out, map[string]any{
			"id":          dbutil.Int64FromAny(u["id"]),
			"email":       dbutil.StringFromAny(u["email"]),
			"name":        dbutil.StringFromAny(u["name"]),
			"avatarUrl":   dbutil.StringFromAny(u["avatar_url"]),
			"active":      dbutil.BoolFromAny(u["active"]),
			"lastLoginAt": u["last_login_at"],
			"createdAt":   u["created_at"],
			"teams":       userTeams,
		})
	}

	apphandlers.RespondOK(c, out)
}

func (h *UserHandler) GetUserByID(c *gin.Context) {
	id, err := apphandlers.ExtractIDParam(c, "id")
	if err != nil {
		apphandlers.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid user id")
		return
	}

	user, err := buildUserResponseByID(h.store, id)
	if err != nil {
		respondServiceError(c, newNotFoundError("User not found", err), "User not found")
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

	email := strings.TrimSpace(req.Email)
	name := strings.TrimSpace(req.Name)
	password := req.Password

	// Mandatory fields: name, email, password, teamIDs
	if email == "" || name == "" || password == "" || len(req.TeamIDs) == 0 {
		apphandlers.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "email, name, password and teamIDs are mandatory")
		return
	}

	role := strings.TrimSpace(req.Role)
	if role == "" {
		role = "member"
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		respondServiceError(c, newInternalError("Failed to hash password", err), "Unable to create user")
		return
	}
	passwordHash := string(hash)

	memberships := make([]TeamMembership, 0, len(req.TeamIDs))
	for _, tid := range req.TeamIDs {
		memberships = append(memberships, TeamMembership{TeamID: tid, Role: role})
	}
	teamsJSON, err := buildTeamsJSON(memberships)
	if err != nil {
		respondServiceError(c, newInternalError("Failed to build teams JSON", err), "Unable to create user")
		return
	}

	userID, err := h.store.CreateUser(email, passwordHash, name, teamsJSON, time.Now().UTC())
	if err != nil {
		respondServiceError(c, newValidationError("Unable to create user", err), "Unable to create user")
		return
	}

	user, err := buildUserResponseByID(h.store, userID)
	if err != nil {
		respondServiceError(c, newInternalError("Failed to load created user", err), "Unable to create user")
		return
	}

	apphandlers.RespondOK(c, user)
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

	role := strings.TrimSpace(c.Query("role"))
	if role == "" {
		role = "member"
	}

	user, err := h.store.FindUserByID(userID)
	if err != nil {
		respondServiceError(c, newInternalError("User not found", err), "Unable to add user to team")
		return
	}

	teamsJSON := dbutil.StringFromAny(user["teams"])
	memberships, _ := parseTeamsJSON(teamsJSON)

	found := false
	for i, m := range memberships {
		if m.TeamID == teamID {
			memberships[i].Role = role
			found = true
			break
		}
	}
	if !found {
		memberships = append(memberships, TeamMembership{TeamID: teamID, Role: role})
	}

	newJSON, err := buildTeamsJSON(memberships)
	if err != nil {
		respondServiceError(c, newInternalError("Failed to build teams JSON", err), "Unable to add user to team")
		return
	}

	if err := h.store.UpdateUserTeams(userID, newJSON); err != nil {
		respondServiceError(c, newInternalError("Unable to add user to team", err), "Unable to add user to team")
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

	user, err := h.store.FindUserByID(userID)
	if err != nil {
		respondServiceError(c, newInternalError("User not found", err), "Unable to remove user from team")
		return
	}

	teamsJSON := dbutil.StringFromAny(user["teams"])
	memberships, _ := parseTeamsJSON(teamsJSON)

	filtered := make([]TeamMembership, 0, len(memberships))
	for _, m := range memberships {
		if m.TeamID != teamID {
			filtered = append(filtered, m)
		}
	}

	newJSON, err := buildTeamsJSON(filtered)
	if err != nil {
		respondServiceError(c, newInternalError("Failed to build teams JSON", err), "Unable to remove user from team")
		return
	}

	if err := h.store.UpdateUserTeams(userID, newJSON); err != nil {
		respondServiceError(c, newInternalError("Unable to remove user from team", err), "Unable to remove user from team")
		return
	}

	apphandlers.RespondOK(c, nil)
}

func (h *UserHandler) GetTeams(c *gin.Context) {
	teamID := h.getTenant(c).TeamID

	team, err := h.store.FindTeamByID(teamID)
	if err != nil {
		respondServiceError(c, newInternalError("Failed to load team", err), "Failed to load teams")
		return
	}
	orgName := dbutil.StringFromAny(team["org_name"])

	rows, err := h.store.ListActiveTeamsByOrganization(orgName)
	if err != nil {
		respondServiceError(c, newInternalError("Failed to load teams", err), "Failed to load teams")
		return
	}
	apphandlers.RespondOK(c, dbutil.NormalizeRows(rows))
}

func (h *UserHandler) GetMyTeams(c *gin.Context) {
	teams, err := listActiveTeamsForUser(h.store, h.getTenant(c).UserID)
	if err != nil {
		respondServiceError(c, newInternalError("Failed to load teams", err), "Failed to load teams")
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

	team, err := h.store.FindTeamByID(id)
	if err != nil || len(team) == 0 {
		respondServiceError(c, newNotFoundError("Team not found", err), "Team not found")
		return
	}
	apphandlers.RespondOK(c, team)
}

func (h *UserHandler) GetTeamBySlug(c *gin.Context) {
	teamID := h.getTenant(c).TeamID
	slug := c.Param("slug")

	currentTeam, err := h.store.FindTeamByID(teamID)
	if err != nil {
		respondServiceError(c, newInternalError("Failed to load team", err), "Team not found")
		return
	}
	orgName := dbutil.StringFromAny(currentTeam["org_name"])

	team, err := h.store.FindTeamBySlug(orgName, slug)
	if err != nil || len(team) == 0 {
		respondServiceError(c, newNotFoundError("Team not found", err), "Team not found")
		return
	}

	apphandlers.RespondOK(c, team)
}

func (h *UserHandler) CreateTeam(c *gin.Context) {
	var req types.TeamRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		apphandlers.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	name := strings.TrimSpace(req.TeamName)
	if name == "" {
		respondServiceError(c, newValidationError("team name is required", nil), "Unable to create team")
		return
	}

	orgName := strings.TrimSpace(req.OrgName)
	if orgName == "" {
		respondServiceError(c, newValidationError("organization name is required", nil), "Unable to create team")
		return
	}

	slug := strings.TrimSpace(req.Slug)
	if slug == "" {
		slug = strings.ToLower(strings.ReplaceAll(name, " ", "-"))
	}

	color := strings.TrimSpace(req.Color)
	if color == "" {
		color = "#3B82F6"
	}

	apiKey, err := generateAPIKey()
	if err != nil {
		respondServiceError(c, newInternalError("Failed to generate api key", err), "Unable to create team")
		return
	}

	var descriptionPtr *string
	if desc := strings.TrimSpace(req.Description); desc != "" {
		descriptionPtr = &desc
	}

	defaultConfigJSON := "{}"
	if h.registry != nil {
		if jsonStr, err := h.registry.GenerateDefaultDashboardConfigsJSON(); err == nil {
			defaultConfigJSON = jsonStr
		}
	}

	teamID, err := h.store.CreateTeam(orgName, name, slug, descriptionPtr, color, apiKey, &defaultConfigJSON, time.Now().UTC())
	if err != nil {
		log.Printf("ERROR: Failed to create team: %v (orgName=%s, name=%s)", err, orgName, name)
		if strings.Contains(err.Error(), "1062") || strings.Contains(err.Error(), "Duplicate entry") {
			respondServiceError(c, newValidationError("Team already exists in this organization", err), "Team already exists in this organization")
			return
		}
		respondServiceError(c, newInternalError("Failed to create team", err), "Unable to create team")
		return
	}

	team, err := h.store.FindTeamByID(teamID)
	if err != nil {
		respondServiceError(c, newInternalError("Failed to load created team", err), "Unable to create team")
		return
	}

	apphandlers.RespondOK(c, team)
}

func (h *UserHandler) GetProfile(c *gin.Context) {
	profile, err := buildSettingsResponse(h.store, h.getTenant(c).UserID)
	if err != nil {
		respondServiceError(c, newNotFoundError("User not found", err), "User not found")
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

	userID := h.getTenant(c).UserID
	name := strings.TrimSpace(req.Name)
	avatarURL := strings.TrimSpace(req.AvatarURL)

	var namePtr *string
	var avatarPtr *string
	if name != "" {
		namePtr = &name
	}
	if avatarURL != "" {
		avatarPtr = &avatarURL
	}

	if err := h.store.UpdateUserProfile(userID, namePtr, avatarPtr); err != nil {
		respondServiceError(c, newInternalError("Unable to update profile", err), "Unable to update profile")
		return
	}

	profile, err := buildSettingsResponse(h.store, userID)
	if err != nil {
		respondServiceError(c, newInternalError("Failed to load updated profile", err), "Unable to update profile")
		return
	}

	apphandlers.RespondOK(c, profile)
}
