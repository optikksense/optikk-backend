package identity

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	types "github.com/observability/observability-backend-go/internal/contracts"
	apphandlers "github.com/observability/observability-backend-go/internal/platform/handlers"
	"golang.org/x/crypto/bcrypt"
)

// UserHandler handles user/team/profile endpoints for the identity module.
type UserHandler struct {
	Tables    TableProvider
	GetTenant apphandlers.GetTenantFunc
}

func (h *UserHandler) GetCurrentUser(c *gin.Context) {
	tenant := h.GetTenant(c)
	user, err := userByID(h.Tables, tenant.UserID)
	if err != nil {
		apphandlers.RespondError(c, http.StatusNotFound, "RESOURCE_NOT_FOUND", "User not found")
		return
	}
	apphandlers.RespondOK(c, user)
}

func (h *UserHandler) GetUsers(c *gin.Context) {
	tenant := h.GetTenant(c)
	limit := apphandlers.ParseIntParam(c, "limit", 100)
	offset := apphandlers.ParseIntParam(c, "offset", 0)
	if limit <= 0 || limit > 500 {
		limit = 100
	}

	users, err := h.Tables.Users().ListActiveByOrganization(tenant.OrganizationID, limit, offset)
	if err != nil {
		apphandlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to load users")
		return
	}

	userIDs := make([]int64, 0, len(users))
	for _, u := range users {
		if id := apphandlers.Int64FromAny(u["id"]); id > 0 {
			userIDs = append(userIDs, id)
		}
	}

	memberships, err := h.Tables.UserTeams().ListByUsers(userIDs, true)
	if err != nil {
		apphandlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to load users")
		return
	}

	teamByUser := make(map[int64][]map[string]any, len(userIDs))
	for _, m := range memberships {
		uid := apphandlers.Int64FromAny(m["user_id"])
		teamID := apphandlers.Int64FromAny(m["team_id"])
		if uid == 0 || teamID == 0 {
			continue
		}
		teamByUser[uid] = append(teamByUser[uid], map[string]any{
			"teamId":   teamID,
			"teamName": apphandlers.StringFromAny(m["team_name"]),
			"teamSlug": apphandlers.StringFromAny(m["team_slug"]),
			"role":     apphandlers.StringFromAny(m["role"]),
		})
	}

	out := make([]map[string]any, 0, len(users))
	for _, u := range users {
		uid := apphandlers.Int64FromAny(u["id"])
		userTeams := teamByUser[uid]
		if userTeams == nil {
			userTeams = []map[string]any{}
		}
		out = append(out, map[string]any{
			"id":             uid,
			"organizationId": apphandlers.Int64FromAny(u["organization_id"]),
			"email":          apphandlers.StringFromAny(u["email"]),
			"name":           apphandlers.StringFromAny(u["name"]),
			"avatarUrl":      apphandlers.StringFromAny(u["avatar_url"]),
			"role":           apphandlers.StringFromAny(u["role"]),
			"active":         apphandlers.BoolFromAny(u["active"]),
			"lastLoginAt":    u["last_login_at"],
			"createdAt":      u["created_at"],
			"teams":          userTeams,
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
	user, err := userByID(h.Tables, id)
	if err != nil {
		apphandlers.RespondError(c, http.StatusNotFound, "RESOURCE_NOT_FOUND", "User not found")
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
	if strings.TrimSpace(req.Email) == "" || strings.TrimSpace(req.Name) == "" {
		apphandlers.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "email and name are required")
		return
	}

	tenant := h.GetTenant(c)
	role := req.Role
	if role == "" {
		role = "member"
	}

	passwordHash := ""
	if req.Password != "" {
		hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
		if err == nil {
			passwordHash = string(hash)
		}
	}

	userID, err := h.Tables.Users().Create(tenant.OrganizationID, req.Email, passwordHash, req.Name, role, time.Now().UTC())
	if err != nil {
		apphandlers.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Unable to create user")
		return
	}

	// Map user to the first active team in the organization
	teams, _ := h.Tables.Teams().ListActiveByOrganization(tenant.OrganizationID)
	if len(teams) > 0 {
		teamID := apphandlers.Int64FromAny(teams[0]["id"])
		_ = h.Tables.UserTeams().Upsert(userID, teamID, role, time.Now().UTC())
	}

	user, _ := userByID(h.Tables, userID)
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
	if strings.TrimSpace(req.Email) == "" || strings.TrimSpace(req.Password) == "" || strings.TrimSpace(req.TeamName) == "" {
		apphandlers.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "email, password, and teamName are required")
		return
	}

	// Determine orgID from request or tenant context, or from database defaults
	var orgID, teamID int64
	var apiKey string

	// Get orgID from request header, tenant context, or default
	if req.OrgID != nil {
		orgID = *req.OrgID
	} else {
		// Try to get from tenant context
		tenant := h.GetTenant(c)
		orgID = tenant.OrganizationID
		if orgID == 0 {
			orgID = 1 // Fallback default
		}
	}
	slug := strings.ToLower(strings.ReplaceAll(req.TeamName, " ", "-"))
	apiKey, _ = generateAPIKey()

	teamID, err := h.Tables.Teams().Create(orgID, req.TeamName, slug, nil, "#3B82F6", apiKey, time.Now().UTC())
	if err != nil {
		// Team might exist, let's try to find it
		existingTeam, findErr := h.Tables.Teams().FindBySlug(orgID, slug)
		if findErr != nil || len(existingTeam) == 0 {
			apphandlers.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", fmt.Sprintf("Unable to create team: %v", err))
			return
		}
		teamID = apphandlers.Int64FromAny(existingTeam["id"])
		apiKey = apphandlers.StringFromAny(existingTeam["api_key"])
	}

	// 2. Hash Password and Create User
	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		apphandlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to hash password")
		return
	}
	passwordHash := string(hash)

	userID, err := h.Tables.Users().Create(orgID, req.Email, passwordHash, req.Name, "admin", time.Now().UTC())
	if err != nil {
		apphandlers.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Unable to create user (may already exist)")
		return
	}

	// 3. Map user to team
	_ = h.Tables.UserTeams().Upsert(userID, teamID, "admin", time.Now().UTC())

	apphandlers.RespondOK(c, map[string]any{
		"user_id": userID,
		"email":   req.Email,
		"team_id": teamID,
		"api_key": apiKey,
	})
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
	role := c.Query("role")
	if role == "" {
		role = "member"
	}

	if err := h.Tables.UserTeams().Upsert(userID, teamID, role, time.Now().UTC()); err != nil {
		apphandlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Unable to add user to team")
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

	if err := h.Tables.UserTeams().Delete(userID, teamID); err != nil {
		apphandlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Unable to remove user from team")
		return
	}
	apphandlers.RespondOK(c, nil)
}

func (h *UserHandler) GetTeams(c *gin.Context) {
	tenant := h.GetTenant(c)
	rows, err := h.Tables.Teams().ListActiveByOrganization(tenant.OrganizationID)
	if err != nil {
		apphandlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to load teams")
		return
	}
	apphandlers.RespondOK(c, apphandlers.NormalizeRows(rows))
}

func (h *UserHandler) GetMyTeams(c *gin.Context) {
	tenant := h.GetTenant(c)
	rows, err := h.Tables.Teams().ListActiveByUser(tenant.UserID)
	if err != nil {
		apphandlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to load teams")
		return
	}
	apphandlers.RespondOK(c, apphandlers.NormalizeRows(rows))
}

func (h *UserHandler) GetTeamByID(c *gin.Context) {
	id, err := apphandlers.ExtractIDParam(c, "id")
	if err != nil {
		apphandlers.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid team id")
		return
	}
	team, err := h.Tables.Teams().FindByID(id)
	if err != nil || len(team) == 0 {
		apphandlers.RespondError(c, http.StatusNotFound, "RESOURCE_NOT_FOUND", "Team not found")
		return
	}
	apphandlers.RespondOK(c, team)
}

func (h *UserHandler) GetTeamBySlug(c *gin.Context) {
	tenant := h.GetTenant(c)
	team, err := h.Tables.Teams().FindBySlug(tenant.OrganizationID, c.Param("slug"))
	if err != nil || len(team) == 0 {
		apphandlers.RespondError(c, http.StatusNotFound, "RESOURCE_NOT_FOUND", "Team not found")
		return
	}
	apphandlers.RespondOK(c, team)
}

func (h *UserHandler) CreateTeam(c *gin.Context) {
	tenant := h.GetTenant(c)
	name := strings.TrimSpace(c.Query("name"))
	if name == "" {
		apphandlers.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "name is required")
		return
	}

	slug := strings.TrimSpace(c.Query("slug"))
	if slug == "" {
		slug = strings.ToLower(strings.ReplaceAll(name, " ", "-"))
	}

	description := strings.TrimSpace(c.Query("description"))
	color := c.Query("color")
	if color == "" {
		color = "#3B82F6"
	}

	apiKey, err := generateAPIKey()
	if err != nil {
		apphandlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to generate api key")
		return
	}

	var descriptionPtr *string
	if description != "" {
		descriptionPtr = &description
	}

	id, err := h.Tables.Teams().Create(tenant.OrganizationID, name, slug, descriptionPtr, color, apiKey, time.Now().UTC())
	if err != nil {
		apphandlers.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Unable to create team")
		return
	}

	team, _ := h.Tables.Teams().FindByID(id)
	apphandlers.RespondOK(c, team)
}

func (h *UserHandler) GetProfile(c *gin.Context) {
	tenant := h.GetTenant(c)
	profile, err := settingsResponse(h.Tables, tenant.UserID)
	if err != nil {
		apphandlers.RespondError(c, http.StatusNotFound, "RESOURCE_NOT_FOUND", "User not found")
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

	tenant := h.GetTenant(c)
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

	if err := h.Tables.Users().UpdateProfile(tenant.UserID, namePtr, avatarPtr, time.Now().UTC()); err != nil {
		apphandlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Unable to update profile")
		return
	}

	profile, _ := settingsResponse(h.Tables, tenant.UserID)
	apphandlers.RespondOK(c, profile)
}

func userByID(tables TableProvider, userID int64) (map[string]any, error) {
	user, err := tables.Users().FindByID(userID)
	if err != nil || len(user) == 0 {
		return nil, sql.ErrNoRows
	}

	teams, _ := tables.UserTeams().ListByUser(userID)
	memberships := make([]map[string]any, 0, len(teams))
	for _, t := range teams {
		memberships = append(memberships, map[string]any{
			"teamId":   apphandlers.Int64FromAny(t["team_id"]),
			"teamName": apphandlers.StringFromAny(t["team_name"]),
			"teamSlug": apphandlers.StringFromAny(t["team_slug"]),
			"role":     apphandlers.StringFromAny(t["role"]),
		})
	}

	return map[string]any{
		"id":             apphandlers.Int64FromAny(user["id"]),
		"organizationId": apphandlers.Int64FromAny(user["organization_id"]),
		"email":          apphandlers.StringFromAny(user["email"]),
		"name":           apphandlers.StringFromAny(user["name"]),
		"avatarUrl":      apphandlers.StringFromAny(user["avatar_url"]),
		"role":           apphandlers.StringFromAny(user["role"]),
		"active":         apphandlers.BoolFromAny(user["active"]),
		"lastLoginAt":    user["last_login_at"],
		"createdAt":      user["created_at"],
		"teams":          memberships,
	}, nil
}

func settingsResponse(tables TableProvider, userID int64) (map[string]any, error) {
	row, err := tables.Users().FindActiveByID(userID)
	if err != nil || len(row) == 0 {
		return nil, sql.ErrNoRows
	}

	teams, _ := tables.Teams().ListActiveByOrganization(apphandlers.Int64FromAny(row["organization_id"]))
	return map[string]any{
		"userId":      apphandlers.Int64FromAny(row["id"]),
		"name":        apphandlers.StringFromAny(row["name"]),
		"email":       apphandlers.StringFromAny(row["email"]),
		"avatarUrl":   apphandlers.StringFromAny(row["avatar_url"]),
		"role":        apphandlers.StringFromAny(row["role"]),
		"preferences": map[string]any{},
		"teams":       apphandlers.NormalizeRows(teams),
	}, nil
}

func generateAPIKey() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
