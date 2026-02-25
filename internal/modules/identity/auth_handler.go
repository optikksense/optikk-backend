package identity

import (
	"database/sql"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	types "github.com/observability/observability-backend-go/internal/contracts"
	"github.com/observability/observability-backend-go/internal/platform/auth"
	apphandlers "github.com/observability/observability-backend-go/internal/platform/handlers"
	"golang.org/x/crypto/bcrypt"
)

// JWTGenerator is the subset of JWT manager required by AuthHandler.
type JWTGenerator interface {
	Generate(userID int64, email, name, role string, orgID, teamID int64) (string, error)
}

// AuthHandler handles authentication endpoints for the identity module.
type AuthHandler struct {
	Tables       TableProvider
	GetTenant    apphandlers.GetTenantFunc
	JWTManager   auth.JWTManager
	JWTExpiresMs int64
}

// @Summary User Login
// @Description Authenticate user and return JWT token
// @Tags Auth
// @Accept json
// @Produce json
// @Param request body types.LoginRequest true "Login Request"
// @Success 200 {object} map[string]interface{}
// @Failure 400 {object} map[string]interface{}
// @Router /api/auth/login [post]
func (h *AuthHandler) Login(c *gin.Context) {
	var req types.LoginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		apphandlers.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Email and password are required")
		return
	}

	user, err := h.Tables.Users().FindActiveByEmail(strings.TrimSpace(req.Email))
	if err != nil {
		apphandlers.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid email or password")
		return
	}

	if user.PasswordHash != "" {
		if bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(req.Password)) != nil {
			apphandlers.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid email or password")
			return
		}
	}

	_ = h.Tables.Users().UpdateLastLogin(user.ID, time.Now().UTC())

	teams, _ := getTeamsForUser(h.Tables, user.ID)
	var currentTeam map[string]any
	if len(teams) > 0 {
		currentTeam = teams[0]
	}

	teamID := int64(1)
	if currentTeam != nil {
		teamID = apphandlers.Int64FromAny(currentTeam["id"])
	}

	token, err := h.JWTManager.Generate(user.ID, user.Email, user.Name, user.Role, user.OrganizationID, teamID)
	if err != nil {
		apphandlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to generate token")
		return
	}

	apphandlers.RespondOK(c, map[string]any{
		"token":       token,
		"tokenType":   "Bearer",
		"expiresIn":   h.JWTExpiresMs,
		"user":        map[string]any{"id": user.ID, "email": user.Email, "name": user.Name, "avatarUrl": user.AvatarURL, "role": user.Role},
		"teams":       teams,
		"currentTeam": currentTeam,
	})
}

func (h *AuthHandler) Logout(c *gin.Context) {
	apphandlers.RespondOK(c, map[string]string{"message": "Logged out successfully"})
}

func (h *AuthHandler) AuthMe(c *gin.Context) {
	tenant := h.GetTenant(c)
	if tenant.UserID == 0 {
		apphandlers.RespondError(c, http.StatusUnauthorized, "UNAUTHORIZED", "Not authenticated")
		return
	}
	ctx, err := buildContextResponse(h.Tables, tenant.UserID)
	if err != nil {
		apphandlers.RespondError(c, http.StatusUnauthorized, "UNAUTHORIZED", "Not authenticated")
		return
	}
	apphandlers.RespondOK(c, ctx)
}

func (h *AuthHandler) AuthContext(c *gin.Context) {
	h.AuthMe(c)
}

func (h *AuthHandler) ValidateToken(c *gin.Context) {
	tenant := h.GetTenant(c)
	if tenant.UserID == 0 {
		apphandlers.RespondError(c, http.StatusUnauthorized, "UNAUTHORIZED", "Invalid or expired token")
		return
	}
	apphandlers.RespondOK(c, map[string]any{
		"valid":          true,
		"userId":         tenant.UserID,
		"organizationId": tenant.OrganizationID,
		"role":           tenant.UserRole,
	})
}

func getTeamsForUser(tables TableProvider, userID int64) ([]map[string]any, error) {
	rows, err := tables.UserTeams().ListActiveByUser(userID)
	if err != nil {
		return nil, err
	}
	teams := make([]map[string]any, 0, len(rows))
	for _, r := range rows {
		teams = append(teams, map[string]any{
			"id":    apphandlers.Int64FromAny(r["team_id"]),
			"name":  apphandlers.StringFromAny(r["team_name"]),
			"slug":  apphandlers.StringFromAny(r["team_slug"]),
			"color": apphandlers.StringFromAny(r["team_color"]),
			"role":  apphandlers.StringFromAny(r["role"]),
		})
	}
	return teams, nil
}

func buildContextResponse(tables TableProvider, userID int64) (map[string]any, error) {
	user, err := tables.Users().FindActiveByID(userID)
	if err != nil || len(user) == 0 {
		return nil, sql.ErrNoRows
	}
	teams, _ := getTeamsForUser(tables, userID)
	var currentTeam map[string]any
	if len(teams) > 0 {
		currentTeam = teams[0]
	}
	return map[string]any{
		"user": map[string]any{
			"id":        apphandlers.Int64FromAny(user["id"]),
			"email":     apphandlers.StringFromAny(user["email"]),
			"name":      apphandlers.StringFromAny(user["name"]),
			"avatarUrl": apphandlers.StringFromAny(user["avatar_url"]),
			"role":      apphandlers.StringFromAny(user["role"]),
		},
		"teams":       teams,
		"currentTeam": currentTeam,
	}, nil
}
