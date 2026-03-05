package user

import (
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/crypto/bcrypt"

	types "github.com/observability/observability-backend-go/internal/contracts"
	apphandlers "github.com/observability/observability-backend-go/internal/modules/common"
	"github.com/observability/observability-backend-go/internal/platform/auth"
)

type AuthHandler struct {
	store        *Store
	getTenant    apphandlers.GetTenantFunc
	jwtManager   auth.JWTManager
	blacklist    *auth.TokenBlacklist
	jwtExpiresMs int64
}

func NewAuthHandler(getTenant apphandlers.GetTenantFunc, store *Store, jwtManager auth.JWTManager, blacklist *auth.TokenBlacklist, jwtExpiresMs int64) *AuthHandler {
	return &AuthHandler{
		store:        store,
		getTenant:    getTenant,
		jwtManager:   jwtManager,
		blacklist:    blacklist,
		jwtExpiresMs: jwtExpiresMs,
	}
}

// @Summary User Login
// @Description Authenticate user and return JWT token
func (h *AuthHandler) Login(c *gin.Context) {
	var req types.LoginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		apphandlers.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Email and password are required")
		return
	}

	email := strings.TrimSpace(req.Email)
	password := strings.TrimSpace(req.Password)

	if email == "" || password == "" {
		respondServiceError(c, newValidationError("Email and password are required", nil), "Failed to login")
		return
	}

	user, err := h.store.FindActiveUserByEmail(email)
	if err != nil {
		respondServiceError(c, newValidationError("Invalid email or password", err), "Failed to login")
		return
	}

	if user.PasswordHash != "" {
		if bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password)) != nil {
			respondServiceError(c, newValidationError("Invalid email or password", nil), "Failed to login")
			return
		}
	}

	_ = h.store.UpdateUserLastLogin(user.ID, time.Now().UTC())

	teams, err := listActiveTeamsForUser(h.store, user.ID)
	if err != nil {
		teams = []map[string]any{}
	}

	var currentTeam map[string]any
	if len(teams) > 0 {
		currentTeam = teams[0]
	}

	teamID := int64(1)
	if currentTeam != nil {
		teamID = mapInt64(currentTeam, "id")
	}

	var teamIDs []int64
	for _, t := range teams {
		teamIDs = append(teamIDs, mapInt64(t, "id"))
	}

	token, err := h.jwtManager.Generate(user.ID, user.Email, user.Name, user.Role, teamID, teamIDs...)
	if err != nil {
		respondServiceError(c, newInternalError("Failed to generate token", err), "Failed to login")
		return
	}

	resp := map[string]any{
		"token":       token,
		"tokenType":   "Bearer",
		"expiresIn":   h.jwtExpiresMs,
		"user":        map[string]any{"id": user.ID, "email": user.Email, "name": user.Name, "avatarUrl": user.AvatarURL, "role": user.Role},
		"teams":       teams,
		"currentTeam": currentTeam,
	}

	apphandlers.RespondOK(c, resp)
}

func (h *AuthHandler) Logout(c *gin.Context) {
	if header := c.GetHeader("Authorization"); strings.HasPrefix(header, "Bearer ") {
		token := strings.TrimPrefix(header, "Bearer ")
		if claims, err := h.jwtManager.Parse(token); err == nil && claims.ExpiresAt != nil {
			h.blacklist.Revoke(token, claims.ExpiresAt.Time)
		}
	}
	apphandlers.RespondOK(c, map[string]string{"message": "Logged out successfully"})
}

func (h *AuthHandler) AuthMe(c *gin.Context) {
	userID := h.getTenant(c).UserID
	if userID == 0 {
		respondServiceError(c, newUnauthorizedError("Not authenticated", nil), "Not authenticated")
		return
	}

	ctx, err := buildAuthContextResponse(h.store, userID)
	if err != nil {
		respondServiceError(c, newUnauthorizedError("Not authenticated", err), "Not authenticated")
		return
	}
	apphandlers.RespondOK(c, ctx)
}

func (h *AuthHandler) AuthContext(c *gin.Context) {
	h.AuthMe(c)
}

func (h *AuthHandler) ValidateToken(c *gin.Context) {
	tenant := h.getTenant(c)
	if tenant.UserID == 0 {
		apphandlers.RespondError(c, http.StatusUnauthorized, "UNAUTHORIZED", "Invalid or expired token")
		return
	}
	apphandlers.RespondOK(c, map[string]any{
		"valid":  true,
		"userId": tenant.UserID,
		"teamId": tenant.TeamID,
		"role":   tenant.UserRole,
	})
}
