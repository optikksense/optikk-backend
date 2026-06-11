package user

import (
	"net/http"
	"strings"

	"github.com/Optikk-Org/optikk-backend/internal/infra/token"
	"github.com/Optikk-Org/optikk-backend/internal/shared/errorcode"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

// Handler handles HTTP requests for users and authentication.
type Handler struct {
	modulecommon.DBTenant
	Service *Service
	Tokens  *token.Service
}

// NewHandler creates a new Handler instance.
func NewHandler(getTenant modulecommon.GetTenantFunc, service *Service, tokens *token.Service) *Handler {
	return &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  service,
		Tokens:   tokens,
	}
}

// Login authenticates a user credentials.
func (h *Handler) Login(c *gin.Context) {
	var req LoginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Email and password are required")
		return
	}

	req.Email = strings.TrimSpace(req.Email)
	req.Password = strings.TrimSpace(req.Password)

	response, refresh, err := h.Service.Login(c.Request.Context(), req, c.ClientIP())
	if err != nil {
		RespondServiceError(c, err, "Failed to login")
		return
	}
	h.Tokens.SetRefreshCookie(c.Writer, refresh)
	modulecommon.RespondOK(c, response)
}

// Refresh exchanges a valid refresh cookie for a new token pair.
func (h *Handler) Refresh(c *gin.Context) {
	raw, err := c.Cookie(h.Tokens.RefreshCookieName())
	if err != nil || raw == "" {
		modulecommon.RespondError(c, http.StatusUnauthorized, errorcode.Unauthorized, "Missing refresh token")
		return
	}

	response, refresh, err := h.Service.Refresh(c.Request.Context(), raw)
	if err != nil {
		RespondServiceError(c, err, "Failed to refresh token")
		return
	}
	h.Tokens.SetRefreshCookie(c.Writer, refresh)
	modulecommon.RespondOK(c, response)
}

// Logout clears the refresh cookie.
func (h *Handler) Logout(c *gin.Context) {
	response := h.Service.Logout(c.Request.Context(), h.GetTenant(c), c.ClientIP())
	h.Tokens.ClearRefreshCookie(c.Writer)
	modulecommon.RespondOK(c, response)
}

// AuthMe returns details for the current user context.
func (h *Handler) AuthMe(c *gin.Context) {
	response, err := h.Service.AuthContext(h.GetTenant(c).UserID)
	if err != nil {
		RespondServiceError(c, err, "Not authenticated")
		return
	}
	modulecommon.RespondOK(c, response)
}

// GetProfile returns the profile settings of the current user.
func (h *Handler) GetProfile(c *gin.Context) {
	profile, err := h.Service.GetProfile(h.GetTenant(c).UserID)
	if err != nil {
		RespondServiceError(c, err, "User not found")
		return
	}
	modulecommon.RespondOK(c, profile)
}

// UpdateProfile updates profile info of the current user.
func (h *Handler) UpdateProfile(c *gin.Context) {
	var req UpdateProfileRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Invalid request body")
		return
	}

	profile, err := h.Service.UpdateProfile(h.GetTenant(c).UserID, req)
	if err != nil {
		RespondServiceError(c, err, "Unable to update profile")
		return
	}
	modulecommon.RespondOK(c, profile)
}

// UpdatePreferences updates preferences of the current user.
func (h *Handler) UpdatePreferences(c *gin.Context) {
	var req UpdatePreferencesRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Invalid request body")
		return
	}

	response, err := h.Service.UpdatePreferences(h.GetTenant(c).UserID, req)
	if err != nil {
		RespondServiceError(c, err, "Unable to update preferences")
		return
	}

	modulecommon.RespondOK(c, response)
}
