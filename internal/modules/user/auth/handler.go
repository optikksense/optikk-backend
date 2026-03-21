package auth

import (
	"net/http"
	"strings"

	"github.com/observability/observability-backend-go/internal/contracts/errorcode"

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

func (h *Handler) Login(c *gin.Context) {
	var req LoginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Email and password are required")
		return
	}

	req.Email = strings.TrimSpace(req.Email)
	req.Password = strings.TrimSpace(req.Password)
	if err := appvalidation.Struct(req); err != nil {
		usershared.RespondServiceError(c, usershared.NewValidationError("Email and password are required", nil), "Failed to login")
		return
	}

	response, err := h.Service.Login(c.Request.Context(), req, c.ClientIP())
	if err != nil {
		usershared.RespondServiceError(c, err, "Failed to login")
		return
	}
	modulecommon.RespondOK(c, response)
}

func (h *Handler) Logout(c *gin.Context) {
	response, err := h.Service.Logout(c.Request.Context(), h.GetTenant(c), c.ClientIP())
	if err != nil {
		usershared.RespondServiceError(c, err, "Failed to logout")
		return
	}
	modulecommon.RespondOK(c, response)
}

func (h *Handler) AuthMe(c *gin.Context) {
	response, err := h.Service.AuthContext(h.GetTenant(c).UserID)
	if err != nil {
		usershared.RespondServiceError(c, err, "Not authenticated")
		return
	}
	modulecommon.RespondOK(c, response)
}

func (h *Handler) AuthContext(c *gin.Context) {
	h.AuthMe(c)
}

func (h *Handler) ValidateToken(c *gin.Context) {
	response, err := h.Service.ValidateToken(h.GetTenant(c))
	if err != nil {
		usershared.RespondServiceError(c, err, "Invalid or expired session")
		return
	}
	modulecommon.RespondOK(c, response)
}

func (h *Handler) GoogleLogin(c *gin.Context) {
	if !h.Service.GoogleConfigured() {
		modulecommon.RespondError(c, http.StatusServiceUnavailable, errorcode.Unavailable, "Google OAuth is not configured")
		return
	}
	url, err := h.Service.GoogleLoginURL()
	if err != nil {
		usershared.RespondServiceError(c, err, "Failed to generate state")
		return
	}
	c.Redirect(http.StatusTemporaryRedirect, url)
}

func (h *Handler) GoogleCallback(c *gin.Context) {
	if !h.Service.GoogleConfigured() {
		modulecommon.RespondError(c, http.StatusServiceUnavailable, errorcode.Unavailable, "Google OAuth is not configured")
		return
	}
	c.Redirect(http.StatusTemporaryRedirect, h.Service.GoogleCallbackRedirect(c.Query("code")))
}

func (h *Handler) GithubLogin(c *gin.Context) {
	if !h.Service.GithubConfigured() {
		modulecommon.RespondError(c, http.StatusServiceUnavailable, errorcode.Unavailable, "GitHub OAuth is not configured")
		return
	}
	url, err := h.Service.GithubLoginURL()
	if err != nil {
		usershared.RespondServiceError(c, err, "Failed to generate state")
		return
	}
	c.Redirect(http.StatusTemporaryRedirect, url)
}

func (h *Handler) GithubCallback(c *gin.Context) {
	if !h.Service.GithubConfigured() {
		modulecommon.RespondError(c, http.StatusServiceUnavailable, errorcode.Unavailable, "GitHub OAuth is not configured")
		return
	}
	c.Redirect(http.StatusTemporaryRedirect, h.Service.GithubCallbackRedirect(c.Query("code")))
}

func (h *Handler) CompleteSignup(c *gin.Context) {
	var req CompleteSignupRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "teamName and orgName are required")
		return
	}

	req.TeamName = strings.TrimSpace(req.TeamName)
	req.OrgName = strings.TrimSpace(req.OrgName)
	if err := appvalidation.Struct(req); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "teamName and orgName are required")
		return
	}

	response, err := h.Service.CompleteSignup(c.Request.Context(), req)
	if err != nil {
		usershared.RespondServiceError(c, err, "Unable to complete signup")
		return
	}
	modulecommon.RespondOK(c, response)
}

func (h *Handler) ForgotPassword(c *gin.Context) {
	modulecommon.RespondOK(c, h.Service.ForgotPassword())
}
