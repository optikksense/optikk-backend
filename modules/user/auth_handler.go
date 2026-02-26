package identity

import (
	"net/http"

	"github.com/gin-gonic/gin"
	types "github.com/observability/observability-backend-go/internal/contracts"
	identityservice "github.com/observability/observability-backend-go/modules/user/service"
	apphandlers "github.com/observability/observability-backend-go/internal/platform/handlers"
)

// AuthHandler handles authentication endpoints for the identity module.
type AuthHandler struct {
	service   identityservice.AuthService
	getTenant apphandlers.GetTenantFunc
}

func NewAuthHandler(getTenant apphandlers.GetTenantFunc, service identityservice.AuthService) *AuthHandler {
	return &AuthHandler{
		service:   service,
		getTenant: getTenant,
	}
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

	resp, err := h.service.Login(req.Email, req.Password)
	if err != nil {
		respondServiceError(c, err, "Failed to login")
		return
	}

	apphandlers.RespondOK(c, resp)
}

func (h *AuthHandler) Logout(c *gin.Context) {
	apphandlers.RespondOK(c, map[string]string{"message": "Logged out successfully"})
}

func (h *AuthHandler) AuthMe(c *gin.Context) {
	ctx, err := h.service.BuildAuthContext(h.getTenant(c).UserID)
	if err != nil {
		respondServiceError(c, err, "Not authenticated")
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
		"valid":          true,
		"userId":         tenant.UserID,
		"organizationId": tenant.OrganizationID,
		"role":           tenant.UserRole,
	})
}
