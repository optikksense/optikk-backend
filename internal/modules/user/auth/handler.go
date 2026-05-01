package auth

import (
	"net/http"
	"strings"

	"github.com/Optikk-Org/optikk-backend/internal/shared/errorcode"

	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"

	usershared "github.com/Optikk-Org/optikk-backend/internal/modules/user/internal/shared"
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

func (h *Handler) ValidateToken(c *gin.Context) {
	response, err := h.Service.ValidateToken(h.GetTenant(c))
	if err != nil {
		usershared.RespondServiceError(c, err, "Invalid or expired session")
		return
	}
	modulecommon.RespondOK(c, response)
}

func (h *Handler) ForgotPassword(c *gin.Context) {
	modulecommon.RespondOK(c, h.Service.ForgotPassword())
}
