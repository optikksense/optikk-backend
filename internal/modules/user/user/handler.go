package userpage

import (
	"net/http"

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

func (h *Handler) GetProfile(c *gin.Context) {
	profile, err := h.Service.GetProfile(h.GetTenant(c).UserID)
	if err != nil {
		usershared.RespondServiceError(c, err, "User not found")
		return
	}
	modulecommon.RespondOK(c, profile)
}

func (h *Handler) UpdateProfile(c *gin.Context) {
	var req UpdateProfileRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Invalid request body")
		return
	}

	profile, err := h.Service.UpdateProfile(h.GetTenant(c).UserID, req)
	if err != nil {
		usershared.RespondServiceError(c, err, "Unable to update profile")
		return
	}
	modulecommon.RespondOK(c, profile)
}

func (h *Handler) UpdatePreferences(c *gin.Context) {
	var req UpdatePreferencesRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Invalid request body")
		return
	}

	response, err := h.Service.UpdatePreferences(h.GetTenant(c).UserID, req)
	if err != nil {
		usershared.RespondServiceError(c, err, "Unable to update preferences")
		return
	}

	modulecommon.RespondOK(c, response)
}
