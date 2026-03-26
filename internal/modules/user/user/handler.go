package userpage

import (
	"net/http"

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

func (h *Handler) GetCurrentUser(c *gin.Context) {
	user, err := h.Service.GetCurrentUser(h.GetTenant(c).UserID)
	if err != nil {
		usershared.RespondServiceError(c, err, "User not found")
		return
	}
	modulecommon.RespondOK(c, user)
}

func (h *Handler) GetUsers(c *gin.Context) {
	users, err := h.Service.GetUsers(
		h.GetTenant(c).TeamID,
		modulecommon.ParseIntParam(c, "limit", 100),
		modulecommon.ParseIntParam(c, "offset", 0),
	)
	if err != nil {
		usershared.RespondServiceError(c, err, "Failed to load users")
		return
	}
	modulecommon.RespondOK(c, users)
}

func (h *Handler) GetUserByID(c *gin.Context) {
	userID, err := modulecommon.ExtractIDParam(c, "id")
	if err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Invalid user id")
		return
	}

	user, err := h.Service.GetUserByID(userID)
	if err != nil {
		usershared.RespondServiceError(c, err, "User not found")
		return
	}
	modulecommon.RespondOK(c, user)
}

func (h *Handler) CreateUser(c *gin.Context) {
	var req CreateUserRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Invalid request body")
		return
	}
	if err := appvalidation.Struct(req); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "email, name, password and teamIDs are mandatory")
		return
	}

	user, err := h.Service.CreateUser(req)
	if err != nil {
		usershared.RespondServiceError(c, err, "Unable to create user")
		return
	}
	modulecommon.RespondOK(c, user)
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
