package errorfingerprint

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/observability/observability-backend-go/internal/modules/common"
)

type Handler struct {
	getTenant common.GetTenantFunc
	service   *Service
}

func NewHandler(getTenant common.GetTenantFunc, service *Service) *Handler {
	return &Handler{getTenant: getTenant, service: service}
}

// ListFingerprints handles GET /v1/errors/fingerprints
func (h *Handler) ListFingerprints(c *gin.Context) {
	teamID := h.getTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	serviceName := c.Query("serviceName")
	limit := common.ParseIntParam(c, "limit", 100)
	if limit <= 0 || limit > 500 {
		limit = 100
	}

	fps, err := h.service.ListFingerprints(teamID, startMs, endMs, serviceName, limit)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query error fingerprints")
		return
	}
	common.RespondOK(c, fps)
}

// GetFingerprintTrend handles GET /v1/errors/fingerprints/trend
func (h *Handler) GetFingerprintTrend(c *gin.Context) {
	teamID := h.getTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	serviceName := c.Query("serviceName")
	operationName := c.Query("operationName")
	exceptionType := c.Query("exceptionType")
	statusMessage := c.Query("statusMessage")

	if serviceName == "" || operationName == "" {
		common.RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "serviceName and operationName are required")
		return
	}

	points, err := h.service.GetFingerprintTrend(teamID, startMs, endMs, serviceName, operationName, exceptionType, statusMessage)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query fingerprint trend")
		return
	}
	common.RespondOK(c, points)
}
