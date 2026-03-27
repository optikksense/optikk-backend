package analytics

import (
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts/errorcode"

	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type Handler struct {
	getTenant modulecommon.GetTenantFunc
	svc       *Service
}

func NewHandler(getTenant modulecommon.GetTenantFunc, svc *Service) *Handler {
	return &Handler{getTenant: getTenant, svc: svc}
}

// PostAnalytics handles POST /v1/spans/analytics.
func (h *Handler) PostAnalytics(c *gin.Context) {
	teamID := h.getTenant(c).TeamID

	var q AnalyticsQuery
	if err := c.ShouldBindJSON(&q); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "Invalid request body: "+err.Error())
		return
	}

	result, err := h.svc.RunQuery(c.Request.Context(), teamID, q)
	if err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.QueryFailed, err.Error())
		return
	}
	modulecommon.RespondOK(c, result)
}

// GetDimensions handles GET /v1/spans/analytics/dimensions.
func (h *Handler) GetDimensions(c *gin.Context) {
	modulecommon.RespondOK(c, AllDimensions())
}
