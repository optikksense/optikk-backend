package analytics

import (
	"net/http"

	"github.com/observability/observability-backend-go/internal/contracts/errorcode"

	"github.com/gin-gonic/gin"
	"github.com/observability/observability-backend-go/internal/modules/common"
)

type Handler struct {
	getTenant common.GetTenantFunc
	svc       *Service
}

func NewHandler(getTenant common.GetTenantFunc, svc *Service) *Handler {
	return &Handler{getTenant: getTenant, svc: svc}
}

// PostAnalytics handles POST /v1/spans/analytics.
func (h *Handler) PostAnalytics(c *gin.Context) {
	teamID := h.getTenant(c).TeamID

	var q AnalyticsQuery
	if err := c.ShouldBindJSON(&q); err != nil {
		common.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "Invalid request body: "+err.Error())
		return
	}

	result, err := h.svc.RunQuery(c.Request.Context(), teamID, q)
	if err != nil {
		common.RespondError(c, http.StatusBadRequest, errorcode.QueryFailed, err.Error())
		return
	}
	common.RespondOK(c, result)
}

// GetDimensions handles GET /v1/spans/analytics/dimensions.
func (h *Handler) GetDimensions(c *gin.Context) {
	common.RespondOK(c, AllDimensions())
}
