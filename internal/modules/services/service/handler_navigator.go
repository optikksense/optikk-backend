package servicepage

import (
	"net/http"

	"github.com/gin-gonic/gin"
	. "github.com/observability/observability-backend-go/internal/modules/common"
)

func (h *ServiceHandler) GetNavigator(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	services, err := h.Service.GetNavigator(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query service health")
		return
	}

	RespondOK(c, services)
}
