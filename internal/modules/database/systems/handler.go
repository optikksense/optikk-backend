package systems

import (
	"net/http"

	"github.com/gin-gonic/gin"
	common "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

type Handler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *Handler) GetDetectedSystems(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetDetectedSystems(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query detected database systems")
		return
	}
	common.RespondOK(c, resp)
}
