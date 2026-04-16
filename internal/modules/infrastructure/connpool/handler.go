package connpool

import (
	"net/http"

	errorcode "github.com/Optikk-Org/optikk-backend/internal/shared/contracts"

	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type ConnPoolHandler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *ConnPoolHandler) GetAvgConnPool(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetAvgConnPool(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query avg connection pool", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *ConnPoolHandler) GetConnPoolByService(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetConnPoolByService(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query connection pool by service", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *ConnPoolHandler) GetConnPoolByInstance(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetConnPoolByInstance(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query connection pool by instance", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}
