package disk

import (
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/shared/errorcode"

	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type DiskHandler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *DiskHandler) GetAvgDisk(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetAvgDisk(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query avg disk", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *DiskHandler) GetDiskByInstance(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	host := c.Query("host")
	pod := c.Query("pod")
	container := c.Query("container")
	serviceName := c.Query("serviceName")
	if serviceName == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "serviceName is required")
		return
	}
	resp, err := h.Service.GetDiskByInstance(c.Request.Context(), teamID, host, pod, container, serviceName, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query disk by instance", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}
