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

func (h *DiskHandler) GetDiskIO(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetDiskIO(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query disk I/O", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *DiskHandler) GetDiskOperations(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetDiskOperations(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query disk operations", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *DiskHandler) GetDiskIOTime(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetDiskIOTime(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query disk I/O time", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *DiskHandler) GetFilesystemUsage(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetFilesystemUsage(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query filesystem usage", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *DiskHandler) GetFilesystemUtilization(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetFilesystemUtilization(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query filesystem utilization", err)
		return
	}
	modulecommon.RespondOK(c, resp)
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

func (h *DiskHandler) GetDiskByService(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	serviceName := c.Query("serviceName")
	if serviceName == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "serviceName is required")
		return
	}
	resp, err := h.Service.GetDiskByService(c.Request.Context(), teamID, serviceName, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query disk by service", err)
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
