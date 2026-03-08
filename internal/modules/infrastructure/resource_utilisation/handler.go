package resource_utilisation

import (
	"net/http"

	"github.com/gin-gonic/gin"
	. "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

// ResourceUtilisationHandler handles API endpoints for 8 individual metrics.
type ResourceUtilisationHandler struct {
	modulecommon.DBTenant
	Service Service
}

func (h *ResourceUtilisationHandler) GetAvgCPU(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	resp, err := h.Service.GetAvgCPU(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query avg CPU")
		return
	}
	RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetAvgMemory(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	resp, err := h.Service.GetAvgMemory(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query avg Memory")
		return
	}
	RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetAvgNetwork(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	resp, err := h.Service.GetAvgNetwork(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query avg Network")
		return
	}
	RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetAvgConnPool(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	resp, err := h.Service.GetAvgConnPool(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query avg Conn Pool")
		return
	}
	RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetCPUUsagePercentage(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	resp, err := h.Service.GetCPUUsagePercentage(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query CPU usage percentage")
		return
	}
	RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetMemoryUsagePercentage(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	resp, err := h.Service.GetMemoryUsagePercentage(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query Memory usage percentage")
		return
	}
	RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetByService(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	resp, err := h.Service.GetResourceUsageByService(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query resource usage by service")
		return
	}
	RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetByInstance(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	resp, err := h.Service.GetResourceUsageByInstance(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query resource usage by instance")
		return
	}
	RespondOK(c, resp)
}

// ─── System Infrastructure Handlers ──────────────────────────────────────────

func (h *ResourceUtilisationHandler) GetCPUTime(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetCPUTime(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query CPU time")
		return
	}
	RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetMemoryUsage(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetMemoryUsage(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query memory usage")
		return
	}
	RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetSwapUsage(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetSwapUsage(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query swap usage")
		return
	}
	RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetDiskIO(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetDiskIO(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query disk I/O")
		return
	}
	RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetDiskOperations(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetDiskOperations(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query disk operations")
		return
	}
	RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetDiskIOTime(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetDiskIOTime(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query disk I/O time")
		return
	}
	RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetFilesystemUsage(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetFilesystemUsage(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query filesystem usage")
		return
	}
	RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetFilesystemUtilization(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetFilesystemUtilization(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query filesystem utilization")
		return
	}
	RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetNetworkIO(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetNetworkIO(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query network I/O")
		return
	}
	RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetNetworkPackets(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetNetworkPackets(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query network packets")
		return
	}
	RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetNetworkErrors(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetNetworkErrors(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query network errors")
		return
	}
	RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetNetworkDropped(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetNetworkDropped(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query dropped packets")
		return
	}
	RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetLoadAverage(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetLoadAverage(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query load average")
		return
	}
	RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetProcessCount(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetProcessCount(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query process count")
		return
	}
	RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetNetworkConnections(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetNetworkConnections(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query network connections")
		return
	}
	RespondOK(c, resp)
}

// ─── JVM Handlers ─────────────────────────────────────────────────────────────

func (h *ResourceUtilisationHandler) GetJVMMemory(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetJVMMemory(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query JVM memory")
		return
	}
	RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetJVMGCDuration(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetJVMGCDuration(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query JVM GC duration")
		return
	}
	RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetJVMGCCollections(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetJVMGCCollections(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query JVM GC collections")
		return
	}
	RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetJVMThreadCount(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetJVMThreadCount(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query JVM thread count")
		return
	}
	RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetJVMClasses(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetJVMClasses(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query JVM classes")
		return
	}
	RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetJVMCPU(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetJVMCPU(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query JVM CPU")
		return
	}
	RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetJVMBuffers(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetJVMBuffers(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query JVM buffers")
		return
	}
	RespondOK(c, resp)
}
