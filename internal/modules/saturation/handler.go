package saturation

import (
	"net/http"

	"github.com/gin-gonic/gin"
	. "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

// SaturationHandler handles saturation page endpoints.
type SaturationHandler struct {
	modulecommon.DBTenant
	Service *SaturationService
}

// GetKafkaQueueLag returns the lag per Kafka queue.
func (h *SaturationHandler) GetKafkaQueueLag(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)

	rows, err := h.Service.GetKafkaQueueLag(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query kafka queue lag")
		return
	}

	RespondOK(c, rows)
}

// GetKafkaProductionRate returns the production rate per Kafka queue.
func (h *SaturationHandler) GetKafkaProductionRate(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)

	rows, err := h.Service.GetKafkaProductionRate(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query kafka production rate")
		return
	}

	RespondOK(c, rows)
}

// GetKafkaConsumptionRate returns the consumption rate per Kafka queue.
func (h *SaturationHandler) GetKafkaConsumptionRate(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)

	rows, err := h.Service.GetKafkaConsumptionRate(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query kafka consumption rate")
		return
	}

	RespondOK(c, rows)
}

// GetDatabaseQueryByTable returns query counts per database table.
func (h *SaturationHandler) GetDatabaseQueryByTable(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)

	rows, err := h.Service.GetDatabaseQueryByTable(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query database query by table")
		return
	}

	RespondOK(c, rows)
}

// GetDatabaseAvgLatency returns latency metrics for the database over time.
func (h *SaturationHandler) GetDatabaseAvgLatency(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)

	rows, err := h.Service.GetDatabaseAvgLatency(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query database avg latency")
		return
	}

	RespondOK(c, rows)
}

// GetDatabaseCacheSummary returns DB query latency and cache-hit ratio insights.
func (h *SaturationHandler) GetDatabaseCacheSummary(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 24*60*60*1000)

	resp, err := h.Service.GetDatabaseCacheSummary(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query database cache summary")
		return
	}

	RespondOK(c, resp)
}

// GetDatabaseSystems returns query counts and latencies per database system.
func (h *SaturationHandler) GetDatabaseSystems(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 24*60*60*1000)

	resp, err := h.Service.GetDatabaseSystems(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query database systems")
		return
	}

	RespondOK(c, resp)
}

// GetDatabaseTopTables returns latency and cache miss metrics per table.
func (h *SaturationHandler) GetDatabaseTopTables(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 24*60*60*1000)

	resp, err := h.Service.GetDatabaseTopTables(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query database top tables")
		return
	}

	RespondOK(c, resp)
}

// GetQueueConsumerLag returns consumer lag timeseries.
func (h *SaturationHandler) GetQueueConsumerLag(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 24*60*60*1000)

	resp, err := h.Service.GetQueueConsumerLag(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query queue consumer lag")
		return
	}

	RespondOK(c, resp)
}

// GetQueueTopicLag returns queue depth timeseries.
func (h *SaturationHandler) GetQueueTopicLag(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 24*60*60*1000)

	resp, err := h.Service.GetQueueTopicLag(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query queue topic lag")
		return
	}

	RespondOK(c, resp)
}

// GetQueueTopQueues returns the aggregated queues stats.
func (h *SaturationHandler) GetQueueTopQueues(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 24*60*60*1000)

	resp, err := h.Service.GetQueueTopQueues(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query top queues")
		return
	}

	RespondOK(c, resp)
}
