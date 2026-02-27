package saturation

import (
	"net/http"

	"github.com/gin-gonic/gin"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
	"github.com/observability/observability-backend-go/internal/modules/saturation/service"
	. "github.com/observability/observability-backend-go/internal/platform/handlers"
)

// SaturationHandler handles saturation page endpoints.
type SaturationHandler struct {
	modulecommon.DBTenant
	Service *service.SaturationService
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

// GetInsightDatabaseCache — DB query latency and cache-hit ratio insights.
func (h *SaturationHandler) GetInsightDatabaseCache(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 24*60*60*1000)

	resp, err := h.Service.GetInsightDatabaseCache(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query database cache insights")
		return
	}

	RespondOK(c, resp)
}

// GetInsightMessagingQueue — queue depth, consumer lag, and message rate insights.
func (h *SaturationHandler) GetInsightMessagingQueue(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 24*60*60*1000)

	resp, err := h.Service.GetInsightMessagingQueue(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query messaging insights")
		return
	}

	RespondOK(c, resp)
}
