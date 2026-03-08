package kafka

import (
	"net/http"

	"github.com/gin-gonic/gin"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

// KafkaHandler handles kafka saturation endpoints.
type KafkaHandler struct {
	modulecommon.DBTenant
	Service *KafkaService
}

// GetKafkaQueueLag returns the lag per Kafka queue.
func (h *KafkaHandler) GetKafkaQueueLag(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	rows, err := h.Service.GetKafkaQueueLag(teamUUID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query kafka queue lag")
		return
	}

	modulecommon.RespondOK(c, rows)
}

// GetKafkaProductionRate returns the production rate per Kafka queue.
func (h *KafkaHandler) GetKafkaProductionRate(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	rows, err := h.Service.GetKafkaProductionRate(teamUUID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query kafka production rate")
		return
	}

	modulecommon.RespondOK(c, rows)
}

// GetKafkaConsumptionRate returns the consumption rate per Kafka queue.
func (h *KafkaHandler) GetKafkaConsumptionRate(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	rows, err := h.Service.GetKafkaConsumptionRate(teamUUID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query kafka consumption rate")
		return
	}

	modulecommon.RespondOK(c, rows)
}

// GetQueueConsumerLag returns consumer lag timeseries.
func (h *KafkaHandler) GetQueueConsumerLag(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	resp, err := h.Service.GetQueueConsumerLag(teamUUID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query queue consumer lag")
		return
	}

	modulecommon.RespondOK(c, resp)
}

// GetQueueTopicLag returns queue depth timeseries.
func (h *KafkaHandler) GetQueueTopicLag(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	resp, err := h.Service.GetQueueTopicLag(teamUUID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query queue topic lag")
		return
	}

	modulecommon.RespondOK(c, resp)
}

// GetQueueTopQueues returns the aggregated queues stats.
func (h *KafkaHandler) GetQueueTopQueues(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	resp, err := h.Service.GetQueueTopQueues(teamUUID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query top queues")
		return
	}

	modulecommon.RespondOK(c, resp)
}

// ─── OTel messaging.* standard metrics ────────────────────────────────────────

func (h *KafkaHandler) GetConsumerLagPerPartition(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetConsumerLagPerPartition(teamUUID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query consumer lag per partition")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *KafkaHandler) GetMessageRates(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetMessageRates(teamUUID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query message rates")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *KafkaHandler) GetOperationDuration(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetOperationDuration(teamUUID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query operation duration")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *KafkaHandler) GetOffsetCommitRate(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetOffsetCommitRate(teamUUID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query offset commit rate")
		return
	}
	modulecommon.RespondOK(c, resp)
}
