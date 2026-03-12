package kafka

import (
	"net/http"

	"github.com/gin-gonic/gin"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

type KafkaHandler struct {
	modulecommon.DBTenant
	Service *KafkaService
}

func (h *KafkaHandler) GetKafkaSummaryStats(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetKafkaSummaryStats(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query kafka summary stats")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *KafkaHandler) GetProduceRateByTopic(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetProduceRateByTopic(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query produce rate by topic")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *KafkaHandler) GetPublishLatencyByTopic(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetPublishLatencyByTopic(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query publish latency by topic")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *KafkaHandler) GetConsumeRateByTopic(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetConsumeRateByTopic(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query consume rate by topic")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *KafkaHandler) GetReceiveLatencyByTopic(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetReceiveLatencyByTopic(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query receive latency by topic")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *KafkaHandler) GetConsumeRateByGroup(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetConsumeRateByGroup(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query consume rate by group")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *KafkaHandler) GetProcessRateByGroup(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetProcessRateByGroup(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query process rate by group")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *KafkaHandler) GetProcessLatencyByGroup(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetProcessLatencyByGroup(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query process latency by group")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *KafkaHandler) GetConsumerLagByGroup(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetConsumerLagByGroup(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query consumer lag by group")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *KafkaHandler) GetConsumerLagPerPartition(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetConsumerLagPerPartition(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query consumer lag per partition")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *KafkaHandler) GetRebalanceSignals(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetRebalanceSignals(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query rebalance signals")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *KafkaHandler) GetE2ELatency(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetE2ELatency(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query end-to-end latency")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *KafkaHandler) GetPublishErrors(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetPublishErrors(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query publish errors")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *KafkaHandler) GetConsumeErrors(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetConsumeErrors(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query consume errors")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *KafkaHandler) GetProcessErrors(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetProcessErrors(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query process errors")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *KafkaHandler) GetClientOpErrors(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetClientOpErrors(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query client operation errors")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *KafkaHandler) GetBrokerConnections(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetBrokerConnections(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query broker connections")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *KafkaHandler) GetClientOperationDuration(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetClientOperationDuration(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query client operation duration")
		return
	}
	modulecommon.RespondOK(c, resp)
}
