package kafka

import (
	"net/http"

	"github.com/observability/observability-backend-go/internal/contracts/errorcode"

	"github.com/gin-gonic/gin"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

type KafkaHandler struct {
	modulecommon.DBTenant
	Service *KafkaService
}

func parseKafkaFilters(c *gin.Context) KafkaFilters {
	return KafkaFilters{
		Topic: c.Query("topic"),
		Group: c.Query("group"),
	}
}

func (h *KafkaHandler) GetKafkaSummaryStats(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	f := parseKafkaFilters(c)
	resp, err := h.Service.GetKafkaSummaryStats(teamID, startMs, endMs, f)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query kafka summary stats", err)
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
	f := parseKafkaFilters(c)
	resp, err := h.Service.GetProduceRateByTopic(teamID, startMs, endMs, f)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query produce rate by topic", err)
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
	f := parseKafkaFilters(c)
	resp, err := h.Service.GetPublishLatencyByTopic(teamID, startMs, endMs, f)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query publish latency by topic", err)
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
	f := parseKafkaFilters(c)
	resp, err := h.Service.GetConsumeRateByTopic(teamID, startMs, endMs, f)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query consume rate by topic", err)
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
	f := parseKafkaFilters(c)
	resp, err := h.Service.GetReceiveLatencyByTopic(teamID, startMs, endMs, f)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query receive latency by topic", err)
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
	f := parseKafkaFilters(c)
	resp, err := h.Service.GetConsumeRateByGroup(teamID, startMs, endMs, f)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query consume rate by group", err)
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
	f := parseKafkaFilters(c)
	resp, err := h.Service.GetProcessRateByGroup(teamID, startMs, endMs, f)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query process rate by group", err)
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
	f := parseKafkaFilters(c)
	resp, err := h.Service.GetProcessLatencyByGroup(teamID, startMs, endMs, f)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query process latency by group", err)
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
	f := parseKafkaFilters(c)
	resp, err := h.Service.GetConsumerLagByGroup(teamID, startMs, endMs, f)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query consumer lag by group", err)
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
	f := parseKafkaFilters(c)
	resp, err := h.Service.GetConsumerLagPerPartition(teamID, startMs, endMs, f)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query consumer lag per partition", err)
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
	f := parseKafkaFilters(c)
	resp, err := h.Service.GetRebalanceSignals(teamID, startMs, endMs, f)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query rebalance signals", err)
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
	f := parseKafkaFilters(c)
	resp, err := h.Service.GetE2ELatency(teamID, startMs, endMs, f)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query end-to-end latency", err)
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
	f := parseKafkaFilters(c)
	resp, err := h.Service.GetPublishErrors(teamID, startMs, endMs, f)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query publish errors", err)
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
	f := parseKafkaFilters(c)
	resp, err := h.Service.GetConsumeErrors(teamID, startMs, endMs, f)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query consume errors", err)
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
	f := parseKafkaFilters(c)
	resp, err := h.Service.GetProcessErrors(teamID, startMs, endMs, f)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query process errors", err)
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
	f := parseKafkaFilters(c)
	resp, err := h.Service.GetClientOpErrors(teamID, startMs, endMs, f)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query client operation errors", err)
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
	f := parseKafkaFilters(c)
	resp, err := h.Service.GetBrokerConnections(teamID, startMs, endMs, f)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query broker connections", err)
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
	f := parseKafkaFilters(c)
	resp, err := h.Service.GetClientOperationDuration(teamID, startMs, endMs, f)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query client operation duration", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}
