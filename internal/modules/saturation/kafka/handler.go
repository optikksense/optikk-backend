package kafka

import (
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts/errorcode"

	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
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

func (h *KafkaHandler) handleRangeQuery(
	c *gin.Context,
	errMessage string,
	query func(teamID, startMs, endMs int64, filters KafkaFilters) (any, error),
) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := query(teamID, startMs, endMs, parseKafkaFilters(c))
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, errMessage, err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *KafkaHandler) GetKafkaSummaryStats(c *gin.Context) {
	h.handleRangeQuery(c, "Failed to query kafka summary stats", func(teamID, startMs, endMs int64, filters KafkaFilters) (any, error) {
		return h.Service.GetKafkaSummaryStats(teamID, startMs, endMs, filters)
	})
}

func (h *KafkaHandler) GetProduceRateByTopic(c *gin.Context) {
	h.handleRangeQuery(c, "Failed to query produce rate by topic", func(teamID, startMs, endMs int64, filters KafkaFilters) (any, error) {
		return h.Service.GetProduceRateByTopic(teamID, startMs, endMs, filters)
	})
}

func (h *KafkaHandler) GetPublishLatencyByTopic(c *gin.Context) {
	h.handleRangeQuery(c, "Failed to query publish latency by topic", func(teamID, startMs, endMs int64, filters KafkaFilters) (any, error) {
		return h.Service.GetPublishLatencyByTopic(teamID, startMs, endMs, filters)
	})
}

func (h *KafkaHandler) GetConsumeRateByTopic(c *gin.Context) {
	h.handleRangeQuery(c, "Failed to query consume rate by topic", func(teamID, startMs, endMs int64, filters KafkaFilters) (any, error) {
		return h.Service.GetConsumeRateByTopic(teamID, startMs, endMs, filters)
	})
}

func (h *KafkaHandler) GetReceiveLatencyByTopic(c *gin.Context) {
	h.handleRangeQuery(c, "Failed to query receive latency by topic", func(teamID, startMs, endMs int64, filters KafkaFilters) (any, error) {
		return h.Service.GetReceiveLatencyByTopic(teamID, startMs, endMs, filters)
	})
}

func (h *KafkaHandler) GetConsumeRateByGroup(c *gin.Context) {
	h.handleRangeQuery(c, "Failed to query consume rate by group", func(teamID, startMs, endMs int64, filters KafkaFilters) (any, error) {
		return h.Service.GetConsumeRateByGroup(teamID, startMs, endMs, filters)
	})
}

func (h *KafkaHandler) GetProcessRateByGroup(c *gin.Context) {
	h.handleRangeQuery(c, "Failed to query process rate by group", func(teamID, startMs, endMs int64, filters KafkaFilters) (any, error) {
		return h.Service.GetProcessRateByGroup(teamID, startMs, endMs, filters)
	})
}

func (h *KafkaHandler) GetProcessLatencyByGroup(c *gin.Context) {
	h.handleRangeQuery(c, "Failed to query process latency by group", func(teamID, startMs, endMs int64, filters KafkaFilters) (any, error) {
		return h.Service.GetProcessLatencyByGroup(teamID, startMs, endMs, filters)
	})
}

func (h *KafkaHandler) GetConsumerLagByGroup(c *gin.Context) {
	h.handleRangeQuery(c, "Failed to query consumer lag by group", func(teamID, startMs, endMs int64, filters KafkaFilters) (any, error) {
		return h.Service.GetConsumerLagByGroup(teamID, startMs, endMs, filters)
	})
}

func (h *KafkaHandler) GetConsumerLagPerPartition(c *gin.Context) {
	h.handleRangeQuery(c, "Failed to query consumer lag per partition", func(teamID, startMs, endMs int64, filters KafkaFilters) (any, error) {
		return h.Service.GetConsumerLagPerPartition(teamID, startMs, endMs, filters)
	})
}

func (h *KafkaHandler) GetRebalanceSignals(c *gin.Context) {
	h.handleRangeQuery(c, "Failed to query rebalance signals", func(teamID, startMs, endMs int64, filters KafkaFilters) (any, error) {
		return h.Service.GetRebalanceSignals(teamID, startMs, endMs, filters)
	})
}

func (h *KafkaHandler) GetE2ELatency(c *gin.Context) {
	h.handleRangeQuery(c, "Failed to query end-to-end latency", func(teamID, startMs, endMs int64, filters KafkaFilters) (any, error) {
		return h.Service.GetE2ELatency(teamID, startMs, endMs, filters)
	})
}

func (h *KafkaHandler) GetPublishErrors(c *gin.Context) {
	h.handleRangeQuery(c, "Failed to query publish errors", func(teamID, startMs, endMs int64, filters KafkaFilters) (any, error) {
		return h.Service.GetPublishErrors(teamID, startMs, endMs, filters)
	})
}

func (h *KafkaHandler) GetConsumeErrors(c *gin.Context) {
	h.handleRangeQuery(c, "Failed to query consume errors", func(teamID, startMs, endMs int64, filters KafkaFilters) (any, error) {
		return h.Service.GetConsumeErrors(teamID, startMs, endMs, filters)
	})
}

func (h *KafkaHandler) GetProcessErrors(c *gin.Context) {
	h.handleRangeQuery(c, "Failed to query process errors", func(teamID, startMs, endMs int64, filters KafkaFilters) (any, error) {
		return h.Service.GetProcessErrors(teamID, startMs, endMs, filters)
	})
}

func (h *KafkaHandler) GetClientOpErrors(c *gin.Context) {
	h.handleRangeQuery(c, "Failed to query client operation errors", func(teamID, startMs, endMs int64, filters KafkaFilters) (any, error) {
		return h.Service.GetClientOpErrors(teamID, startMs, endMs, filters)
	})
}

func (h *KafkaHandler) GetBrokerConnections(c *gin.Context) {
	h.handleRangeQuery(c, "Failed to query broker connections", func(teamID, startMs, endMs int64, filters KafkaFilters) (any, error) {
		return h.Service.GetBrokerConnections(teamID, startMs, endMs, filters)
	})
}

func (h *KafkaHandler) GetClientOperationDuration(c *gin.Context) {
	h.handleRangeQuery(c, "Failed to query client operation duration", func(teamID, startMs, endMs int64, filters KafkaFilters) (any, error) {
		return h.Service.GetClientOperationDuration(teamID, startMs, endMs, filters)
	})
}
