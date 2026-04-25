package kafka

import (
	"context"
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/shared/errorcode"

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
	query func(ctx context.Context, teamID, startMs, endMs int64, filters KafkaFilters) (any, error),
) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := query(c.Request.Context(), teamID, startMs, endMs, parseKafkaFilters(c))
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, errMessage, err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *KafkaHandler) GetKafkaSummaryStats(c *gin.Context) {
	h.handleRangeQuery(c, "Failed to query kafka summary stats", func(ctx context.Context, teamID, startMs, endMs int64, filters KafkaFilters) (any, error) {
		return h.Service.GetKafkaSummaryStats(ctx, teamID, startMs, endMs, filters)
	})
}

func (h *KafkaHandler) GetProduceRateByTopic(c *gin.Context) {
	h.handleRangeQuery(c, "Failed to query produce rate by topic", func(ctx context.Context, teamID, startMs, endMs int64, filters KafkaFilters) (any, error) {
		return h.Service.GetProduceRateByTopic(ctx, teamID, startMs, endMs, filters)
	})
}

func (h *KafkaHandler) GetPublishLatencyByTopic(c *gin.Context) {
	h.handleRangeQuery(c, "Failed to query publish latency by topic", func(ctx context.Context, teamID, startMs, endMs int64, filters KafkaFilters) (any, error) {
		return h.Service.GetPublishLatencyByTopic(ctx, teamID, startMs, endMs, filters)
	})
}

func (h *KafkaHandler) GetConsumeRateByTopic(c *gin.Context) {
	h.handleRangeQuery(c, "Failed to query consume rate by topic", func(ctx context.Context, teamID, startMs, endMs int64, filters KafkaFilters) (any, error) {
		return h.Service.GetConsumeRateByTopic(ctx, teamID, startMs, endMs, filters)
	})
}

func (h *KafkaHandler) GetReceiveLatencyByTopic(c *gin.Context) {
	h.handleRangeQuery(c, "Failed to query receive latency by topic", func(ctx context.Context, teamID, startMs, endMs int64, filters KafkaFilters) (any, error) {
		return h.Service.GetReceiveLatencyByTopic(ctx, teamID, startMs, endMs, filters)
	})
}

func (h *KafkaHandler) GetConsumeRateByGroup(c *gin.Context) {
	h.handleRangeQuery(c, "Failed to query consume rate by group", func(ctx context.Context, teamID, startMs, endMs int64, filters KafkaFilters) (any, error) {
		return h.Service.GetConsumeRateByGroup(ctx, teamID, startMs, endMs, filters)
	})
}

func (h *KafkaHandler) GetProcessRateByGroup(c *gin.Context) {
	h.handleRangeQuery(c, "Failed to query process rate by group", func(ctx context.Context, teamID, startMs, endMs int64, filters KafkaFilters) (any, error) {
		return h.Service.GetProcessRateByGroup(ctx, teamID, startMs, endMs, filters)
	})
}

func (h *KafkaHandler) GetProcessLatencyByGroup(c *gin.Context) {
	h.handleRangeQuery(c, "Failed to query process latency by group", func(ctx context.Context, teamID, startMs, endMs int64, filters KafkaFilters) (any, error) {
		return h.Service.GetProcessLatencyByGroup(ctx, teamID, startMs, endMs, filters)
	})
}

func (h *KafkaHandler) GetConsumerLagByGroup(c *gin.Context) {
	h.handleRangeQuery(c, "Failed to query consumer lag by group", func(ctx context.Context, teamID, startMs, endMs int64, filters KafkaFilters) (any, error) {
		return h.Service.GetConsumerLagByGroup(ctx, teamID, startMs, endMs, filters)
	})
}

func (h *KafkaHandler) GetConsumerLagPerPartition(c *gin.Context) {
	h.handleRangeQuery(c, "Failed to query consumer lag per partition", func(ctx context.Context, teamID, startMs, endMs int64, filters KafkaFilters) (any, error) {
		return h.Service.GetConsumerLagPerPartition(ctx, teamID, startMs, endMs, filters)
	})
}

func (h *KafkaHandler) GetRebalanceSignals(c *gin.Context) {
	h.handleRangeQuery(c, "Failed to query rebalance signals", func(ctx context.Context, teamID, startMs, endMs int64, filters KafkaFilters) (any, error) {
		return h.Service.GetRebalanceSignals(ctx, teamID, startMs, endMs, filters)
	})
}

func (h *KafkaHandler) GetE2ELatency(c *gin.Context) {
	h.handleRangeQuery(c, "Failed to query end-to-end latency", func(ctx context.Context, teamID, startMs, endMs int64, filters KafkaFilters) (any, error) {
		return h.Service.GetE2ELatency(ctx, teamID, startMs, endMs, filters)
	})
}

func (h *KafkaHandler) GetPublishErrors(c *gin.Context) {
	h.handleRangeQuery(c, "Failed to query publish errors", func(ctx context.Context, teamID, startMs, endMs int64, filters KafkaFilters) (any, error) {
		return h.Service.GetPublishErrors(ctx, teamID, startMs, endMs, filters)
	})
}

func (h *KafkaHandler) GetConsumeErrors(c *gin.Context) {
	h.handleRangeQuery(c, "Failed to query consume errors", func(ctx context.Context, teamID, startMs, endMs int64, filters KafkaFilters) (any, error) {
		return h.Service.GetConsumeErrors(ctx, teamID, startMs, endMs, filters)
	})
}

func (h *KafkaHandler) GetProcessErrors(c *gin.Context) {
	h.handleRangeQuery(c, "Failed to query process errors", func(ctx context.Context, teamID, startMs, endMs int64, filters KafkaFilters) (any, error) {
		return h.Service.GetProcessErrors(ctx, teamID, startMs, endMs, filters)
	})
}

func (h *KafkaHandler) GetClientOpErrors(c *gin.Context) {
	h.handleRangeQuery(c, "Failed to query client operation errors", func(ctx context.Context, teamID, startMs, endMs int64, filters KafkaFilters) (any, error) {
		return h.Service.GetClientOpErrors(ctx, teamID, startMs, endMs, filters)
	})
}

func (h *KafkaHandler) GetBrokerConnections(c *gin.Context) {
	h.handleRangeQuery(c, "Failed to query broker connections", func(ctx context.Context, teamID, startMs, endMs int64, filters KafkaFilters) (any, error) {
		return h.Service.GetBrokerConnections(ctx, teamID, startMs, endMs, filters)
	})
}

func (h *KafkaHandler) GetClientOperationDuration(c *gin.Context) {
	h.handleRangeQuery(c, "Failed to query client operation duration", func(ctx context.Context, teamID, startMs, endMs int64, filters KafkaFilters) (any, error) {
		return h.Service.GetClientOperationDuration(ctx, teamID, startMs, endMs, filters)
	})
}
