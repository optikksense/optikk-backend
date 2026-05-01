package explorer

import (
	"net/http"
	"strings"

	"github.com/Optikk-Org/optikk-backend/internal/shared/errorcode"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type Handler struct {
	modulecommon.DBTenant
	Service *Service
}

func requireQueryParam(c *gin.Context, key string) (string, bool) {
	value := strings.TrimSpace(c.Query(key))
	if value == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, "MISSING_PARAM", key+" query param is required")
		return "", false
	}
	return value, true
}

func (h *Handler) handleRangeQuery(
	c *gin.Context,
	errMessage string,
	query func(teamID, startMs, endMs int64) (any, error),
) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := query(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, errMessage, err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetKafkaSummary(c *gin.Context) {
	h.handleRangeQuery(c, "Failed to query kafka explorer summary", func(teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetKafkaSummary(c.Request.Context(), teamID, startMs, endMs)
	})
}

func (h *Handler) GetKafkaTopics(c *gin.Context) {
	h.handleRangeQuery(c, "Failed to query kafka topics", func(teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetKafkaTopics(c.Request.Context(), teamID, startMs, endMs)
	})
}

func (h *Handler) GetKafkaGroups(c *gin.Context) {
	h.handleRangeQuery(c, "Failed to query kafka consumer groups", func(teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetKafkaGroups(c.Request.Context(), teamID, startMs, endMs)
	})
}

func (h *Handler) GetKafkaTopicOverview(c *gin.Context) {
	topic, ok := requireQueryParam(c, "topic")
	if !ok {
		return
	}
	h.handleRangeQuery(c, "Failed to query kafka topic overview", func(teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetKafkaTopicOverview(c.Request.Context(), teamID, startMs, endMs, topic)
	})
}

func (h *Handler) GetKafkaTopicGroups(c *gin.Context) {
	topic, ok := requireQueryParam(c, "topic")
	if !ok {
		return
	}
	h.handleRangeQuery(c, "Failed to query kafka topic groups", func(teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetKafkaTopicGroups(c.Request.Context(), teamID, startMs, endMs, topic)
	})
}

func (h *Handler) GetKafkaTopicPartitions(c *gin.Context) {
	topic, ok := requireQueryParam(c, "topic")
	if !ok {
		return
	}
	h.handleRangeQuery(c, "Failed to query kafka topic partitions", func(teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetKafkaTopicPartitions(c.Request.Context(), teamID, startMs, endMs, topic)
	})
}

func (h *Handler) GetKafkaGroupOverview(c *gin.Context) {
	group, ok := requireQueryParam(c, "group")
	if !ok {
		return
	}
	h.handleRangeQuery(c, "Failed to query kafka group overview", func(teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetKafkaGroupOverview(c.Request.Context(), teamID, startMs, endMs, group)
	})
}

func (h *Handler) GetKafkaGroupTopics(c *gin.Context) {
	group, ok := requireQueryParam(c, "group")
	if !ok {
		return
	}
	h.handleRangeQuery(c, "Failed to query kafka group topics", func(teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetKafkaGroupTopics(c.Request.Context(), teamID, startMs, endMs, group)
	})
}

func (h *Handler) GetKafkaGroupPartitions(c *gin.Context) {
	group, ok := requireQueryParam(c, "group")
	if !ok {
		return
	}
	h.handleRangeQuery(c, "Failed to query kafka group partitions", func(teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetKafkaGroupPartitions(c.Request.Context(), teamID, startMs, endMs, group)
	})
}
