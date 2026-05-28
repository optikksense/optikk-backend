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

func (h *Handler) GetTopicThroughput(c *gin.Context) {
	topic := c.Query("topic")
	h.handleRangeQuery(c, "Failed to query topic throughput", func(teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetTopicThroughput(c.Request.Context(), teamID, startMs, endMs, topic)
	})
}

func (h *Handler) GetTopicLag(c *gin.Context) {
	topic := c.Query("topic")
	h.handleRangeQuery(c, "Failed to query topic lag", func(teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetTopicLag(c.Request.Context(), teamID, startMs, endMs, topic)
	})
}

func (h *Handler) GetTopicConsumers(c *gin.Context) {
	topic := c.Query("topic")
	h.handleRangeQuery(c, "Failed to query topic consumers", func(teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetTopicConsumers(c.Request.Context(), teamID, startMs, endMs, topic)
	})
}

func (h *Handler) GetGroupPartitions(c *gin.Context) {
	group := c.Query("group")
	h.handleRangeQuery(c, "Failed to query group partitions", func(teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetGroupPartitions(c.Request.Context(), teamID, startMs, endMs, group)
	})
}

func (h *Handler) GetGroupCommits(c *gin.Context) {
	group := c.Query("group")
	h.handleRangeQuery(c, "Failed to query group commits", func(teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetGroupCommits(c.Request.Context(), teamID, startMs, endMs, group)
	})
}

func (h *Handler) GetGroupFetches(c *gin.Context) {
	group := c.Query("group")
	h.handleRangeQuery(c, "Failed to query group fetches", func(teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetGroupFetches(c.Request.Context(), teamID, startMs, endMs, group)
	})
}

func (h *Handler) GetGroupHealth(c *gin.Context) {
	group := c.Query("group")
	h.handleRangeQuery(c, "Failed to query group health", func(teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetGroupHealth(c.Request.Context(), teamID, startMs, endMs, group)
	})
}

func (h *Handler) GetTopicGroupThroughput(c *gin.Context) {
	topic, ok := requireQueryParam(c, "topic")
	if !ok {
		return
	}
	h.handleRangeQuery(c, "Failed to query topic group throughput", func(teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetTopicGroupThroughput(c.Request.Context(), teamID, startMs, endMs, topic)
	})
}

func (h *Handler) GetTopicGroupLag(c *gin.Context) {
	topic, ok := requireQueryParam(c, "topic")
	if !ok {
		return
	}
	h.handleRangeQuery(c, "Failed to query topic group lag", func(teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetTopicGroupLag(c.Request.Context(), teamID, startMs, endMs, topic)
	})
}

func (h *Handler) GetGroupTopics(c *gin.Context) {
	group, ok := requireQueryParam(c, "group")
	if !ok {
		return
	}
	h.handleRangeQuery(c, "Failed to query group topics", func(teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetGroupTopics(c.Request.Context(), teamID, startMs, endMs, group)
	})
}
