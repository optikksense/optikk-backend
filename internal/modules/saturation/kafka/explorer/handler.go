package explorer

import (
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/shared/errorcode"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type Handler struct {
	modulecommon.DBTenant
	Service *Service
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
