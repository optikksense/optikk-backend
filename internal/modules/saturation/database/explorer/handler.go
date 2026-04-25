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

func (h *Handler) GetDatastoreSummary(c *gin.Context) {
	h.handleRangeQuery(c, "Failed to query datastore summary", func(teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetDatastoreSummary(c.Request.Context(), teamID, startMs, endMs)
	})
}

func (h *Handler) GetDatastoreSystems(c *gin.Context) {
	h.handleRangeQuery(c, "Failed to query datastore systems", func(teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetDatastoreSystems(c.Request.Context(), teamID, startMs, endMs)
	})
}

func (h *Handler) GetDatastoreSystemOverview(c *gin.Context) {
	system, ok := requireQueryParam(c, "system")
	if !ok {
		return
	}
	h.handleRangeQuery(c, "Failed to query datastore system overview", func(teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetDatastoreSystemOverview(c.Request.Context(), teamID, startMs, endMs, system)
	})
}

func (h *Handler) GetDatastoreSystemServers(c *gin.Context) {
	system, ok := requireQueryParam(c, "system")
	if !ok {
		return
	}
	h.handleRangeQuery(c, "Failed to query datastore system servers", func(teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetDatastoreSystemServers(c.Request.Context(), teamID, startMs, endMs, system)
	})
}

func (h *Handler) GetDatastoreSystemNamespaces(c *gin.Context) {
	system, ok := requireQueryParam(c, "system")
	if !ok {
		return
	}
	h.handleRangeQuery(c, "Failed to query datastore system namespaces", func(teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetDatastoreSystemNamespaces(c.Request.Context(), teamID, startMs, endMs, system)
	})
}

func (h *Handler) GetDatastoreSystemOperations(c *gin.Context) {
	system, ok := requireQueryParam(c, "system")
	if !ok {
		return
	}
	h.handleRangeQuery(c, "Failed to query datastore system operations", func(teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetDatastoreSystemOperations(c.Request.Context(), teamID, startMs, endMs, system)
	})
}

func (h *Handler) GetDatastoreSystemErrors(c *gin.Context) {
	system, ok := requireQueryParam(c, "system")
	if !ok {
		return
	}
	h.handleRangeQuery(c, "Failed to query datastore system errors", func(teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetDatastoreSystemErrors(c.Request.Context(), teamID, startMs, endMs, system)
	})
}

func (h *Handler) GetDatastoreSystemConnections(c *gin.Context) {
	system, ok := requireQueryParam(c, "system")
	if !ok {
		return
	}
	h.handleRangeQuery(c, "Failed to query datastore system connections", func(teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetDatastoreSystemConnections(c.Request.Context(), teamID, startMs, endMs, system)
	})
}

func (h *Handler) GetDatastoreSystemSlowQueries(c *gin.Context) {
	system, ok := requireQueryParam(c, "system")
	if !ok {
		return
	}
	h.handleRangeQuery(c, "Failed to query datastore system slow queries", func(teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetDatastoreSystemSlowQueries(c.Request.Context(), teamID, startMs, endMs, system)
	})
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
