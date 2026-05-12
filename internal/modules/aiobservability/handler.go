package aiobservability

import (
	"context"
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/shared/errorcode"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type Handler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *Handler) GetLLMRequestRateByModel(c *gin.Context) {
	h.handle(c, "Failed to query AI LLM request rate by model", func(ctx context.Context, q QueryOptions) (any, error) {
		return h.Service.GetLLMRequestRateByModel(ctx, q)
	})
}

func (h *Handler) GetLLMLatencyByModel(c *gin.Context) {
	h.handle(c, "Failed to query AI LLM latency by model", func(ctx context.Context, q QueryOptions) (any, error) {
		return h.Service.GetLLMLatencyByModel(ctx, q)
	})
}

func (h *Handler) GetLLMErrorRateByModel(c *gin.Context) {
	h.handle(c, "Failed to query AI LLM error rate by model", func(ctx context.Context, q QueryOptions) (any, error) {
		return h.Service.GetLLMErrorRateByModel(ctx, q)
	})
}

func (h *Handler) GetLLMTokenUsageByModel(c *gin.Context) {
	h.handle(c, "Failed to query AI LLM token usage by model", func(ctx context.Context, q QueryOptions) (any, error) {
		return h.Service.GetLLMTokenUsageByModel(ctx, q)
	})
}

func (h *Handler) GetLLMTokenUsageByProvider(c *gin.Context) {
	h.handle(c, "Failed to query AI LLM token usage by provider", func(ctx context.Context, q QueryOptions) (any, error) {
		return h.Service.GetLLMTokenUsageByProvider(ctx, q)
	})
}

func (h *Handler) GetLLMCostByModel(c *gin.Context) {
	h.handle(c, "Failed to query AI LLM cost by model", func(ctx context.Context, q QueryOptions) (any, error) {
		return h.Service.GetLLMCostByModel(ctx, q)
	})
}

func (h *Handler) GetLLMCostByProvider(c *gin.Context) {
	h.handle(c, "Failed to query AI LLM cost by provider", func(ctx context.Context, q QueryOptions) (any, error) {
		return h.Service.GetLLMCostByProvider(ctx, q)
	})
}

func (h *Handler) GetTopExpensiveCalls(c *gin.Context) {
	h.handle(c, "Failed to query top expensive AI calls", func(ctx context.Context, q QueryOptions) (any, error) {
		return h.Service.GetTopExpensiveCalls(ctx, q)
	})
}

func (h *Handler) GetTopSlowCalls(c *gin.Context) {
	h.handle(c, "Failed to query top slow AI calls", func(ctx context.Context, q QueryOptions) (any, error) {
		return h.Service.GetTopSlowCalls(ctx, q)
	})
}

func (h *Handler) GetPromptUsageByPrompt(c *gin.Context) {
	h.handle(c, "Failed to query AI prompt usage by prompt", func(ctx context.Context, q QueryOptions) (any, error) {
		return h.Service.GetPromptUsageByPrompt(ctx, q)
	})
}

func (h *Handler) GetPromptUsageByVersion(c *gin.Context) {
	h.handle(c, "Failed to query AI prompt usage by version", func(ctx context.Context, q QueryOptions) (any, error) {
		return h.Service.GetPromptUsageByVersion(ctx, q)
	})
}

func (h *Handler) GetPromptLatencyByVersion(c *gin.Context) {
	h.handle(c, "Failed to query AI prompt latency by version", func(ctx context.Context, q QueryOptions) (any, error) {
		return h.Service.GetPromptLatencyByVersion(ctx, q)
	})
}

func (h *Handler) GetPromptTokenUsageByVersion(c *gin.Context) {
	h.handle(c, "Failed to query AI prompt token usage by version", func(ctx context.Context, q QueryOptions) (any, error) {
		return h.Service.GetPromptTokenUsageByVersion(ctx, q)
	})
}

func (h *Handler) GetPromptCostByVersion(c *gin.Context) {
	h.handle(c, "Failed to query AI prompt cost by version", func(ctx context.Context, q QueryOptions) (any, error) {
		return h.Service.GetPromptCostByVersion(ctx, q)
	})
}

func (h *Handler) GetPromptTraces(c *gin.Context) {
	h.handle(c, "Failed to query AI prompt traces", func(ctx context.Context, q QueryOptions) (any, error) {
		return h.Service.GetPromptTraces(ctx, q)
	})
}

func (h *Handler) GetAgentRunsByAgent(c *gin.Context) {
	h.handle(c, "Failed to query AI agent runs by agent", func(ctx context.Context, q QueryOptions) (any, error) {
		return h.Service.GetAgentRunsByAgent(ctx, q)
	})
}

func (h *Handler) GetToolCallsByTool(c *gin.Context) {
	h.handle(c, "Failed to query AI tool calls by tool", func(ctx context.Context, q QueryOptions) (any, error) {
		return h.Service.GetToolCallsByTool(ctx, q)
	})
}

func (h *Handler) GetToolErrorsByTool(c *gin.Context) {
	h.handle(c, "Failed to query AI tool errors by tool", func(ctx context.Context, q QueryOptions) (any, error) {
		return h.Service.GetToolErrorsByTool(ctx, q)
	})
}

func (h *Handler) GetToolLatencyByTool(c *gin.Context) {
	h.handle(c, "Failed to query AI tool latency by tool", func(ctx context.Context, q QueryOptions) (any, error) {
		return h.Service.GetToolLatencyByTool(ctx, q)
	})
}

func (h *Handler) GetRetrievalRequestRateByStore(c *gin.Context) {
	h.handle(c, "Failed to query AI retrieval request rate by store", func(ctx context.Context, q QueryOptions) (any, error) {
		return h.Service.GetRetrievalRequestRateByStore(ctx, q)
	})
}

func (h *Handler) GetRetrievalLatencyByStore(c *gin.Context) {
	h.handle(c, "Failed to query AI retrieval latency by store", func(ctx context.Context, q QueryOptions) (any, error) {
		return h.Service.GetRetrievalLatencyByStore(ctx, q)
	})
}

func (h *Handler) GetRetrievalErrorsByStore(c *gin.Context) {
	h.handle(c, "Failed to query AI retrieval errors by store", func(ctx context.Context, q QueryOptions) (any, error) {
		return h.Service.GetRetrievalErrorsByStore(ctx, q)
	})
}

func (h *Handler) QueryTraces(c *gin.Context) {
	h.handle(c, "Failed to query AI traces", func(ctx context.Context, q QueryOptions) (any, error) {
		return h.Service.QueryTraces(ctx, q)
	})
}

func (h *Handler) GetFacets(c *gin.Context) {
	h.handle(c, "Failed to query AI facets", func(ctx context.Context, q QueryOptions) (any, error) {
		return h.Service.GetFacets(ctx, q)
	})
}

func (h *Handler) handle(
	c *gin.Context,
	errMessage string,
	query func(context.Context, QueryOptions) (any, error),
) {
	q, ok := h.queryOptions(c)
	if !ok {
		return
	}
	resp, err := query(c.Request.Context(), q)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, errMessage, err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) queryOptions(c *gin.Context) (QueryOptions, bool) {
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return QueryOptions{}, false
	}
	return QueryOptions{
		TeamID:  h.GetTenant(c).TeamID,
		StartMs: startMs,
		EndMs:   endMs,
		Limit:   modulecommon.ParsePageSize(c, "limit", 100),
		Filters: Filters{
			Providers:      listAny(c, "provider", "providers"),
			Models:         listAny(c, "model", "models"),
			Operations:     listAny(c, "operation", "operations"),
			Services:       listAny(c, "service", "services"),
			Environments:   listAny(c, "environment", "environments"),
			PromptNames:    listAny(c, "promptName", "promptNames"),
			PromptVersions: listAny(c, "promptVersion", "promptVersions"),
			AgentNames:     listAny(c, "agentName", "agentNames"),
			ToolNames:      listAny(c, "toolName", "toolNames"),
			DataSources:    listAny(c, "dataSource", "dataSources", "store", "stores"),
		},
	}, true
}

func listAny(c *gin.Context, keys ...string) []string {
	var out []string
	seen := make(map[string]struct{})
	for _, k := range keys {
		for _, v := range modulecommon.ParseListParam(c, k) {
			if _, ok := seen[v]; ok {
				continue
			}
			seen[v] = struct{}{}
			out = append(out, v)
		}
	}
	return out
}
