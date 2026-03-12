package database

import "github.com/gin-gonic/gin"

// Config controls whether this module's routes are registered.
type Config struct {
	Enabled bool
}

func DefaultConfig() Config {
	return Config{Enabled: true}
}

// RegisterRoutes registers all /database/* routes on the supplied router group.
func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *Handler) {
	if !cfg.Enabled || h == nil {
		return
	}

	g := v1.Group("/database")

	// Section 1 — Summary
	g.GET("/summary", h.GetSummaryStats)

	// Section 2 — Detected Systems
	g.GET("/systems", h.GetDetectedSystems)

	// Section 3 — Query Latency
	g.GET("/latency/by-system", h.GetLatencyBySystem)
	g.GET("/latency/by-operation", h.GetLatencyByOperation)
	g.GET("/latency/by-collection", h.GetLatencyByCollection)
	g.GET("/latency/by-namespace", h.GetLatencyByNamespace)
	g.GET("/latency/by-server", h.GetLatencyByServer)
	g.GET("/latency/heatmap", h.GetLatencyHeatmap)

	// Section 4 — Query Volume
	g.GET("/ops/by-system", h.GetOpsBySystem)
	g.GET("/ops/by-operation", h.GetOpsByOperation)
	g.GET("/ops/by-collection", h.GetOpsByCollection)
	g.GET("/ops/read-vs-write", h.GetReadVsWrite)
	g.GET("/ops/by-namespace", h.GetOpsByNamespace)

	// Section 5 — Slow Queries
	g.GET("/slow-queries/patterns", h.GetSlowQueryPatterns)
	g.GET("/slow-queries/collections", h.GetSlowestCollections)
	g.GET("/slow-queries/rate", h.GetSlowQueryRate)
	g.GET("/slow-queries/p99-by-text", h.GetP99ByQueryText)

	// Section 6 — Error Rates
	g.GET("/errors/by-system", h.GetErrorsBySystem)
	g.GET("/errors/by-operation", h.GetErrorsByOperation)
	g.GET("/errors/by-error-type", h.GetErrorsByErrorType)
	g.GET("/errors/by-collection", h.GetErrorsByCollection)
	g.GET("/errors/by-status", h.GetErrorsByResponseStatus)
	g.GET("/errors/ratio", h.GetErrorRatio)

	// Sections 7 & 8 — Connection Pools
	g.GET("/connections/count", h.GetConnectionCountSeries)
	g.GET("/connections/utilization", h.GetConnectionUtilization)
	g.GET("/connections/limits", h.GetConnectionLimits)
	g.GET("/connections/pending", h.GetPendingRequests)
	g.GET("/connections/timeout-rate", h.GetConnectionTimeoutRate)
	g.GET("/connections/wait-time", h.GetConnectionWaitTime)
	g.GET("/connections/create-time", h.GetConnectionCreateTime)
	g.GET("/connections/use-time", h.GetConnectionUseTime)

	// Section 9 — Per-Collection Deep Dive
	g.GET("/collection/latency", h.GetCollectionLatency)
	g.GET("/collection/ops", h.GetCollectionOps)
	g.GET("/collection/errors", h.GetCollectionErrors)
	g.GET("/collection/query-texts", h.GetCollectionQueryTexts)
	g.GET("/collection/read-vs-write", h.GetCollectionReadVsWrite)

	// Section 10 — Per-System Deep Dive
	g.GET("/system/latency", h.GetSystemLatency)
	g.GET("/system/ops", h.GetSystemOps)
	g.GET("/system/top-collections-by-latency", h.GetSystemTopCollectionsByLatency)
	g.GET("/system/top-collections-by-volume", h.GetSystemTopCollectionsByVolume)
	g.GET("/system/errors", h.GetSystemErrors)
	g.GET("/system/namespaces", h.GetSystemNamespaces)
}
