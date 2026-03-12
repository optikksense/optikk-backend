package database

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	. "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

// Handler handles all database observability HTTP requests.
type Handler struct {
	modulecommon.DBTenant
	Service Service
}

// ---------------------------------------------------------------------------
// Param parsing helpers
// ---------------------------------------------------------------------------

// parseFilters reads optional repeated query params into a Filters struct.
func parseFilters(c *gin.Context) Filters {
	return Filters{
		DBSystem:   c.QueryArray("db_system"),
		Collection: c.QueryArray("collection"),
		Namespace:  c.QueryArray("namespace"),
		Server:     c.QueryArray("server"),
	}
}

func parseLimit(c *gin.Context, def int) int {
	if s := c.Query("limit"); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v > 0 {
			return v
		}
	}
	return def
}

func parseThreshold(c *gin.Context, def float64) float64 {
	if s := c.Query("threshold_ms"); s != "" {
		if v, err := strconv.ParseFloat(strings.TrimSpace(s), 64); err == nil && v > 0 {
			return v
		}
	}
	return def
}

func parseCollection(c *gin.Context) (string, bool) {
	v := c.Query("collection")
	if v == "" {
		RespondError(c, http.StatusBadRequest, "MISSING_PARAM", "collection query param is required")
		return "", false
	}
	return v, true
}

func parseDbSystem(c *gin.Context) (string, bool) {
	v := c.Query("db_system")
	if v == "" {
		RespondError(c, http.StatusBadRequest, "MISSING_PARAM", "db_system query param is required")
		return "", false
	}
	return v, true
}

// ---------------------------------------------------------------------------
// Section 1 — Summary
// ---------------------------------------------------------------------------

func (h *Handler) GetSummaryStats(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetSummaryStats(teamID, startMs, endMs, parseFilters(c))
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query database summary stats")
		return
	}
	RespondOK(c, resp)
}

// ---------------------------------------------------------------------------
// Section 2 — Detected Systems
// ---------------------------------------------------------------------------

func (h *Handler) GetDetectedSystems(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetDetectedSystems(teamID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query detected database systems")
		return
	}
	RespondOK(c, resp)
}

// ---------------------------------------------------------------------------
// Section 3 — Query Latency
// ---------------------------------------------------------------------------

func (h *Handler) GetLatencyBySystem(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetLatencyBySystem(teamID, startMs, endMs, parseFilters(c))
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query latency by system")
		return
	}
	RespondOK(c, resp)
}

func (h *Handler) GetLatencyByOperation(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetLatencyByOperation(teamID, startMs, endMs, parseFilters(c))
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query latency by operation")
		return
	}
	RespondOK(c, resp)
}

func (h *Handler) GetLatencyByCollection(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetLatencyByCollection(teamID, startMs, endMs, parseFilters(c))
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query latency by collection")
		return
	}
	RespondOK(c, resp)
}

func (h *Handler) GetLatencyByNamespace(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetLatencyByNamespace(teamID, startMs, endMs, parseFilters(c))
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query latency by namespace")
		return
	}
	RespondOK(c, resp)
}

func (h *Handler) GetLatencyByServer(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetLatencyByServer(teamID, startMs, endMs, parseFilters(c))
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query latency by server")
		return
	}
	RespondOK(c, resp)
}

func (h *Handler) GetLatencyHeatmap(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetLatencyHeatmap(teamID, startMs, endMs, parseFilters(c))
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query latency heatmap")
		return
	}
	RespondOK(c, resp)
}

// ---------------------------------------------------------------------------
// Section 4 — Query Volume
// ---------------------------------------------------------------------------

func (h *Handler) GetOpsBySystem(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetOpsBySystem(teamID, startMs, endMs, parseFilters(c))
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query ops by system")
		return
	}
	RespondOK(c, resp)
}

func (h *Handler) GetOpsByOperation(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetOpsByOperation(teamID, startMs, endMs, parseFilters(c))
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query ops by operation")
		return
	}
	RespondOK(c, resp)
}

func (h *Handler) GetOpsByCollection(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetOpsByCollection(teamID, startMs, endMs, parseFilters(c))
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query ops by collection")
		return
	}
	RespondOK(c, resp)
}

func (h *Handler) GetReadVsWrite(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetReadVsWrite(teamID, startMs, endMs, parseFilters(c))
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query read vs write ratio")
		return
	}
	RespondOK(c, resp)
}

func (h *Handler) GetOpsByNamespace(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetOpsByNamespace(teamID, startMs, endMs, parseFilters(c))
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query ops by namespace")
		return
	}
	RespondOK(c, resp)
}

// ---------------------------------------------------------------------------
// Section 5 — Slow Queries
// ---------------------------------------------------------------------------

func (h *Handler) GetSlowQueryPatterns(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetSlowQueryPatterns(teamID, startMs, endMs, parseFilters(c), parseLimit(c, 20))
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query slow query patterns")
		return
	}
	RespondOK(c, resp)
}

func (h *Handler) GetSlowestCollections(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetSlowestCollections(teamID, startMs, endMs, parseFilters(c))
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query slowest collections")
		return
	}
	RespondOK(c, resp)
}

func (h *Handler) GetSlowQueryRate(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetSlowQueryRate(teamID, startMs, endMs, parseFilters(c), parseThreshold(c, 100))
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query slow query rate")
		return
	}
	RespondOK(c, resp)
}

func (h *Handler) GetP99ByQueryText(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetP99ByQueryText(teamID, startMs, endMs, parseFilters(c), parseLimit(c, 10))
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query p99 by query text")
		return
	}
	RespondOK(c, resp)
}

// ---------------------------------------------------------------------------
// Section 6 — Error Rates
// ---------------------------------------------------------------------------

func (h *Handler) GetErrorsBySystem(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetErrorsBySystem(teamID, startMs, endMs, parseFilters(c))
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query errors by system")
		return
	}
	RespondOK(c, resp)
}

func (h *Handler) GetErrorsByOperation(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetErrorsByOperation(teamID, startMs, endMs, parseFilters(c))
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query errors by operation")
		return
	}
	RespondOK(c, resp)
}

func (h *Handler) GetErrorsByErrorType(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetErrorsByErrorType(teamID, startMs, endMs, parseFilters(c))
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query errors by error type")
		return
	}
	RespondOK(c, resp)
}

func (h *Handler) GetErrorsByCollection(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetErrorsByCollection(teamID, startMs, endMs, parseFilters(c))
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query errors by collection")
		return
	}
	RespondOK(c, resp)
}

func (h *Handler) GetErrorsByResponseStatus(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetErrorsByResponseStatus(teamID, startMs, endMs, parseFilters(c))
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query errors by response status")
		return
	}
	RespondOK(c, resp)
}

func (h *Handler) GetErrorRatio(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetErrorRatio(teamID, startMs, endMs, parseFilters(c))
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query error ratio")
		return
	}
	RespondOK(c, resp)
}

// ---------------------------------------------------------------------------
// Sections 7 & 8 — Connection Pools
// ---------------------------------------------------------------------------

func (h *Handler) GetConnectionCountSeries(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetConnectionCountSeries(teamID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query connection count series")
		return
	}
	RespondOK(c, resp)
}

func (h *Handler) GetConnectionUtilization(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetConnectionUtilization(teamID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query connection utilization")
		return
	}
	RespondOK(c, resp)
}

func (h *Handler) GetConnectionLimits(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetConnectionLimits(teamID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query connection limits")
		return
	}
	RespondOK(c, resp)
}

func (h *Handler) GetPendingRequests(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetPendingRequests(teamID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query pending connection requests")
		return
	}
	RespondOK(c, resp)
}

func (h *Handler) GetConnectionTimeoutRate(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetConnectionTimeoutRate(teamID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query connection timeout rate")
		return
	}
	RespondOK(c, resp)
}

func (h *Handler) GetConnectionWaitTime(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetConnectionWaitTime(teamID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query connection wait time")
		return
	}
	RespondOK(c, resp)
}

func (h *Handler) GetConnectionCreateTime(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetConnectionCreateTime(teamID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query connection create time")
		return
	}
	RespondOK(c, resp)
}

func (h *Handler) GetConnectionUseTime(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetConnectionUseTime(teamID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query connection use time")
		return
	}
	RespondOK(c, resp)
}

// ---------------------------------------------------------------------------
// Section 9 — Per-Collection Deep Dive
// ---------------------------------------------------------------------------

func (h *Handler) GetCollectionLatency(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	coll, ok := parseCollection(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetCollectionLatency(teamID, startMs, endMs, coll, parseFilters(c))
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query collection latency")
		return
	}
	RespondOK(c, resp)
}

func (h *Handler) GetCollectionOps(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	coll, ok := parseCollection(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetCollectionOps(teamID, startMs, endMs, coll, parseFilters(c))
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query collection ops")
		return
	}
	RespondOK(c, resp)
}

func (h *Handler) GetCollectionErrors(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	coll, ok := parseCollection(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetCollectionErrors(teamID, startMs, endMs, coll, parseFilters(c))
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query collection errors")
		return
	}
	RespondOK(c, resp)
}

func (h *Handler) GetCollectionQueryTexts(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	coll, ok := parseCollection(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetCollectionQueryTexts(teamID, startMs, endMs, coll, parseFilters(c), parseLimit(c, 10))
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query collection query texts")
		return
	}
	RespondOK(c, resp)
}

func (h *Handler) GetCollectionReadVsWrite(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	coll, ok := parseCollection(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetCollectionReadVsWrite(teamID, startMs, endMs, coll)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query collection read vs write")
		return
	}
	RespondOK(c, resp)
}

// ---------------------------------------------------------------------------
// Section 10 — Per-System Deep Dive
// ---------------------------------------------------------------------------

func (h *Handler) GetSystemLatency(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	sys, ok := parseDbSystem(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetSystemLatency(teamID, startMs, endMs, sys, parseFilters(c))
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query system latency")
		return
	}
	RespondOK(c, resp)
}

func (h *Handler) GetSystemOps(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	sys, ok := parseDbSystem(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetSystemOps(teamID, startMs, endMs, sys, parseFilters(c))
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query system ops")
		return
	}
	RespondOK(c, resp)
}

func (h *Handler) GetSystemTopCollectionsByLatency(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	sys, ok := parseDbSystem(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetSystemTopCollectionsByLatency(teamID, startMs, endMs, sys)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query system top collections by latency")
		return
	}
	RespondOK(c, resp)
}

func (h *Handler) GetSystemTopCollectionsByVolume(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	sys, ok := parseDbSystem(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetSystemTopCollectionsByVolume(teamID, startMs, endMs, sys)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query system top collections by volume")
		return
	}
	RespondOK(c, resp)
}

func (h *Handler) GetSystemErrors(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	sys, ok := parseDbSystem(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetSystemErrors(teamID, startMs, endMs, sys)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query system errors")
		return
	}
	RespondOK(c, resp)
}

func (h *Handler) GetSystemNamespaces(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	sys, ok := parseDbSystem(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetSystemNamespaces(teamID, startMs, endMs, sys)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query system namespaces")
		return
	}
	RespondOK(c, resp)
}
