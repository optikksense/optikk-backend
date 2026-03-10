package database

import (
	"net/http"

	"github.com/gin-gonic/gin"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

// DatabaseHandler handles database saturation endpoints.
type DatabaseHandler struct {
	modulecommon.DBTenant
	Service *DatabaseService
}

// GetDatabaseQueryByTable returns query counts per database table.
func (h *DatabaseHandler) GetDatabaseQueryByTable(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	rows, err := h.Service.GetDatabaseQueryByTable(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query database query by table")
		return
	}

	modulecommon.RespondOK(c, rows)
}

// GetDatabaseAvgLatency returns latency metrics for the database over time.
func (h *DatabaseHandler) GetDatabaseAvgLatency(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	rows, err := h.Service.GetDatabaseAvgLatency(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query database avg latency")
		return
	}

	modulecommon.RespondOK(c, rows)
}

// GetDatabaseCacheSummary returns DB query latency and cache-hit ratio insights.
func (h *DatabaseHandler) GetDatabaseCacheSummary(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	resp, err := h.Service.GetDatabaseCacheSummary(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query database cache summary")
		return
	}

	modulecommon.RespondOK(c, resp)
}

// GetDatabaseSystems returns query counts and latencies per database system.
func (h *DatabaseHandler) GetDatabaseSystems(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	resp, err := h.Service.GetDatabaseSystems(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query database systems")
		return
	}

	modulecommon.RespondOK(c, resp)
}

// GetDatabaseTopTables returns latency and cache miss metrics per table.
func (h *DatabaseHandler) GetDatabaseTopTables(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	resp, err := h.Service.GetDatabaseTopTables(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query database top tables")
		return
	}

	modulecommon.RespondOK(c, resp)
}

// ─── db.client.* OTel standard metrics ────────────────────────────────────────

func (h *DatabaseHandler) GetConnectionCount(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetConnectionCount(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query connection count")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *DatabaseHandler) GetConnectionWaitTime(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetConnectionWaitTime(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query connection wait time")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *DatabaseHandler) GetConnectionPending(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetConnectionPending(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query pending connections")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *DatabaseHandler) GetConnectionTimeouts(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetConnectionTimeouts(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query connection timeouts")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *DatabaseHandler) GetQueryDuration(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetQueryDuration(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query operation duration")
		return
	}
	modulecommon.RespondOK(c, resp)
}

// ─── Redis metrics ─────────────────────────────────────────────────────────────

func (h *DatabaseHandler) GetRedisCacheHitRate(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetRedisCacheHitRate(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query Redis cache hit rate")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *DatabaseHandler) GetRedisConnectedClients(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetRedisConnectedClients(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query Redis clients")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *DatabaseHandler) GetRedisMemoryUsed(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetRedisMemoryUsed(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query Redis memory")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *DatabaseHandler) GetRedisMemoryFragmentation(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetRedisMemoryFragmentation(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query Redis memory fragmentation")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *DatabaseHandler) GetRedisCommandRate(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetRedisCommandRate(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query Redis commands")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *DatabaseHandler) GetRedisEvictions(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetRedisEvictions(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query Redis evictions")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *DatabaseHandler) GetRedisKeyspaceSize(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetRedisKeyspaceSize(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query Redis keyspace")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *DatabaseHandler) GetRedisKeyExpiries(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetRedisKeyExpiries(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query Redis key expiries")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *DatabaseHandler) GetRedisReplicationLag(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetRedisReplicationLag(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query Redis replication lag")
		return
	}
	modulecommon.RespondOK(c, resp)
}
