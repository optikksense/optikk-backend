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
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	rows, err := h.Service.GetDatabaseQueryByTable(teamUUID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query database query by table")
		return
	}

	modulecommon.RespondOK(c, rows)
}

// GetDatabaseAvgLatency returns latency metrics for the database over time.
func (h *DatabaseHandler) GetDatabaseAvgLatency(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	rows, err := h.Service.GetDatabaseAvgLatency(teamUUID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query database avg latency")
		return
	}

	modulecommon.RespondOK(c, rows)
}

// GetDatabaseCacheSummary returns DB query latency and cache-hit ratio insights.
func (h *DatabaseHandler) GetDatabaseCacheSummary(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	resp, err := h.Service.GetDatabaseCacheSummary(teamUUID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query database cache summary")
		return
	}

	modulecommon.RespondOK(c, resp)
}

// GetDatabaseSystems returns query counts and latencies per database system.
func (h *DatabaseHandler) GetDatabaseSystems(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	resp, err := h.Service.GetDatabaseSystems(teamUUID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query database systems")
		return
	}

	modulecommon.RespondOK(c, resp)
}

// GetDatabaseTopTables returns latency and cache miss metrics per table.
func (h *DatabaseHandler) GetDatabaseTopTables(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	resp, err := h.Service.GetDatabaseTopTables(teamUUID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query database top tables")
		return
	}

	modulecommon.RespondOK(c, resp)
}

// ─── db.client.* OTel standard metrics ────────────────────────────────────────

func (h *DatabaseHandler) GetConnectionCount(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetConnectionCount(teamUUID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query connection count")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *DatabaseHandler) GetConnectionWaitTime(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetConnectionWaitTime(teamUUID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query connection wait time")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *DatabaseHandler) GetConnectionPending(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetConnectionPending(teamUUID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query pending connections")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *DatabaseHandler) GetConnectionTimeouts(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetConnectionTimeouts(teamUUID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query connection timeouts")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *DatabaseHandler) GetQueryDuration(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetQueryDuration(teamUUID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query operation duration")
		return
	}
	modulecommon.RespondOK(c, resp)
}

// ─── Redis metrics ─────────────────────────────────────────────────────────────

func (h *DatabaseHandler) GetRedisCacheHitRate(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetRedisCacheHitRate(teamUUID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query Redis cache hit rate")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *DatabaseHandler) GetRedisConnectedClients(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetRedisConnectedClients(teamUUID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query Redis clients")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *DatabaseHandler) GetRedisMemoryUsed(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetRedisMemoryUsed(teamUUID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query Redis memory")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *DatabaseHandler) GetRedisMemoryFragmentation(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetRedisMemoryFragmentation(teamUUID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query Redis memory fragmentation")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *DatabaseHandler) GetRedisCommandRate(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetRedisCommandRate(teamUUID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query Redis commands")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *DatabaseHandler) GetRedisEvictions(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetRedisEvictions(teamUUID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query Redis evictions")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *DatabaseHandler) GetRedisKeyspaceSize(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetRedisKeyspaceSize(teamUUID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query Redis keyspace")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *DatabaseHandler) GetRedisKeyExpiries(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetRedisKeyExpiries(teamUUID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query Redis key expiries")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *DatabaseHandler) GetRedisReplicationLag(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetRedisReplicationLag(teamUUID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query Redis replication lag")
		return
	}
	modulecommon.RespondOK(c, resp)
}
