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
	startMs, endMs := modulecommon.ParseRange(c, 60*60*1000)

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
	startMs, endMs := modulecommon.ParseRange(c, 60*60*1000)

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
	startMs, endMs := modulecommon.ParseRange(c, 24*60*60*1000)

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
	startMs, endMs := modulecommon.ParseRange(c, 24*60*60*1000)

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
	startMs, endMs := modulecommon.ParseRange(c, 24*60*60*1000)

	resp, err := h.Service.GetDatabaseTopTables(teamUUID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query database top tables")
		return
	}

	modulecommon.RespondOK(c, resp)
}
