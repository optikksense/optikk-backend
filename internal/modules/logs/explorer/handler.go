package explorer

import (
	"net/http"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	shared "github.com/Optikk-Org/optikk-backend/internal/modules/logs/internal/shared"
	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts/errorcode"

	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type Handler struct {
	modulecommon.DBTenant
	Service  *Service
	LogStats *LogStatsService
	db       *dbutil.NativeQuerier
}

func NewHandler(getTenant modulecommon.GetTenantFunc, service *Service, logStats *LogStatsService, db *dbutil.NativeQuerier) *Handler {
	return &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  service,
		LogStats: logStats,
		db:       db,
	}
}

func (h *Handler) Query(c *gin.Context) {
	var req QueryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Invalid request body")
		return
	}
	if req.StartTime <= 0 || req.EndTime <= 0 || req.StartTime >= req.EndTime {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Valid startTime and endTime are required")
		return
	}

	resp, err := h.Service.Query(c.Request.Context(), req, h.GetTenant(c).TeamID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query logs explorer", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetLogVolume(c *gin.Context) {
	filters, ok := shared.EnrichFilters(c, h.GetTenant(c).TeamID)
	if !ok {
		return
	}
	resp, err := h.LogStats.GetLogVolume(c.Request.Context(), filters, c.Query("step"))
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query log volume", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetLogStats(c *gin.Context) {
	filters, ok := shared.EnrichFilters(c, h.GetTenant(c).TeamID)
	if !ok {
		return
	}
	resp, err := h.LogStats.GetLogStats(c.Request.Context(), filters)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query log stats", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetLogAggregate(c *gin.Context) {
	filters, ok := shared.EnrichFilters(c, h.GetTenant(c).TeamID)
	if !ok {
		return
	}
	var req LogAggregateRequest
	_ = c.ShouldBindQuery(&req)
	if _, err := buildLogAggregateQuery(req); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, err.Error())
		return
	}
	resp, err := h.LogStats.GetLogAggregate(c.Request.Context(), filters, req)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query log aggregate", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}
