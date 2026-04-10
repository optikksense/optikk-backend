package explorer

import (
	"context"
	"fmt"
	"net/http"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/explorer/analytics"
	"github.com/Optikk-Org/optikk-backend/internal/modules/explorer/queryparser"
	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts/errorcode"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

// Analytics handles POST /v1/explorer/logs/analytics
func (h *Handler) Analytics(c *gin.Context) {
	var req analytics.AnalyticsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Invalid request body")
		return
	}
	if req.StartTime <= 0 || req.EndTime <= 0 || req.StartTime >= req.EndTime {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Valid startTime and endTime are required")
		return
	}

	result, err := runLogsAnalytics(c.Request.Context(), h.db, h.GetTenant(c).TeamID, req)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Analytics query failed", err)
		return
	}
	modulecommon.RespondOK(c, result)
}

func runLogsAnalytics(ctx context.Context, db *dbutil.NativeQuerier, teamID int64, req analytics.AnalyticsRequest) (*analytics.AnalyticsResult, error) {
	cfg := LogsScopeConfig()

	origBase := cfg.BaseWhereFunc
	cfg.BaseWhereFunc = func(_ int64, startMs, endMs int64) (string, []any) {
		return origBase(teamID, startMs, endMs)
	}

	var queryWhere string
	var queryArgs []any
	if req.Query != "" {
		node, parseErr := queryparser.Parse(req.Query)
		if parseErr != nil {
			return nil, fmt.Errorf("invalid query: %w", parseErr)
		}
		if node != nil {
			compiled, compileErr := queryparser.Compile(node, queryparser.LogsSchema{})
			if compileErr != nil {
				return nil, fmt.Errorf("query compilation error: %w", compileErr)
			}
			queryWhere = compiled.Where
			queryArgs = compiled.Args
		}
	}

	sql, args, err := analytics.BuildQuery(req, cfg, queryWhere, queryArgs)
	if err != nil {
		return nil, err
	}

	var rows []analytics.AnalyticsRowDTO
	if err := db.Select(ctx, &rows, sql, args...); err != nil {
		return nil, fmt.Errorf("analytics query failed: %w", err)
	}

	return analytics.BuildResult(req, rows), nil
}
