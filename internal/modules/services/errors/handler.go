package errors

import (
	"context"
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/infra/cursor"
	"github.com/Optikk-Org/optikk-backend/internal/shared/errorcode"

	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type ErrorHandler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *ErrorHandler) GetServiceErrorRate(c *gin.Context) {
	serviceName := c.Query("serviceName")
	modulecommon.HandleRangeQuery(c, h.GetTenant, "Failed to query service error rate", func(ctx context.Context, teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetServiceErrorRate(ctx, teamID, startMs, endMs, serviceName)
	})
}

func (h *ErrorHandler) GetErrorVolume(c *gin.Context) {
	serviceName := c.Query("serviceName")
	modulecommon.HandleRangeQuery(c, h.GetTenant, "Failed to query error volume", func(ctx context.Context, teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetErrorVolume(ctx, teamID, startMs, endMs, serviceName)
	})
}

func (h *ErrorHandler) GetErrorGroups(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	serviceName := c.Query("serviceName")

	limit := modulecommon.ParseIntParam(c, "limit", 100)
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	cursorStr := c.Query("cursor")
	var cur ErrorGroupsCursor
	if cursorStr != "" {
		if decoded, ok := cursor.Decode[ErrorGroupsCursor](cursorStr); ok {
			cur = decoded
		}
	}

	groups, err := h.Service.GetErrorGroups(c.Request.Context(), teamID, startMs, endMs, serviceName, limit, cur)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query overview errors", err)
		return
	}

	modulecommon.RespondOK(c, groups)
}

func (h *ErrorHandler) GetErrorGroupDetail(c *gin.Context) {
	groupID := c.Param("groupId")
	modulecommon.HandleRangeQuery(c, h.GetTenant, "Failed to query error group detail", func(ctx context.Context, teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetErrorGroupDetail(ctx, teamID, startMs, endMs, groupID)
	})
}

const maxTracesLimit = 20

func (h *ErrorHandler) GetErrorGroupTraces(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	groupID := c.Param("groupId")
	limit := modulecommon.ParseIntParam(c, "limit", maxTracesLimit)
	if limit < 1 || limit > maxTracesLimit {
		limit = maxTracesLimit
	}
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	var cur ErrorTracesCursor
	if cursorStr := c.Query("cursor"); cursorStr != "" {
		if decoded, ok := cursor.Decode[ErrorTracesCursor](cursorStr); ok {
			cur = decoded
		}
	}

	traces, err := h.Service.GetErrorGroupTraces(c.Request.Context(), teamID, startMs, endMs, groupID, limit, cur)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query error group traces", err)
		return
	}
	modulecommon.RespondOK(c, traces)
}

func (h *ErrorHandler) GetErrorGroupTimeseries(c *gin.Context) {
	groupID := c.Param("groupId")
	modulecommon.HandleRangeQuery(c, h.GetTenant, "Failed to query error group timeseries", func(ctx context.Context, teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetErrorGroupTimeseries(ctx, teamID, startMs, endMs, groupID)
	})
}

func (h *ErrorHandler) GetErrorGroupLatestOccurrence(c *gin.Context) {
	groupID := c.Param("groupId")
	modulecommon.HandleRangeQuery(c, h.GetTenant, "Failed to query error group latest occurrence", func(ctx context.Context, teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetErrorGroupLatestOccurrence(ctx, teamID, startMs, endMs, groupID)
	})
}

func (h *ErrorHandler) GetErrorGroupFacets(c *gin.Context) {
	groupID := c.Param("groupId")
	modulecommon.HandleRangeQuery(c, h.GetTenant, "Failed to query error group facets", func(ctx context.Context, teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetErrorGroupFacets(ctx, teamID, startMs, endMs, groupID)
	})
}

// Migrated from errortracking

func (h *ErrorHandler) GetErrorHotspot(c *gin.Context) {
	modulecommon.HandleRangeQuery(c, h.GetTenant, "Failed to query error hotspot", func(ctx context.Context, teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetErrorHotspot(ctx, teamID, startMs, endMs)
	})
}
