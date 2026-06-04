package errors

import (
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
	teamID := h.GetTenant(c).TeamID
	serviceName := c.Query("serviceName")

	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	points, err := h.Service.GetServiceErrorRate(c.Request.Context(), teamID, startMs, endMs, serviceName)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query service error rate", err)
		return
	}

	modulecommon.RespondOK(c, points)
}

func (h *ErrorHandler) GetErrorVolume(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	serviceName := c.Query("serviceName")

	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	points, err := h.Service.GetErrorVolume(c.Request.Context(), teamID, startMs, endMs, serviceName)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query error volume", err)
		return
	}

	modulecommon.RespondOK(c, points)
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
	teamID := h.GetTenant(c).TeamID
	groupID := c.Param("groupId")
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	detail, err := h.Service.GetErrorGroupDetail(c.Request.Context(), teamID, startMs, endMs, groupID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query error group detail", err)
		return
	}
	modulecommon.RespondOK(c, detail)
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
	teamID := h.GetTenant(c).TeamID
	groupID := c.Param("groupId")
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	points, err := h.Service.GetErrorGroupTimeseries(c.Request.Context(), teamID, startMs, endMs, groupID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query error group timeseries", err)
		return
	}
	modulecommon.RespondOK(c, points)
}

<<<<<<< HEAD
=======
func (h *ErrorHandler) GetErrorGroupLatestOccurrence(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	groupID := c.Param("groupId")
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	occ, err := h.Service.GetErrorGroupLatestOccurrence(c.Request.Context(), teamID, startMs, endMs, groupID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query error group latest occurrence", err)
		return
	}
	modulecommon.RespondOK(c, occ)
}

func (h *ErrorHandler) GetErrorGroupFacets(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	groupID := c.Param("groupId")
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	facets, err := h.Service.GetErrorGroupFacets(c.Request.Context(), teamID, startMs, endMs, groupID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query error group facets", err)
		return
	}
	modulecommon.RespondOK(c, facets)
}

>>>>>>> f512576e76eb5e661aabd2a3202a40891770b326
// Migrated from errortracking

func (h *ErrorHandler) GetErrorHotspot(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	cells, err := h.Service.GetErrorHotspot(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query error hotspot", err)
		return
	}
	modulecommon.RespondOK(c, cells)
}
<<<<<<< HEAD

=======
>>>>>>> f512576e76eb5e661aabd2a3202a40891770b326
