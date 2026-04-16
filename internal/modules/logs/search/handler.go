package search

import (
	"net/http"
	"strings"

	errorcode "github.com/Optikk-Org/optikk-backend/internal/shared/contracts"

	shared "github.com/Optikk-Org/optikk-backend/internal/modules/logs/internal/shared"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type Handler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *Handler) GetLogs(c *gin.Context) {
	filters, ok := shared.EnrichFilters(c, h.GetTenant(c).TeamID)
	if !ok {
		return
	}

	limit := modulecommon.ParseIntParam(c, "limit", 100)
	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	direction := strings.ToLower(c.DefaultQuery("direction", "desc"))
	if direction != "asc" {
		direction = "desc"
	}

	var cursor shared.LogCursor
	if raw := strings.TrimSpace(c.Query("cursor")); raw != "" {
		parsed, ok := shared.ParseLogCursor(raw)
		if !ok {
			modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Invalid cursor")
			return
		}
		cursor = parsed
	}

	resp, err := h.Service.GetLogs(c.Request.Context(), filters, limit, direction, cursor)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query logs", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}
