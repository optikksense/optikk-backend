package explore

import (
	"database/sql"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
	. "github.com/observability/observability-backend-go/internal/platform/handlers"
)

var allowedQueryTypes = map[string]struct{}{
	"logs":    {},
	"metrics": {},
	"traces":  {},
}

type ExploreHandler struct {
	modulecommon.DBTenant
	Repo *Repository
}

func (h *ExploreHandler) ListSavedQueries(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	queryType, ok := normalizeQueryType(c.Query("queryType"), true)
	if !ok {
		RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "queryType must be one of: logs, metrics, traces")
		return
	}

	items, err := h.Repo.ListSavedQueries(teamID, queryType)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to load saved queries")
		return
	}

	RespondOK(c, map[string]any{"savedQueries": items})
}

func (h *ExploreHandler) CreateSavedQuery(c *gin.Context) {
	tenant := h.GetTenant(c)
	var req struct {
		QueryType   string `json:"queryType"`
		Name        string `json:"name"`
		Description string `json:"description"`
		Query       any    `json:"query"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid JSON payload")
		return
	}

	name := strings.TrimSpace(req.Name)
	if name == "" {
		RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "name is required")
		return
	}

	queryType, ok := normalizeQueryType(req.QueryType, false)
	if !ok {
		RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "queryType must be one of: logs, metrics, traces")
		return
	}

	item, err := h.Repo.CreateSavedQuery(SavedQueryInput{
		OrganizationID:  tenant.OrganizationID,
		TeamID:          tenant.TeamID,
		QueryType:       queryType,
		Name:            name,
		Description:     strings.TrimSpace(req.Description),
		Query:           req.Query,
		CreatedByUserID: tenant.UserID,
		CreatedByEmail:  tenant.UserEmail,
	})
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to save query")
		return
	}

	RespondOK(c, item)
}

func (h *ExploreHandler) UpdateSavedQuery(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	id, err := ExtractIDParam(c, "id")
	if err != nil {
		RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid id")
		return
	}

	var req struct {
		QueryType   string `json:"queryType"`
		Name        string `json:"name"`
		Description string `json:"description"`
		Query       any    `json:"query"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid JSON payload")
		return
	}

	name := strings.TrimSpace(req.Name)
	if name == "" {
		RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "name is required")
		return
	}

	queryType, ok := normalizeQueryType(req.QueryType, false)
	if !ok {
		RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "queryType must be one of: logs, metrics, traces")
		return
	}

	item, err := h.Repo.UpdateSavedQuery(teamID, id, SavedQueryInput{
		QueryType:   queryType,
		Name:        name,
		Description: strings.TrimSpace(req.Description),
		Query:       req.Query,
	})
	if err == sql.ErrNoRows {
		RespondError(c, http.StatusNotFound, "RESOURCE_NOT_FOUND", "Saved query not found")
		return
	}
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update query")
		return
	}

	RespondOK(c, item)
}

func (h *ExploreHandler) DeleteSavedQuery(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	id, err := ExtractIDParam(c, "id")
	if err != nil {
		RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid id")
		return
	}

	deleted, err := h.Repo.DeleteSavedQuery(teamID, id)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete query")
		return
	}
	if !deleted {
		RespondError(c, http.StatusNotFound, "RESOURCE_NOT_FOUND", "Saved query not found")
		return
	}

	RespondOK(c, map[string]any{"id": id, "deleted": true})
}

func normalizeQueryType(raw string, allowEmpty bool) (string, bool) {
	val := strings.ToLower(strings.TrimSpace(raw))
	if val == "" && allowEmpty {
		return "", true
	}
	_, ok := allowedQueryTypes[val]
	return val, ok
}
