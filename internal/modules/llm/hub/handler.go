package hub

import (
	"database/sql"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts"
	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts/errorcode"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

// Handler exposes LLM hub HTTP routes.
type Handler struct {
	modulecommon.DBTenant
	Service *Service
}

// NewHandler constructs the hub handler.
func NewHandler(db modulecommon.DBTenant, svc *Service) *Handler {
	return &Handler{DBTenant: db, Service: svc}
}

func (h *Handler) PostScore(c *gin.Context) {
	var req CreateScoreRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Invalid request body")
		return
	}
	id, err := h.Service.CreateScore(c.Request.Context(), h.GetTenant(c).TeamID, req)
	if err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, err.Error())
		return
	}
	modulecommon.RespondOK(c, gin.H{"id": id})
}

func (h *Handler) PostScoresBatch(c *gin.Context) {
	var req BatchCreateScoresRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Invalid request body")
		return
	}
	ids, err := h.Service.BatchCreateScores(c.Request.Context(), h.GetTenant(c).TeamID, req.Scores)
	if err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, err.Error())
		return
	}
	modulecommon.RespondOK(c, gin.H{"ids": ids})
}

func (h *Handler) ListScores(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, err1 := parseInt64Query(c, "startTime", 0)
	endMs, err2 := parseInt64Query(c, "endTime", 0)
	if err1 != nil || err2 != nil || startMs <= 0 || endMs <= 0 || startMs >= endMs {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Valid startTime and endTime (ms) are required")
		return
	}
	limit, _ := parseInt64Query(c, "limit", 50)
	offset, _ := parseInt64Query(c, "offset", 0)
	name := strings.TrimSpace(c.Query("name"))
	traceID := strings.TrimSpace(c.Query("traceId"))
	resp, err := h.Service.ListScores(c.Request.Context(), teamID, startMs, endMs, name, traceID, int(limit), int(offset))
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to list scores", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func parseInt64Query(c *gin.Context, key string, def int64) (int64, error) {
	s := c.Query(key)
	if s == "" {
		return def, nil
	}
	return strconv.ParseInt(s, 10, 64)
}

func (h *Handler) ListPrompts(c *gin.Context) {
	resp, err := h.Service.ListPrompts(c.Request.Context(), h.GetTenant(c).TeamID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to list prompts", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) CreatePrompt(c *gin.Context) {
	var req CreatePromptRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Invalid request body")
		return
	}
	p, err := h.Service.CreatePrompt(c.Request.Context(), h.GetTenant(c).TeamID, req)
	if err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, err.Error())
		return
	}
	c.JSON(http.StatusCreated, contracts.Success(p))
}

func (h *Handler) UpdatePrompt(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Invalid prompt id")
		return
	}
	var req UpdatePromptRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Invalid request body")
		return
	}
	if err := h.Service.UpdatePrompt(c.Request.Context(), h.GetTenant(c).TeamID, id, req); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, err.Error())
		return
	}
	modulecommon.RespondOK(c, gin.H{"ok": true})
}

func (h *Handler) DeletePrompt(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Invalid prompt id")
		return
	}
	if err := h.Service.DeletePrompt(c.Request.Context(), h.GetTenant(c).TeamID, id); err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to delete prompt", err)
		return
	}
	c.Status(http.StatusNoContent)
}

func (h *Handler) ListDatasets(c *gin.Context) {
	limit, _ := parseInt64Query(c, "limit", 50)
	resp, err := h.Service.ListDatasets(c.Request.Context(), h.GetTenant(c).TeamID, int(limit))
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to list datasets", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) CreateDataset(c *gin.Context) {
	var req CreateDatasetRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Invalid request body")
		return
	}
	d, err := h.Service.CreateDataset(c.Request.Context(), h.GetTenant(c).TeamID, req)
	if err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, err.Error())
		return
	}
	c.JSON(http.StatusCreated, contracts.Success(d))
}

func (h *Handler) GetDataset(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Invalid dataset id")
		return
	}
	d, err := h.Service.GetDataset(c.Request.Context(), h.GetTenant(c).TeamID, id)
	if err != nil {
		modulecommon.RespondError(c, http.StatusNotFound, errorcode.NotFound, "Dataset not found")
		return
	}
	modulecommon.RespondOK(c, d)
}

func (h *Handler) GetSettings(c *gin.Context) {
	s, err := h.Service.GetTeamSettings(c.Request.Context(), h.GetTenant(c).TeamID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to load settings", err)
		return
	}
	modulecommon.RespondOK(c, s)
}

func (h *Handler) PatchSettings(c *gin.Context) {
	var req PatchTeamSettingsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Invalid request body")
		return
	}
	if err := h.Service.PatchTeamSettings(c.Request.Context(), h.GetTenant(c).TeamID, req); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			modulecommon.RespondError(c, http.StatusNotFound, errorcode.NotFound, "Team not found")
			return
		}
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, err.Error())
		return
	}
	modulecommon.RespondOK(c, gin.H{"ok": true})
}
