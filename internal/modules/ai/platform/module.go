package platform

import (
	"database/sql"
	"errors"
	"net/http"
	"strings"

	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts/errorcode"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type Handler struct {
	modulecommon.DBTenant
	Repo *Repository
}

type Module struct {
	handler *Handler
	runner  *ExecutionLoop
}

func NewModule(db *sql.DB, getTenant registry.GetTenantFunc) registry.Module {
	repo := NewRepository(db)
	return &Module{
		handler: &Handler{
			DBTenant: modulecommon.DBTenant{DB: db, GetTenant: getTenant},
			Repo:     repo,
		},
		runner: NewExecutionLoop(repo),
	}
}

func (m *Module) Name() string                      { return "aiPlatform" }
func (m *Module) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *Module) RegisterRoutes(group *gin.RouterGroup) {
	group.GET("/ai/prompts", m.handler.ListPrompts)
	group.GET("/ai/prompts/:promptId", m.handler.GetPrompt)
	group.POST("/ai/prompts", m.handler.CreatePrompt)
	group.GET("/ai/prompts/:promptId/versions", m.handler.ListPromptVersions)
	group.POST("/ai/prompts/:promptId/versions", m.handler.CreatePromptVersion)

	group.GET("/ai/datasets", m.handler.ListDatasets)
	group.GET("/ai/datasets/:datasetId", m.handler.GetDataset)
	group.POST("/ai/datasets", m.handler.CreateDataset)
	group.GET("/ai/datasets/:datasetId/items", m.handler.ListDatasetItems)
	group.POST("/ai/datasets/:datasetId/items", m.handler.CreateDatasetItem)

	group.GET("/ai/feedback", m.handler.ListFeedback)
	group.POST("/ai/feedback", m.handler.CreateFeedback)

	group.GET("/ai/evals", m.handler.ListEvals)
	group.GET("/ai/evals/:evalId", m.handler.GetEval)
	group.POST("/ai/evals", m.handler.CreateEval)
	group.GET("/ai/evals/:evalId/runs", m.handler.ListEvalRuns)
	group.POST("/ai/evals/:evalId/runs", m.handler.LaunchEvalRun)
	group.GET("/ai/evals/:evalId/runs/:runId/scores", m.handler.ListEvalScores)

	group.GET("/ai/experiments", m.handler.ListExperiments)
	group.GET("/ai/experiments/:experimentId", m.handler.GetExperiment)
	group.POST("/ai/experiments", m.handler.CreateExperiment)
	group.GET("/ai/experiments/:experimentId/variants", m.handler.ListExperimentVariants)
	group.GET("/ai/experiments/:experimentId/runs", m.handler.ListExperimentRuns)
	group.POST("/ai/experiments/:experimentId/runs", m.handler.LaunchExperimentRun)
}

func (m *Module) Start() {
	m.runner.Start()
}

func (m *Module) Stop() error {
	return m.runner.Stop()
}

func (h *Handler) ListPrompts(c *gin.Context) {
	prompts, err := h.Repo.ListPrompts(c.Request.Context(), h.GetTenant(c).TeamID)
	if err != nil {
		respondInternal(c, "Failed to list prompts", err)
		return
	}
	modulecommon.RespondOK(c, prompts)
}

func (h *Handler) GetPrompt(c *gin.Context) {
	prompt, err := h.Repo.GetPrompt(c.Request.Context(), h.GetTenant(c).TeamID, c.Param("promptId"))
	if err != nil {
		respondLookupError(c, "Prompt not found", "Failed to get prompt", err)
		return
	}
	modulecommon.RespondOK(c, prompt)
}

func (h *Handler) CreatePrompt(c *gin.Context) {
	var input CreatePromptInput
	if err := c.ShouldBindJSON(&input); err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.BadRequest, "Invalid prompt payload", err)
		return
	}
	if err := requireNonEmpty(map[string]string{
		"name":          input.Name,
		"slug":          input.Slug,
		"modelProvider": input.ModelProvider,
		"modelName":     input.ModelName,
		"systemPrompt":  input.SystemPrompt,
		"userTemplate":  input.UserTemplate,
	}); err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.Validation, "Prompt is missing required fields", err)
		return
	}

	prompt, err := h.Repo.CreatePrompt(c.Request.Context(), h.GetTenant(c).TeamID, input)
	if err != nil {
		respondInternal(c, "Failed to create prompt", err)
		return
	}
	modulecommon.RespondOK(c, prompt)
}

func (h *Handler) ListPromptVersions(c *gin.Context) {
	versions, err := h.Repo.ListPromptVersions(c.Request.Context(), h.GetTenant(c).TeamID, c.Param("promptId"))
	if err != nil {
		respondInternal(c, "Failed to list prompt versions", err)
		return
	}
	modulecommon.RespondOK(c, versions)
}

func (h *Handler) CreatePromptVersion(c *gin.Context) {
	var input CreatePromptVersionInput
	if err := c.ShouldBindJSON(&input); err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.BadRequest, "Invalid prompt version payload", err)
		return
	}
	if err := requireNonEmpty(map[string]string{
		"systemPrompt": input.SystemPrompt,
		"userTemplate": input.UserTemplate,
	}); err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.Validation, "Prompt version is missing required fields", err)
		return
	}

	version, err := h.Repo.CreatePromptVersion(c.Request.Context(), h.GetTenant(c).TeamID, c.Param("promptId"), input)
	if err != nil {
		respondLookupError(c, "Prompt not found", "Failed to create prompt version", err)
		return
	}
	modulecommon.RespondOK(c, version)
}

func (h *Handler) ListDatasets(c *gin.Context) {
	datasets, err := h.Repo.ListDatasets(c.Request.Context(), h.GetTenant(c).TeamID)
	if err != nil {
		respondInternal(c, "Failed to list datasets", err)
		return
	}
	modulecommon.RespondOK(c, datasets)
}

func (h *Handler) GetDataset(c *gin.Context) {
	dataset, err := h.Repo.GetDataset(c.Request.Context(), h.GetTenant(c).TeamID, c.Param("datasetId"))
	if err != nil {
		respondLookupError(c, "Dataset not found", "Failed to get dataset", err)
		return
	}
	modulecommon.RespondOK(c, dataset)
}

func (h *Handler) CreateDataset(c *gin.Context) {
	var input CreateDatasetInput
	if err := c.ShouldBindJSON(&input); err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.BadRequest, "Invalid dataset payload", err)
		return
	}
	if err := requireNonEmpty(map[string]string{"name": input.Name}); err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.Validation, "Dataset name is required", err)
		return
	}

	dataset, err := h.Repo.CreateDataset(c.Request.Context(), h.GetTenant(c).TeamID, input)
	if err != nil {
		respondInternal(c, "Failed to create dataset", err)
		return
	}
	modulecommon.RespondOK(c, dataset)
}

func (h *Handler) ListDatasetItems(c *gin.Context) {
	items, err := h.Repo.ListDatasetItems(c.Request.Context(), h.GetTenant(c).TeamID, c.Param("datasetId"))
	if err != nil {
		respondInternal(c, "Failed to list dataset items", err)
		return
	}
	modulecommon.RespondOK(c, items)
}

func (h *Handler) CreateDatasetItem(c *gin.Context) {
	var input CreateDatasetItemInput
	if err := c.ShouldBindJSON(&input); err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.BadRequest, "Invalid dataset item payload", err)
		return
	}
	item, err := h.Repo.CreateDatasetItem(c.Request.Context(), h.GetTenant(c).TeamID, c.Param("datasetId"), input)
	if err != nil {
		respondLookupError(c, "Dataset not found", "Failed to create dataset item", err)
		return
	}
	modulecommon.RespondOK(c, item)
}

func (h *Handler) ListFeedback(c *gin.Context) {
	feedback, err := h.Repo.ListFeedback(
		c.Request.Context(),
		h.GetTenant(c).TeamID,
		strings.TrimSpace(c.Query("targetType")),
		strings.TrimSpace(c.Query("targetId")),
	)
	if err != nil {
		respondInternal(c, "Failed to list feedback", err)
		return
	}
	modulecommon.RespondOK(c, feedback)
}

func (h *Handler) CreateFeedback(c *gin.Context) {
	var input CreateFeedbackInput
	if err := c.ShouldBindJSON(&input); err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.BadRequest, "Invalid feedback payload", err)
		return
	}
	if err := ensureValidFeedbackTarget(input.TargetType); err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.Validation, "Feedback target is invalid", err)
		return
	}
	if err := requireNonEmpty(map[string]string{
		"targetType": input.TargetType,
		"targetId":   input.TargetID,
		"label":      input.Label,
	}); err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.Validation, "Feedback is missing required fields", err)
		return
	}
	if input.Score < 0 || input.Score > 100 {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "score must be between 0 and 100")
		return
	}

	tenant := h.GetTenant(c)
	createdBy := strings.TrimSpace(tenant.UserEmail)
	if createdBy == "" && tenant.UserID > 0 {
		createdBy = "user"
	}

	feedback, err := h.Repo.CreateFeedback(c.Request.Context(), tenant.TeamID, createdBy, input)
	if err != nil {
		respondInternal(c, "Failed to create feedback", err)
		return
	}
	modulecommon.RespondOK(c, feedback)
}

func (h *Handler) ListEvals(c *gin.Context) {
	evals, err := h.Repo.ListEvals(c.Request.Context(), h.GetTenant(c).TeamID)
	if err != nil {
		respondInternal(c, "Failed to list evaluations", err)
		return
	}
	modulecommon.RespondOK(c, evals)
}

func (h *Handler) GetEval(c *gin.Context) {
	eval, err := h.Repo.GetEval(c.Request.Context(), h.GetTenant(c).TeamID, c.Param("evalId"))
	if err != nil {
		respondLookupError(c, "Evaluation not found", "Failed to get evaluation", err)
		return
	}
	modulecommon.RespondOK(c, eval)
}

func (h *Handler) CreateEval(c *gin.Context) {
	var input CreateEvalInput
	if err := c.ShouldBindJSON(&input); err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.BadRequest, "Invalid evaluation payload", err)
		return
	}
	if err := requireNonEmpty(map[string]string{
		"name":       input.Name,
		"promptId":   input.PromptID,
		"datasetId":  input.DatasetID,
		"judgeModel": input.JudgeModel,
	}); err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.Validation, "Evaluation is missing required fields", err)
		return
	}
	if strings.TrimSpace(input.Status) != "" && !allowedEvalStatus(input.Status) {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "status must be draft, active, or paused")
		return
	}

	eval, err := h.Repo.CreateEval(c.Request.Context(), h.GetTenant(c).TeamID, input)
	if err != nil {
		respondInternal(c, "Failed to create evaluation", err)
		return
	}
	modulecommon.RespondOK(c, eval)
}

func (h *Handler) ListEvalRuns(c *gin.Context) {
	runs, err := h.Repo.ListEvalRuns(c.Request.Context(), h.GetTenant(c).TeamID, c.Param("evalId"))
	if err != nil {
		respondInternal(c, "Failed to list evaluation runs", err)
		return
	}
	modulecommon.RespondOK(c, runs)
}

func (h *Handler) LaunchEvalRun(c *gin.Context) {
	var input LaunchEvalInput
	if err := c.ShouldBindJSON(&input); err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.BadRequest, "Invalid evaluation launch payload", err)
		return
	}
	if err := requireNonEmpty(map[string]string{"promptVersionId": input.PromptVersionID}); err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.Validation, "promptVersionId is required", err)
		return
	}

	run, err := h.Repo.QueueEvalRun(c.Request.Context(), h.GetTenant(c).TeamID, c.Param("evalId"), input.PromptVersionID)
	if err != nil {
		respondLookupError(c, "Evaluation not found", "Failed to queue evaluation run", err)
		return
	}
	modulecommon.RespondOK(c, run)
}

func (h *Handler) ListEvalScores(c *gin.Context) {
	scores, err := h.Repo.ListEvalScores(c.Request.Context(), h.GetTenant(c).TeamID, c.Param("evalId"), c.Param("runId"))
	if err != nil {
		respondInternal(c, "Failed to list evaluation scores", err)
		return
	}
	modulecommon.RespondOK(c, scores)
}

func (h *Handler) ListExperiments(c *gin.Context) {
	experiments, err := h.Repo.ListExperiments(c.Request.Context(), h.GetTenant(c).TeamID)
	if err != nil {
		respondInternal(c, "Failed to list experiments", err)
		return
	}
	modulecommon.RespondOK(c, experiments)
}

func (h *Handler) GetExperiment(c *gin.Context) {
	experiment, err := h.Repo.GetExperiment(c.Request.Context(), h.GetTenant(c).TeamID, c.Param("experimentId"))
	if err != nil {
		respondLookupError(c, "Experiment not found", "Failed to get experiment", err)
		return
	}
	modulecommon.RespondOK(c, experiment)
}

func (h *Handler) CreateExperiment(c *gin.Context) {
	var input CreateExperimentInput
	if err := c.ShouldBindJSON(&input); err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.BadRequest, "Invalid experiment payload", err)
		return
	}
	if err := requireNonEmpty(map[string]string{
		"name":      input.Name,
		"datasetId": input.DatasetID,
	}); err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.Validation, "Experiment is missing required fields", err)
		return
	}
	if len(input.Variants) == 0 {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "At least one variant is required")
		return
	}
	for _, variant := range input.Variants {
		if err := requireNonEmpty(map[string]string{
			"promptVersionId": variant.PromptVersionID,
			"label":           variant.Label,
		}); err != nil {
			modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.Validation, "Each experiment variant requires a promptVersionId and label", err)
			return
		}
	}

	experiment, err := h.Repo.CreateExperiment(c.Request.Context(), h.GetTenant(c).TeamID, input)
	if err != nil {
		respondInternal(c, "Failed to create experiment", err)
		return
	}
	modulecommon.RespondOK(c, experiment)
}

func (h *Handler) ListExperimentVariants(c *gin.Context) {
	variants, err := h.Repo.ListExperimentVariants(c.Request.Context(), h.GetTenant(c).TeamID, c.Param("experimentId"))
	if err != nil {
		respondInternal(c, "Failed to list experiment variants", err)
		return
	}
	modulecommon.RespondOK(c, variants)
}

func (h *Handler) ListExperimentRuns(c *gin.Context) {
	runs, err := h.Repo.ListExperimentRuns(c.Request.Context(), h.GetTenant(c).TeamID, c.Param("experimentId"))
	if err != nil {
		respondInternal(c, "Failed to list experiment runs", err)
		return
	}
	modulecommon.RespondOK(c, runs)
}

func (h *Handler) LaunchExperimentRun(c *gin.Context) {
	run, err := h.Repo.QueueExperimentRun(c.Request.Context(), h.GetTenant(c).TeamID, c.Param("experimentId"))
	if err != nil {
		respondLookupError(c, "Experiment not found", "Failed to queue experiment run", err)
		return
	}
	modulecommon.RespondOK(c, run)
}

func respondLookupError(c *gin.Context, notFoundMessage, internalMessage string, err error) {
	if errors.Is(err, sql.ErrNoRows) {
		modulecommon.RespondError(c, http.StatusNotFound, errorcode.NotFound, notFoundMessage)
		return
	}
	respondInternal(c, internalMessage, err)
}

func respondInternal(c *gin.Context, message string, err error) {
	modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, message, err)
}

var (
	_ registry.Module           = (*Module)(nil)
	_ registry.BackgroundRunner = (*Module)(nil)
)
