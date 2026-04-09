package platform

type Prompt struct {
	ID              string   `json:"id"`
	TeamID          int64    `json:"teamId"`
	Name            string   `json:"name"`
	Slug            string   `json:"slug"`
	Description     string   `json:"description"`
	ModelProvider   string   `json:"modelProvider"`
	ModelName       string   `json:"modelName"`
	SystemPrompt    string   `json:"systemPrompt"`
	UserTemplate    string   `json:"userTemplate"`
	Tags            []string `json:"tags"`
	LatestVersion   int      `json:"latestVersion"`
	ActiveVersionID string   `json:"activeVersionId,omitempty"`
	UpdatedAt       string   `json:"updatedAt"`
	CreatedAt       string   `json:"createdAt"`
}

type PromptVersion struct {
	ID            string   `json:"id"`
	PromptID      string   `json:"promptId"`
	VersionNumber int      `json:"versionNumber"`
	Changelog     string   `json:"changelog"`
	SystemPrompt  string   `json:"systemPrompt"`
	UserTemplate  string   `json:"userTemplate"`
	Variables     []string `json:"variables"`
	IsActive      bool     `json:"isActive"`
	CreatedAt     string   `json:"createdAt"`
}

type Dataset struct {
	ID          string   `json:"id"`
	TeamID      int64    `json:"teamId"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Tags        []string `json:"tags"`
	ItemCount   int      `json:"itemCount"`
	UpdatedAt   string   `json:"updatedAt"`
	CreatedAt   string   `json:"createdAt"`
}

type DatasetItem struct {
	ID             string         `json:"id"`
	DatasetID      string         `json:"datasetId"`
	Input          map[string]any `json:"input"`
	ExpectedOutput string         `json:"expectedOutput"`
	Metadata       map[string]any `json:"metadata"`
	CreatedAt      string         `json:"createdAt"`
}

type Feedback struct {
	ID         string `json:"id"`
	TeamID     int64  `json:"teamId"`
	TargetType string `json:"targetType"`
	TargetID   string `json:"targetId"`
	RunSpanID  string `json:"runSpanId,omitempty"`
	TraceID    string `json:"traceId,omitempty"`
	Score      int    `json:"score"`
	Label      string `json:"label"`
	Comment    string `json:"comment"`
	CreatedBy  string `json:"createdBy"`
	CreatedAt  string `json:"createdAt"`
}

type EvalSuite struct {
	ID          string `json:"id"`
	TeamID      int64  `json:"teamId"`
	Name        string `json:"name"`
	Description string `json:"description"`
	PromptID    string `json:"promptId"`
	DatasetID   string `json:"datasetId"`
	JudgeModel  string `json:"judgeModel"`
	Status      string `json:"status"`
	UpdatedAt   string `json:"updatedAt"`
	CreatedAt   string `json:"createdAt"`
}

type EvalRun struct {
	ID              string         `json:"id"`
	TeamID          int64          `json:"-"`
	EvalID          string         `json:"evalId"`
	PromptVersionID string         `json:"promptVersionId"`
	DatasetID       string         `json:"datasetId"`
	Status          string         `json:"status"`
	AverageScore    float64        `json:"averageScore"`
	PassRate        float64        `json:"passRate"`
	TotalCases      int            `json:"totalCases"`
	CompletedCases  int            `json:"completedCases"`
	Summary         map[string]any `json:"summary"`
	StartedAt       string         `json:"startedAt,omitempty"`
	FinishedAt      string         `json:"finishedAt,omitempty"`
	CreatedAt       string         `json:"createdAt"`
}

type EvalScore struct {
	ID            string  `json:"id"`
	EvalRunID     string  `json:"evalRunId"`
	DatasetItemID string  `json:"datasetItemId"`
	Score         float64 `json:"score"`
	ResultLabel   string  `json:"resultLabel"`
	Reason        string  `json:"reason"`
	OutputText    string  `json:"outputText"`
	CreatedAt     string  `json:"createdAt"`
}

type Experiment struct {
	ID          string `json:"id"`
	TeamID      int64  `json:"teamId"`
	Name        string `json:"name"`
	Description string `json:"description"`
	DatasetID   string `json:"datasetId"`
	Status      string `json:"status"`
	UpdatedAt   string `json:"updatedAt"`
	CreatedAt   string `json:"createdAt"`
}

type ExperimentVariant struct {
	ID              string  `json:"id"`
	ExperimentID    string  `json:"experimentId"`
	PromptVersionID string  `json:"promptVersionId"`
	Label           string  `json:"label"`
	Weight          float64 `json:"weight"`
	CreatedAt       string  `json:"createdAt"`
}

type ExperimentRun struct {
	ID              string         `json:"id"`
	TeamID          int64          `json:"-"`
	ExperimentID    string         `json:"experimentId"`
	Status          string         `json:"status"`
	WinnerVariantID string         `json:"winnerVariantId,omitempty"`
	Summary         map[string]any `json:"summary"`
	StartedAt       string         `json:"startedAt,omitempty"`
	FinishedAt      string         `json:"finishedAt,omitempty"`
	CreatedAt       string         `json:"createdAt"`
}

type CreatePromptInput struct {
	Name          string   `json:"name"`
	Slug          string   `json:"slug"`
	Description   string   `json:"description"`
	ModelProvider string   `json:"modelProvider"`
	ModelName     string   `json:"modelName"`
	SystemPrompt  string   `json:"systemPrompt"`
	UserTemplate  string   `json:"userTemplate"`
	Tags          []string `json:"tags"`
}

type CreatePromptVersionInput struct {
	Changelog    string   `json:"changelog"`
	SystemPrompt string   `json:"systemPrompt"`
	UserTemplate string   `json:"userTemplate"`
	Variables    []string `json:"variables"`
	Activate     bool     `json:"activate"`
}

type CreateDatasetInput struct {
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Tags        []string `json:"tags"`
}

type CreateDatasetItemInput struct {
	Input          map[string]any `json:"input"`
	ExpectedOutput string         `json:"expectedOutput"`
	Metadata       map[string]any `json:"metadata"`
}

type CreateFeedbackInput struct {
	TargetType string `json:"targetType"`
	TargetID   string `json:"targetId"`
	RunSpanID  string `json:"runSpanId"`
	TraceID    string `json:"traceId"`
	Score      int    `json:"score"`
	Label      string `json:"label"`
	Comment    string `json:"comment"`
}

type CreateEvalInput struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	PromptID    string `json:"promptId"`
	DatasetID   string `json:"datasetId"`
	JudgeModel  string `json:"judgeModel"`
	Status      string `json:"status"`
}

type LaunchEvalInput struct {
	PromptVersionID string `json:"promptVersionId"`
}

type CreateExperimentVariantInput struct {
	PromptVersionID string  `json:"promptVersionId"`
	Label           string  `json:"label"`
	Weight          float64 `json:"weight"`
}

type CreateExperimentInput struct {
	Name        string                         `json:"name"`
	Description string                         `json:"description"`
	DatasetID   string                         `json:"datasetId"`
	Variants    []CreateExperimentVariantInput `json:"variants"`
}
