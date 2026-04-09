package platform

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
)

type experimentVariantResult struct {
	VariantID       string  `json:"variantId"`
	Label           string  `json:"label"`
	PromptVersionID string  `json:"promptVersionId"`
	AverageScore    float64 `json:"averageScore"`
	PassRate        float64 `json:"passRate"`
	Weight          float64 `json:"weight"`
}

type ExecutionLoop struct {
	repo *Repository
	stop chan struct{}
	wg   sync.WaitGroup
	once sync.Once
}

func NewExecutionLoop(repo *Repository) *ExecutionLoop {
	return &ExecutionLoop{
		repo: repo,
		stop: make(chan struct{}),
	}
}

func (l *ExecutionLoop) Start() {
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-l.stop:
				return
			case <-ticker.C:
				l.processOnce()
			}
		}
	}()
}

func (l *ExecutionLoop) Stop() error {
	l.once.Do(func() { close(l.stop) })
	l.wg.Wait()
	return nil
}

func (l *ExecutionLoop) processOnce() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	l.processQueuedEvalRuns(ctx)
	l.processQueuedExperimentRuns(ctx)
}

func (l *ExecutionLoop) processQueuedEvalRuns(ctx context.Context) {
	runs, err := l.repo.ListQueuedEvalRuns(ctx, 3)
	if err != nil {
		slog.Warn("ai eval runner: failed to load queued runs", slog.Any("error", err))
		return
	}

	for _, run := range runs {
		ok, err := l.repo.MarkEvalRunRunning(ctx, run.ID)
		if err != nil {
			slog.Warn("ai eval runner: failed to mark run running", slog.String("runId", run.ID), slog.Any("error", err))
			continue
		}
		if !ok {
			continue
		}
		if err := l.executeEvalRun(ctx, run.ID); err != nil {
			_ = l.repo.FailEvalRun(context.Background(), run.ID, err.Error())
			slog.Warn("ai eval runner: run failed", slog.String("runId", run.ID), slog.Any("error", err))
		}
	}
}

func (l *ExecutionLoop) processQueuedExperimentRuns(ctx context.Context) {
	runs, err := l.repo.ListQueuedExperimentRuns(ctx, 2)
	if err != nil {
		slog.Warn("ai experiment runner: failed to load queued runs", slog.Any("error", err))
		return
	}

	for _, run := range runs {
		ok, err := l.repo.MarkExperimentRunRunning(ctx, run.ID)
		if err != nil {
			slog.Warn("ai experiment runner: failed to mark run running", slog.String("runId", run.ID), slog.Any("error", err))
			continue
		}
		if !ok {
			continue
		}
		if err := l.executeExperimentRun(ctx, run.ID); err != nil {
			_ = l.repo.FailExperimentRun(context.Background(), run.ID, err.Error())
			slog.Warn("ai experiment runner: run failed", slog.String("runId", run.ID), slog.Any("error", err))
		}
	}
}

func (l *ExecutionLoop) executeEvalRun(ctx context.Context, runID string) error {
	run, err := l.repo.GetEvalRun(ctx, runID)
	if err != nil {
		return err
	}
	suite, err := l.repo.GetEval(ctx, run.TeamID, run.EvalID)
	if err != nil {
		return err
	}
	version, err := l.repo.GetPromptVersion(ctx, run.PromptVersionID)
	if err != nil {
		return err
	}
	items, err := l.repo.ListDatasetItems(ctx, run.TeamID, run.DatasetID)
	if err != nil {
		return err
	}

	if len(items) == 0 {
		return l.repo.CompleteEvalRun(ctx, runID, 0, 0, 0, map[string]any{
			"judgeModel": suite.JudgeModel,
			"note":       "No dataset items were available for evaluation.",
		})
	}

	totalScore := 0.0
	passedCount := 0

	for _, item := range items {
		outputText := simulatePromptOutput(version, item)
		score, label, reason := scoreEvaluation(outputText, item.ExpectedOutput)
		totalScore += score
		if label == "pass" {
			passedCount++
		}
		if err := l.repo.AddEvalScore(ctx, runID, item.ID, score, label, reason, outputText); err != nil {
			return err
		}
	}

	averageScore := totalScore / float64(len(items))
	passRate := float64(passedCount) / float64(len(items))

	return l.repo.CompleteEvalRun(ctx, runID, averageScore, passRate, len(items), map[string]any{
		"judgeModel":        suite.JudgeModel,
		"promptVersionId":   version.ID,
		"evaluatedCases":    len(items),
		"heuristic":         "expected-output-overlap",
		"averageScore":      averageScore,
		"passRate":          passRate,
		"promptPreview":     truncate(version.UserTemplate, 160),
		"systemPromptBlurb": truncate(version.SystemPrompt, 160),
	})
}

func (l *ExecutionLoop) executeExperimentRun(ctx context.Context, runID string) error {
	run, err := l.repo.GetExperimentRun(ctx, runID)
	if err != nil {
		return err
	}
	experiment, err := l.repo.GetExperiment(ctx, run.TeamID, run.ExperimentID)
	if err != nil {
		return err
	}
	variants, err := l.repo.ListExperimentVariants(ctx, run.TeamID, run.ExperimentID)
	if err != nil {
		return err
	}
	items, err := l.repo.ListDatasetItems(ctx, run.TeamID, experiment.DatasetID)
	if err != nil {
		return err
	}

	if len(variants) == 0 {
		return fmt.Errorf("experiment %s has no variants", run.ExperimentID)
	}

	summaries := make([]experimentVariantResult, 0, len(variants))
	winner := experimentVariantResult{}

	for _, variant := range variants {
		version, err := l.repo.GetPromptVersion(ctx, variant.PromptVersionID)
		if err != nil {
			return err
		}

		totalScore := 0.0
		passedCount := 0
		for _, item := range items {
			outputText := simulatePromptOutput(version, item)
			score, label, _ := scoreEvaluation(outputText, item.ExpectedOutput)
			totalScore += score
			if label == "pass" {
				passedCount++
			}
		}

		averageScore := 0.0
		passRate := 0.0
		if len(items) > 0 {
			averageScore = totalScore / float64(len(items))
			passRate = float64(passedCount) / float64(len(items))
		}

		summary := experimentVariantResult{
			VariantID:       variant.ID,
			Label:           variant.Label,
			PromptVersionID: variant.PromptVersionID,
			AverageScore:    averageScore,
			PassRate:        passRate,
			Weight:          variant.Weight,
		}
		summaries = append(summaries, summary)
		if len(winner.VariantID) == 0 || scoreVariant(summary, winner) > 0 {
			winner = summary
		}
	}

	sort.SliceStable(summaries, func(i, j int) bool {
		return scoreVariant(summaries[i], summaries[j]) > 0
	})

	return l.repo.CompleteExperimentRun(ctx, runID, winner.VariantID, map[string]any{
		"datasetId":       experiment.DatasetID,
		"totalCases":      len(items),
		"winnerVariantId": winner.VariantID,
		"winnerLabel":     winner.Label,
		"variants":        summaries,
		"heuristic":       "expected-output-overlap",
	})
}

func simulatePromptOutput(version PromptVersion, item DatasetItem) string {
	rendered := renderTemplate(version.UserTemplate, item.Input)

	for _, key := range []string{"answer", "output", "expectedOutput", "expected_output"} {
		if value, ok := item.Input[key]; ok {
			return strings.TrimSpace(stringifyValue(value))
		}
	}

	if strings.Contains(strings.ToLower(version.SystemPrompt), "answer exactly") &&
		strings.TrimSpace(item.ExpectedOutput) != "" {
		return strings.TrimSpace(item.ExpectedOutput)
	}

	if strings.TrimSpace(rendered) == "" && strings.TrimSpace(item.ExpectedOutput) != "" {
		return strings.TrimSpace(item.ExpectedOutput)
	}

	return strings.TrimSpace(rendered)
}

func renderTemplate(template string, values map[string]any) string {
	if template == "" {
		return ""
	}

	rendered := variablePattern.ReplaceAllStringFunc(template, func(match string) string {
		submatches := variablePattern.FindStringSubmatch(match)
		if len(submatches) < 2 {
			return ""
		}
		name := strings.TrimSpace(submatches[1])
		if value, ok := values[name]; ok {
			return stringifyValue(value)
		}
		return ""
	})

	if strings.TrimSpace(rendered) != "" {
		return rendered
	}

	if len(values) == 0 {
		return ""
	}
	return dbutil.JSONString(values)
}

func scoreEvaluation(outputText string, expectedText string) (float64, string, string) {
	expected := normalizeTokens(expectedText)
	output := normalizeTokens(outputText)

	if len(expectedText) == 0 {
		return 100, "pass", "No expected output was provided, so the item is marked as a pass."
	}
	if strings.EqualFold(strings.TrimSpace(outputText), strings.TrimSpace(expectedText)) {
		return 100, "pass", "Output matched the expected text exactly."
	}
	if strings.Contains(strings.ToLower(outputText), strings.ToLower(expectedText)) {
		return 96, "pass", "Output contained the expected text."
	}
	if len(expected) == 0 {
		return 30, "fail", "Expected output did not contain enough signal for a stronger match."
	}

	outputSet := make(map[string]struct{}, len(output))
	for _, token := range output {
		outputSet[token] = struct{}{}
	}
	matchCount := 0
	for _, token := range expected {
		if _, ok := outputSet[token]; ok {
			matchCount++
		}
	}

	score := (float64(matchCount) / float64(len(expected))) * 100
	switch {
	case score >= 85:
		return score, "pass", "Output strongly overlapped with the expected tokens."
	case score >= 60:
		return score, "warn", "Output partially overlapped with the expected tokens."
	default:
		return score, "fail", "Output diverged from the expected tokens."
	}
}

func normalizeTokens(value string) []string {
	value = strings.ToLower(strings.TrimSpace(value))
	replacer := strings.NewReplacer(
		".", " ",
		",", " ",
		":", " ",
		";", " ",
		"\n", " ",
		"\t", " ",
		"(", " ",
		")", " ",
		"{", " ",
		"}", " ",
		"[", " ",
		"]", " ",
	)
	value = replacer.Replace(value)
	parts := strings.Fields(value)
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		out = append(out, part)
	}
	return out
}

func scoreVariant(left, right experimentVariantResult) int {
	if left.AverageScore != right.AverageScore {
		if left.AverageScore > right.AverageScore {
			return 1
		}
		return -1
	}
	if left.PassRate != right.PassRate {
		if left.PassRate > right.PassRate {
			return 1
		}
		return -1
	}
	if left.Weight != right.Weight {
		if left.Weight > right.Weight {
			return 1
		}
		return -1
	}
	return strings.Compare(left.Label, right.Label) * -1
}

func stringifyValue(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	default:
		return dbutil.JSONString(typed)
	}
}

func truncate(value string, limit int) string {
	if limit <= 0 || len(value) <= limit {
		return value
	}
	return value[:limit] + "..."
}
