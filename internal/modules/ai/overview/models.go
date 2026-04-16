package overview

type qualitySummaryRow struct {
	AvgQualityScore  float64 `ch:"avg_quality_score"`
	AvgFeedbackScore float64 `ch:"avg_feedback_score"`
	ScoredRuns       uint64  `ch:"scored_runs"`
	FeedbackRuns     uint64  `ch:"feedback_runs"`
	GuardrailBlocks  uint64  `ch:"guardrail_blocks"`
}
