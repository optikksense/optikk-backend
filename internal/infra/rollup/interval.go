package rollup

// BucketInterval returns the minute interval a reader should bind as
// `@intervalMin` in its SELECT. It is max(tier's native step, uiStepMin)
// so queries can re-bucket to a coarser resolution than the tier provides
// but never finer. A 0 uiStepMin (caller doesn't care) falls back to the
// tier's native step.
//
// Replaces the pre-rewrite `queryIntervalMinutes` helper that lived in two
// places (overview/redmetrics + saturation/database/internal/shared). Now
// one source of truth; readers migrate to this.
func BucketInterval(t Tier, uiStepMin int64) int64 {
	if uiStepMin <= 0 || uiStepMin < t.StepMin {
		return t.StepMin
	}
	return uiStepMin
}
