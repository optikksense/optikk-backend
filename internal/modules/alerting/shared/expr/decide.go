// Package expr is the evaluator's pure decide function. Given the previous
// state, a fresh evaluation value, and the monitor's stored conditions, it
// returns the new status and the transition flags the evaluator uses to decide
// whether to write a monitor_events row or fire a dispatch.
package expr

import (
	"time"

	models "github.com/Optikk-Org/optikk-backend/internal/modules/alerting/shared/models"
)

// Decision is the output of Decide. NewStatus is always populated; Transition
// is true iff the status differs from prev.Status (no-data → no-data is not a
// transition). ShouldNotify reflects whether dispatch is owed — either a
// transition into alert/warn/recovered, or a renotify interval elapsed while
// alerting.
type Decision struct {
	NewStatus    string
	Transition   bool
	ShouldNotify bool
	IsRecovery   bool
}

// Decide is pure: same inputs → same Decision. Hysteresis lives here:
// recovery requires the value to cross RecoveryThreshold (not Warn) so a
// monitor that bounces between alert and warn around the warn line doesn't
// flap. NoData disposition (`no_data` | `alert` | `ok`) is consulted only
// when the underlying query had no data.
func Decide(prev models.MonitorStateRow, m models.MonitorRow, cond models.Conditions, value float64, hasData bool, renotifyEverySec int64, now time.Time) Decision {
	prevStatus := prev.Status
	if prevStatus == "" {
		prevStatus = "no_data"
	}
	newStatus := classify(prevStatus, cond, value, hasData)
	transition := newStatus != prevStatus

	notify := false
	isRecovery := false
	switch {
	case transition && newStatus == "alert":
		notify = true
	case transition && newStatus == "warn":
		notify = true
	case transition && newStatus == "ok" && (prevStatus == "alert" || prevStatus == "warn"):
		notify = true
		isRecovery = true
	case !transition && newStatus == "alert" && renotifyEverySec > 0 && prev.LastNotifiedAt.Valid:
		elapsed := now.Sub(prev.LastNotifiedAt.Time)
		if elapsed >= time.Duration(renotifyEverySec)*time.Second {
			notify = true
		}
	}
	return Decision{
		NewStatus:    newStatus,
		Transition:   transition,
		ShouldNotify: notify,
		IsRecovery:   isRecovery,
	}
}

// classify maps a (prev, value) pair to a new status under the monitor's
// conditions. Recovery threshold is consulted only when prev is alert/warn —
// from ok we use warn/alert directly. This is the hysteresis loop.
func classify(prev string, cond models.Conditions, value float64, hasData bool) string {
	if !hasData {
		switch cond.NoDataAs {
		case "alert":
			return "alert"
		case "ok":
			return "ok"
		default:
			return "no_data"
		}
	}
	cmp := cond.Comparator
	if cmp == "" {
		cmp = "above"
	}
	hit := func(threshold *float64) bool {
		if threshold == nil {
			return false
		}
		switch cmp {
		case "above":
			return value > *threshold
		case "below":
			return value < *threshold
		case "equal":
			return value == *threshold
		}
		return false
	}
	hitRecovery := func() bool {
		t := cond.RecoveryThreshold
		if t == nil {
			t = cond.WarnThreshold
		}
		if t == nil {
			t = cond.AlertThreshold
		}
		if t == nil {
			return true
		}
		switch cmp {
		case "above":
			return value <= *t
		case "below":
			return value >= *t
		case "equal":
			return value != *t
		}
		return true
	}

	switch prev {
	case "alert", "warn":
		if hit(cond.AlertThreshold) {
			return "alert"
		}
		if hit(cond.WarnThreshold) {
			return "warn"
		}
		if hitRecovery() {
			return "ok"
		}
		return prev
	default:
		if hit(cond.AlertThreshold) {
			return "alert"
		}
		if hit(cond.WarnThreshold) {
			return "warn"
		}
		return "ok"
	}
}
