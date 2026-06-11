// Package expr provides the pure decide function for evaluating monitor state
// transitions.
package expr

import (
	"time"

	models "github.com/Optikk-Org/optikk-backend/internal/modules/alerting/shared/models"
)

// Decision contains the output status and notification flags of Decide.
type Decision struct {
	NewStatus    string
	Transition   bool
	ShouldNotify bool
	IsRecovery   bool
}

// Decide evaluates a monitor's conditions to output a new Decision.
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

// classify maps a (prev, value) pair to a new status under the conditions.
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
