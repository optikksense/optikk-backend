package shared

import "time"

// Transition is the result of running the state machine once for one instance.
type Transition struct {
	FromState string
	ToState   string
	Changed   bool
}

// Decide evaluates the threshold policy + hysteresis rules against the
// instance's previous state + new windowed values and returns the resulting
// state transition. This is pure / deterministic — no IO.
func Decide(rule *Rule, inst *Instance, windows map[string]float64, noData bool, now time.Time) Transition {
	prev := inst.State
	if prev == "" {
		prev = StateOK
	}

	// Mutes and silences short-circuit everything.
	if rule.MuteUntil != nil && now.Before(*rule.MuteUntil) {
		return Transition{FromState: prev, ToState: StateMuted, Changed: prev != StateMuted}
	}
	if inst.SnoozedUntil != nil && now.Before(*inst.SnoozedUntil) {
		return Transition{FromState: prev, ToState: StateMuted, Changed: prev != StateMuted}
	}

	if noData {
		// First time entering no-data → stamp the clock.
		if inst.NoDataSince == nil {
			t := now
			inst.NoDataSince = &t
		}
		if rule.NoDataSecs > 0 && now.Sub(*inst.NoDataSince) >= time.Duration(rule.NoDataSecs)*time.Second {
			return Transition{FromState: prev, ToState: StateNoData, Changed: prev != StateNoData}
		}
		return Transition{FromState: prev, ToState: prev, Changed: false}
	}
	inst.NoDataSince = nil

	// Determine threshold crossing against the worst window.
	critical := CrossesAny(windows, rule.Operator, rule.CriticalThreshold)
	warn := false
	if rule.WarnThreshold != nil {
		warn = CrossesAny(windows, rule.Operator, *rule.WarnThreshold)
	}
	recovered := true
	if rule.RecoveryThreshold != nil {
		recovered = !CrossesAny(windows, rule.Operator, *rule.RecoveryThreshold)
	} else {
		recovered = !critical && !warn
	}

	switch prev {
	case StateOK, StateNoData:
		if critical || warn {
			if inst.PendingSince == nil {
				t := now
				inst.PendingSince = &t
			}
			if rule.ForSecs <= 0 || now.Sub(*inst.PendingSince) >= time.Duration(rule.ForSecs)*time.Second {
				to := StateWarn
				if critical {
					to = StateAlert
				}
				inst.PendingSince = nil
				t := now
				inst.FiredAt = &t
				return Transition{FromState: prev, ToState: to, Changed: true}
			}
			return Transition{FromState: prev, ToState: prev, Changed: false}
		}
		inst.PendingSince = nil
		return Transition{FromState: prev, ToState: StateOK, Changed: prev != StateOK}

	case StateWarn, StateAlert:
		if critical {
			if prev == StateAlert {
				return Transition{FromState: prev, ToState: prev, Changed: false}
			}
			return Transition{FromState: prev, ToState: StateAlert, Changed: true}
		}
		if warn {
			if prev == StateWarn {
				return Transition{FromState: prev, ToState: prev, Changed: false}
			}
			return Transition{FromState: prev, ToState: StateWarn, Changed: true}
		}
		if recovered {
			if inst.ResolvedAt == nil {
				t := now
				inst.ResolvedAt = &t
			}
			if rule.RecoverForSecs <= 0 || now.Sub(*inst.ResolvedAt) >= time.Duration(rule.RecoverForSecs)*time.Second {
				inst.FiredAt = nil
				return Transition{FromState: prev, ToState: StateOK, Changed: true}
			}
		}
		return Transition{FromState: prev, ToState: prev, Changed: false}
	}
	return Transition{FromState: prev, ToState: prev, Changed: false}
}

// CrossesAny reports whether any window's value crosses the threshold under
// the given operator. Used by rules.Service.TestRule to compute a dry-run's
// "would fire" flag.
func CrossesAny(windows map[string]float64, op string, threshold float64) bool {
	for _, v := range windows {
		if crosses(v, op, threshold) {
			return true
		}
	}
	return false
}

func crosses(value float64, op string, threshold float64) bool {
	switch op {
	case OpGT:
		return value > threshold
	case OpGTE:
		return value >= threshold
	case OpLT:
		return value < threshold
	case OpLTE:
		return value <= threshold
	case OpEQ:
		return value == threshold
	default:
		return value > threshold
	}
}

// RollUpRuleState walks a rule's instances and returns the worst-case state.
// Precedence: alert > warn > no_data > muted > ok.
func RollUpRuleState(instances InstancesMap) string {
	worst := StateOK
	priority := map[string]int{
		StateOK: 0, StateMuted: 1, StateNoData: 2, StateWarn: 3, StateAlert: 4,
	}
	for _, inst := range instances {
		if priority[inst.State] > priority[worst] {
			worst = inst.State
		}
	}
	return worst
}
