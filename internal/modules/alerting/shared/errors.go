package shared

import "errors"

// ErrRuleNotFound is returned when a rule lookup by (team_id, id) finds nothing.
var ErrRuleNotFound = errors.New("alerting: rule not found")

// ErrUnsupportedCondition is returned when a rule specifies a condition_type
// that has no registered evaluator.
var ErrUnsupportedCondition = errors.New("alerting: unsupported condition type")
