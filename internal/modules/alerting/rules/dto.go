package rules

import (
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/shared"
)

// Rule lifecycle DTOs (CRUD + preview + mute + test + backtest + audit).
// Shared at the top-level alerting package currently for legacy reasons;
// re-exported here via type aliases so handlers/service/tests refer only to
// rules.CreateRuleRequest etc.

type CreateRuleRequest = shared.CreateRuleRequest
type UpdateRuleRequest = shared.UpdateRuleRequest
type RuleResponse = shared.RuleResponse
type PreviewRuleResponse = shared.PreviewRuleResponse
type MuteRuleRequest = shared.MuteRuleRequest
type TestRuleResponse = shared.TestRuleResponse
type TestInstanceResult = shared.TestInstanceResult
type BacktestRequest = shared.BacktestRequest
type BacktestResponse = shared.BacktestResponse
type AuditEntry = shared.AuditEntry

// ensure time import stays valid even if the aliases above are all the types
// exposed (keeps imports compact if we later add local-only DTOs here).
var _ = time.Time{}
