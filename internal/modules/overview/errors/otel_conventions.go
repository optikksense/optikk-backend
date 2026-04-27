package errors

import rootspan "github.com/Optikk-Org/optikk-backend/internal/modules/traces/shared/rootspan"

// Raw ClickHouse column references for observability.spans (aliased
// as s). Resource-scoped filters in this module flow through
// internal/modules/traces/shared/resource.WithFingerprints into a
// `fingerprint IN (...)` PREWHERE — no JOIN against a separate
// resource table is required.

func ErrorCondition() string {
	return "s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400"
}

func RootSpanCondition() string {
	return rootspan.Condition("s")
}
