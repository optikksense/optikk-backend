package contracts

// Request & validation errors (4xx)
const (
	BadRequest      = "BAD_REQUEST"
	Validation      = "VALIDATION_ERROR"
	Unauthorized    = "UNAUTHORIZED"
	Forbidden       = "FORBIDDEN"
	NotFound        = "NOT_FOUND"
	Conflict        = "CONFLICT"
	PayloadTooLarge = "PAYLOAD_TOO_LARGE"
	RateLimited     = "RATE_LIMITED"
)

// Server & infrastructure errors (5xx)
const (
	Internal        = "INTERNAL_ERROR"
	QueryFailed     = "QUERY_FAILED"
	QueryTimeout    = "QUERY_TIMEOUT"
	ConnectionError = "CONNECTION_ERROR"
	Unavailable     = "SERVICE_UNAVAILABLE"
	CircuitOpen     = "CIRCUIT_OPEN"
)

// Data-level codes (returned inside success responses)
const (
	NoData      = "NO_DATA"
	PartialData = "PARTIAL_DATA"
)

// Alerting module codes.
const (
	AlertRuleNotFound         = "ALERT_RULE_NOT_FOUND"
	AlertInstanceNotFound     = "ALERT_INSTANCE_NOT_FOUND"
	AlertRuleInvalidCondition = "ALERT_RULE_INVALID_CONDITION"
	AlertChannelNotFound      = "ALERT_CHANNEL_NOT_FOUND"
	AlertSilenceNotFound      = "ALERT_SILENCE_NOT_FOUND"
)
