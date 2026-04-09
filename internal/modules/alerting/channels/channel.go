// Package channels holds delivery channel implementations for dispatched
// alerts. v1 only ships Slack; the registry is channel-agnostic so webhook,
// email, PagerDuty etc. become additive registrations.
package channels

import "context"

// Rendered is the payload produced by alerting.Template rendering.
type Rendered struct {
	Title       string
	Body        string
	Severity    string
	DeepLinkURL string
	Tags        map[string]string
}

// Channel sends a rendered alert. Implementations must be safe for concurrent use.
type Channel interface {
	Name() string
	Send(ctx context.Context, target string, r Rendered) error
}
