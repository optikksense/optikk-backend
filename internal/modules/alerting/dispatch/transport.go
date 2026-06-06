// Package dispatch sends notification payloads to notification channels.
// Transports route payloads depending on their channel type.
package dispatch

import (
	"context"

	models "github.com/Optikk-Org/optikk-backend/internal/modules/alerting/shared/models"
)

// Payload is the transport-agnostic message passed to the underlying Transport.
type Payload struct {
	MonitorID    int64
	MonitorName  string
	MonitorURL   string
	// MonitorType is metric, apm, or log.
	MonitorType  string
	// Priority ranges from P1 to P4.
	Priority     string
	// Transition represents status change, e.g. ok->alert, alert->ok.
	Transition   string
	// Status represents the alert state (e.g. alert, warn, ok, no_data).
	Status       string
	Value        float64
	Threshold    float64
	ScopeSummary string
	// Message is the rendered template body.
	Message      string
	IsAlert      bool
	IsWarning    bool
	IsRecovery   bool
}

// Transport sends a Payload to a single channel.
type Transport interface {
	Send(ctx context.Context, ch models.ChannelRow, p Payload) error
}

// Dispatcher routes a Payload to the right Transport for the given channel.
type Dispatcher interface {
	Dispatch(ctx context.Context, ch models.ChannelRow, p Payload) error
}
