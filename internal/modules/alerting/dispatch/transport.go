// Package dispatch sends a rendered notification payload to a notification
// channel. Each Transport is one channel type. The Dispatcher selects an
// implementation by channel.Type — Slack is the only wired transport in v1;
// every other type maps to Stub (logs the payload, marks delivered).
package dispatch

import (
	"context"

	models "github.com/Optikk-Org/optikk-backend/internal/modules/alerting/shared/models"
)

// Payload is the transport-agnostic message produced by the evaluator and
// passed through the Dispatcher to the underlying Transport.
type Payload struct {
	MonitorID     int64
	MonitorName   string
	MonitorURL    string
	MonitorType   string // metric | apm | log
	Priority      string // P1..P4
	Transition    string // ok->alert | alert->ok | warn->alert | etc.
	Status        string // alert | warn | ok | no_data
	Value         float64
	Threshold     float64
	ScopeSummary  string
	Message       string // rendered template body
	IsAlert       bool
	IsWarning     bool
	IsRecovery    bool
}

// Transport sends a Payload to a single channel.
type Transport interface {
	Send(ctx context.Context, ch models.ChannelRow, p Payload) error
}

// Dispatcher routes a Payload to the right Transport for the given channel.
type Dispatcher interface {
	Dispatch(ctx context.Context, ch models.ChannelRow, p Payload) error
}
