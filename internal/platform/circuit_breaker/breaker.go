package circuitbreaker

import (
	"errors"
	"sync"
	"time"
)

// ── Circuit Breaker ────────────────────────────────────────────────────────
//
// CircuitBreaker implements the classic three-state pattern (Closed → Open →
// Half-Open) without any external dependencies. It wraps arbitrary function
// calls and returns ErrCircuitOpen when the circuit is open.
//
// States:
//   - Closed (normal): requests pass through; failures are counted.
//   - Open: requests are rejected immediately after consecutiveFailures
//     exceeds Threshold within the window.
//   - Half-Open: after resetTimeout the circuit allows a single probe request.
//     If it succeeds the circuit resets to Closed; if it fails it goes back Open.
//
// Usage:
//
//	cb := circuitbreaker.NewCircuitBreaker("clickhouse", 5, 30*time.Second)
//	err := cb.Call(func() error { return doClickHouseQuery() })
//	if errors.Is(err, circuitbreaker.ErrCircuitOpen) {
//	    // surface as 503
//	}

// ErrCircuitOpen is returned when a call is rejected because the circuit is open.
var ErrCircuitOpen = errors.New("circuit breaker open")

type cbState int

const (
	cbClosed cbState = iota
	cbOpen
	cbHalfOpen
)

// CircuitBreaker is safe for concurrent use.
type CircuitBreaker struct {
	name     string
	mu       sync.Mutex
	state    cbState
	failures int
	// Threshold is the consecutive failure count that trips the circuit.
	Threshold int
	// ResetTimeout is how long the circuit stays open before attempting a probe.
	ResetTimeout time.Duration
	openedAt     time.Time
}

// NewCircuitBreaker creates a circuit breaker that opens after `threshold`
// consecutive failures and resets after `resetTimeout`.
func NewCircuitBreaker(name string, threshold int, resetTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		name:         name,
		Threshold:    threshold,
		ResetTimeout: resetTimeout,
	}
}

// Call executes fn under the circuit breaker.
// Returns ErrCircuitOpen without calling fn when the circuit is open.
func (cb *CircuitBreaker) Call(fn func() error) error {
	cb.mu.Lock()
	switch cb.state {
	case cbOpen:
		if time.Since(cb.openedAt) < cb.ResetTimeout {
			cb.mu.Unlock()
			return ErrCircuitOpen
		}
		// Transition to half-open and allow a single probe.
		cb.state = cbHalfOpen
	}
	cb.mu.Unlock()

	err := fn()

	cb.mu.Lock()
	defer cb.mu.Unlock()
	if err != nil {
		cb.failures++
		if cb.state == cbHalfOpen || cb.failures >= cb.Threshold {
			cb.state = cbOpen
			cb.openedAt = time.Now()
		}
	} else {
		cb.failures = 0
		cb.state = cbClosed
	}
	return err
}

// State returns a human-readable state string (useful for health endpoints).
func (cb *CircuitBreaker) State() string {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	switch cb.state {
	case cbOpen:
		return "open"
	case cbHalfOpen:
		return "half-open"
	default:
		return "closed"
	}
}
