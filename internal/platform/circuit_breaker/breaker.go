package circuitbreaker

import (
	"errors"
	"log"
	"math/rand"
	"sync"
	"time"
)

// CircuitBreaker reopens after a cooldown plus jitter to avoid stampeding
// retries when a dependency starts recovering.
var ErrCircuitOpen = errors.New("circuit breaker open")

type cbState int

const (
	cbClosed cbState = iota
	cbOpen
	cbHalfOpen
)

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

func NewCircuitBreaker(name string, threshold int, resetTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		name:         name,
		Threshold:    threshold,
		ResetTimeout: resetTimeout,
	}
}

func (cb *CircuitBreaker) Call(fn func() error) error {
	cb.mu.Lock()
	switch cb.state {
	case cbOpen:
		// Add 0-25% jitter to prevent thundering herd on recovery.
		jitter := time.Duration(rand.Int63n(int64(cb.ResetTimeout / 4)))
		if time.Since(cb.openedAt) < cb.ResetTimeout+jitter {
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
	prevState := cb.state
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
	if cb.state != prevState {
		log.Printf("circuit_breaker: %s transitioned %s -> %s (failures=%d)",
			cb.name, stateName(prevState), stateName(cb.state), cb.failures)
	}
	return err
}

func stateName(s cbState) string {
	switch s {
	case cbOpen:
		return "open"
	case cbHalfOpen:
		return "half-open"
	default:
		return "closed"
	}
}

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
