package circuitbreaker

import (
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/logger"
	"github.com/sony/gobreaker"
)

var ErrCircuitOpen = errors.New("circuit breaker open")

type CircuitBreaker struct {
	cb *gobreaker.CircuitBreaker
}

var breakerRegistry syncRegistry

func NewCircuitBreaker(name string, threshold int, resetTimeout time.Duration) *CircuitBreaker {
	return breakerRegistry.getOrCreate(name, threshold, resetTimeout, nil)
}

func NewCircuitBreakerWithSuccessDecider(name string, threshold int, resetTimeout time.Duration, isSuccessful func(error) bool) *CircuitBreaker {
	return breakerRegistry.getOrCreate(name, threshold, resetTimeout, isSuccessful)
}

func (cb *CircuitBreaker) Call(fn func() error) error {
	_, err := cb.cb.Execute(func() (any, error) {
		return nil, fn()
	})
	if errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests) {
		return ErrCircuitOpen
	}
	return err
}

func (cb *CircuitBreaker) State() string {
	return cb.cb.State().String()
}

type syncRegistry struct {
	breakers sync.Map
}

func (r *syncRegistry) getOrCreate(name string, threshold int, resetTimeout time.Duration, isSuccessful func(error) bool) *CircuitBreaker {
	if existing, ok := r.breakers.Load(name); ok {
		cb, _ := existing.(*CircuitBreaker)
		return cb
	}

	created := &CircuitBreaker{
		cb: gobreaker.NewCircuitBreaker(gobreaker.Settings{
			Name:        name,
			MaxRequests: 1,
			Timeout:     resetTimeout,
			ReadyToTrip: func(counts gobreaker.Counts) bool {
				return counts.ConsecutiveFailures >= uint32(threshold) //nolint:gosec // G115 - domain-constrained value
			},
			IsSuccessful: func(err error) bool {
				if isSuccessful != nil {
					return isSuccessful(err)
				}
				return err == nil
			},
			OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
				logger.L().Warn("circuit_breaker state change", slog.String("name", name), slog.String("from", from.String()), slog.String("to", to.String()))
			},
		}),
	}

	actual, _ := r.breakers.LoadOrStore(name, created)
	cb, _ := actual.(*CircuitBreaker)
	return cb
}
