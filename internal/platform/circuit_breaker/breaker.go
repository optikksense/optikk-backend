package circuitbreaker

import (
	"errors"
	"log"
	"sync"
	"time"

	"github.com/sony/gobreaker"
)

var ErrCircuitOpen = errors.New("circuit breaker open")

type CircuitBreaker struct {
	cb *gobreaker.CircuitBreaker
}

var breakerRegistry syncRegistry

func NewCircuitBreaker(name string, threshold int, resetTimeout time.Duration) *CircuitBreaker {
	return breakerRegistry.getOrCreate(name, threshold, resetTimeout)
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

func (r *syncRegistry) getOrCreate(name string, threshold int, resetTimeout time.Duration) *CircuitBreaker {
	if existing, ok := r.breakers.Load(name); ok {
		return existing.(*CircuitBreaker)
	}

	created := &CircuitBreaker{
		cb: gobreaker.NewCircuitBreaker(gobreaker.Settings{
			Name:        name,
			MaxRequests: 1,
			Timeout:     resetTimeout,
			ReadyToTrip: func(counts gobreaker.Counts) bool {
				return counts.ConsecutiveFailures >= uint32(threshold)
			},
			OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
				log.Printf("circuit_breaker: %s transitioned %s -> %s", name, from.String(), to.String())
			},
		}),
	}

	actual, _ := r.breakers.LoadOrStore(name, created)
	return actual.(*CircuitBreaker)
}
