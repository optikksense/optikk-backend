package ingestion_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/ingestion"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/streamworkers"
)

// stubDispatcher satisfies ingestion.Dispatcher[T] for test fan-in. Only
// Persistence() is exercised by RunPersistence; Streaming/Dispatch/Close are
// inert stubs.
type stubDispatcher[T any] struct {
	persistence chan ingestion.AckableBatch[T]
	streaming   chan ingestion.TelemetryBatch[T]
}

func newStubDispatcher[T any]() *stubDispatcher[T] {
	return &stubDispatcher[T]{
		persistence: make(chan ingestion.AckableBatch[T], 8),
		streaming:   make(chan ingestion.TelemetryBatch[T], 8),
	}
}

func (s *stubDispatcher[T]) Dispatch(_ ingestion.TelemetryBatch[T]) error  { return nil }
func (s *stubDispatcher[T]) Persistence() <-chan ingestion.AckableBatch[T] { return s.persistence }
func (s *stubDispatcher[T]) Streaming() <-chan ingestion.TelemetryBatch[T] { return s.streaming }
func (s *stubDispatcher[T]) Close()                                        {}

type fakeRow struct{ ID int }

// TestRunPersistence_AcksOnSuccess verifies that a successful flush results
// in Ack(nil) — the contract that commits the source Kafka offset.
func TestRunPersistence_AcksOnSuccess(t *testing.T) {
	t.Parallel()

	d := newStubDispatcher[fakeRow]()
	var flushCalls, tokenMismatch atomic.Int32
	flush := func(rows []fakeRow, token string) error {
		flushCalls.Add(1)
		if token != "tok-1" {
			tokenMismatch.Add(1)
		}
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		streamworkers.RunPersistence[fakeRow](ctx, "test", d, flush)
		close(done)
	}()

	var (
		mu       sync.Mutex
		ackCalls int
		lastErr  error
	)
	d.persistence <- ingestion.AckableBatch[fakeRow]{
		Batch:      ingestion.TelemetryBatch[fakeRow]{TeamID: 7, Rows: []fakeRow{{ID: 1}}},
		DedupToken: "tok-1",
		Ack: func(err error) {
			mu.Lock()
			defer mu.Unlock()
			ackCalls++
			lastErr = err
		},
	}

	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return ackCalls == 1
	})
	cancel()
	<-done

	if flushCalls.Load() != 1 {
		t.Fatalf("expected 1 flush, got %d", flushCalls.Load())
	}
	if tokenMismatch.Load() != 0 {
		t.Fatal("dedup token not propagated to flush")
	}
	mu.Lock()
	defer mu.Unlock()
	if lastErr != nil {
		t.Fatalf("expected Ack(nil) on flush success, got %v", lastErr)
	}
}

// TestRunPersistence_AcksErrorOnFlushFailure verifies that a flush error is
// propagated to Ack so the dispatcher can route the record to DLQ and commit
// the offset rather than block the partition or drop silently.
func TestRunPersistence_AcksErrorOnFlushFailure(t *testing.T) {
	t.Parallel()

	d := newStubDispatcher[fakeRow]()
	sentinel := errors.New("clickhouse connection reset")
	flush := func(rows []fakeRow, token string) error {
		return sentinel
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		streamworkers.RunPersistence[fakeRow](ctx, "test", d, flush)
		close(done)
	}()

	received := make(chan error, 1)
	d.persistence <- ingestion.AckableBatch[fakeRow]{
		Batch: ingestion.TelemetryBatch[fakeRow]{TeamID: 7, Rows: []fakeRow{{ID: 1}}},
		Ack:   func(err error) { received <- err },
	}

	select {
	case got := <-received:
		if !errors.Is(got, sentinel) {
			t.Fatalf("expected flush error propagated to Ack, got %v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Ack was never called on flush failure")
	}

	cancel()
	<-done
}

// TestRunPersistence_ExitsOnContextCancel proves the worker shuts down cleanly
// when its context is cancelled — needed for graceful pod drain so that a
// pending AckableBatch is not consumed after the dispatcher is closed.
func TestRunPersistence_ExitsOnContextCancel(t *testing.T) {
	t.Parallel()

	d := newStubDispatcher[fakeRow]()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		streamworkers.RunPersistence[fakeRow](ctx, "test", d, func([]fakeRow, string) error { return nil })
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RunPersistence did not return after context cancel")
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("waitFor: condition never became true")
}
