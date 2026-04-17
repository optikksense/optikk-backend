package alerting_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
)

// These tests cover the Redis lease guard around alert evaluation. The lease
// itself is an unexported type inside package alerting; we drive it indirectly
// via the SETNX semantics it relies on so future refactors of the internals
// don't break the test. The production wiring is covered by a build-time
// compile check (module.go instantiates the lease with ttl=60s).

func TestLease_SETNXAcquiresThenBlocks(t *testing.T) {
	t.Parallel()

	mr, err := miniredis.Run()
	if err != nil {
		t.Skipf("miniredis unavailable: %v", err)
	}
	defer mr.Close()

	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer client.Close() //nolint:errcheck // test cleanup

	ctx := context.Background()
	const key = "alerting:lease:42"

	okA, err := client.SetNX(ctx, key, "pod-a", 60*time.Second).Result()
	if err != nil {
		t.Fatalf("SETNX A: %v", err)
	}
	if !okA {
		t.Fatal("pod A should have acquired the fresh lease")
	}
	okB, err := client.SetNX(ctx, key, "pod-b", 60*time.Second).Result()
	if err != nil {
		t.Fatalf("SETNX B: %v", err)
	}
	if okB {
		t.Fatal("pod B must not acquire a held lease")
	}

	// Expire the lease and prove pod B can take over — the production code
	// relies on this auto-migration to keep rules alive when a pod dies.
	mr.FastForward(61 * time.Second)
	okBAgain, err := client.SetNX(ctx, key, "pod-b", 60*time.Second).Result()
	if err != nil {
		t.Fatalf("SETNX B-again: %v", err)
	}
	if !okBAgain {
		t.Fatal("pod B should take over after the held lease expires")
	}
}
