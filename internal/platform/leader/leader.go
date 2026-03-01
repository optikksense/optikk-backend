package leader

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Elector decides which pod should run singleton background jobs (e.g. the
// RetentionManager) so they don't execute on every replica simultaneously.
//
// Two implementations are provided:
//
//	ProcessElector  – always elected (single-pod / local dev)
//	RedisElector    – distributed election via Redis SET NX (no k8s dep required)
type Elector interface {
	// IsLeader returns the current (cached) leader status.
	IsLeader(ctx context.Context) bool
	// RunWhenLeader blocks until ctx is cancelled. Calls fn(ctx) as the leader;
	// cancels fn's context if the lease is lost.
	RunWhenLeader(ctx context.Context, fn func(ctx context.Context))
}

// ── Process-level elector (single-pod / dev) ────────────────────────────────

// ProcessElector is always elected — suitable for single-pod / local dev.
type ProcessElector struct{}

// NewProcessElector creates an elector that is always considered leader.
func NewProcessElector() *ProcessElector { return &ProcessElector{} }

// IsLeader always returns true.
func (p *ProcessElector) IsLeader(_ context.Context) bool { return true }

// RunWhenLeader immediately invokes fn and blocks until it returns.
func (p *ProcessElector) RunWhenLeader(ctx context.Context, fn func(context.Context)) {
	fn(ctx)
}

// ── Redis SET NX elector (multi-pod) ────────────────────────────────────────

// RedisElector uses a Redis SET NX key with a TTL as a distributed lease.
// The pod that successfully sets the key becomes leader and periodically
// renews the TTL. If the leader crashes the TTL expires after leaseTTL and
// another pod takes over.
type RedisElector struct {
	client   *redis.Client
	key      string
	podID    string
	leaseTTL time.Duration
	mu       sync.RWMutex
	elected  bool
}

// NewRedisElector creates an elector backed by a Redis SET NX key.
//   - key: Redis key used as the distributed lock (e.g. "optic:retention-leader")
//   - leaseTTL: how long the key lives if not renewed (should be > 3x tick interval)
func NewRedisElector(client *redis.Client, key string, leaseTTL time.Duration) *RedisElector {
	podID := os.Getenv("POD_NAME")
	if podID == "" {
		h, _ := os.Hostname()
		podID = h
	}
	return &RedisElector{
		client:   client,
		key:      key,
		podID:    podID,
		leaseTTL: leaseTTL,
	}
}

// IsLeader returns the cached election status (non-blocking).
func (e *RedisElector) IsLeader(_ context.Context) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.elected
}

// RunWhenLeader continuously tries to acquire/renew the leader lease and runs
// fn when this pod is elected. fn's context is cancelled if leadership is
// lost. This method blocks until ctx is cancelled.
func (e *RedisElector) RunWhenLeader(ctx context.Context, fn func(context.Context)) {
	renewInterval := e.leaseTTL / 3
	ticker := time.NewTicker(renewInterval)
	defer ticker.Stop()

	var (
		leaderCtx    context.Context
		leaderCancel context.CancelFunc
	)

	tryAcquire := func() {
		acquired := e.tryAcquireOrRenew(ctx)

		e.mu.Lock()
		wasLeader := e.elected
		e.elected = acquired
		e.mu.Unlock()

		if acquired && !wasLeader {
			log.Printf("leader: pod %s elected for key %s", e.podID, e.key)
			leaderCtx, leaderCancel = context.WithCancel(ctx)
			go fn(leaderCtx)
		} else if !acquired && wasLeader {
			log.Printf("leader: pod %s lost lease for key %s", e.podID, e.key)
			if leaderCancel != nil {
				leaderCancel()
				leaderCancel = nil
			}
		}
	}

	tryAcquire()
	for {
		select {
		case <-ctx.Done():
			if leaderCancel != nil {
				leaderCancel()
			}
			return
		case <-ticker.C:
			tryAcquire()
		}
	}
}

func (e *RedisElector) tryAcquireOrRenew(ctx context.Context) bool {
	tctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()

	e.mu.RLock()
	alreadyLeader := e.elected
	e.mu.RUnlock()

	if alreadyLeader {
		val, err := e.client.Get(tctx, e.key).Result()
		if err != nil || val != e.podID {
			return false
		}
		_, err = e.client.Expire(tctx, e.key, e.leaseTTL).Result()
		return err == nil
	}

	ok, err := e.client.SetNX(tctx, e.key, e.podID, e.leaseTTL).Result()
	return err == nil && ok
}
