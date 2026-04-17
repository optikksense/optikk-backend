package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// Leaser guards rule evaluation across pods: each pod tries to acquire a
// short-lived Redis key per rule before evaluating; a dead pod's rules are
// picked up by another pod within the lease TTL.
//
// When the client is nil (dev / single-pod), Acquire always succeeds so the
// HA path is purely opt-in.
type Leaser struct {
	client *goredis.Client
	podID  string
	ttl    time.Duration
}

func NewLeaser(client *goredis.Client, ttl time.Duration) *Leaser {
	if ttl <= 0 {
		ttl = 60 * time.Second
	}
	return &Leaser{client: client, podID: newPodID(), ttl: ttl}
}

// Acquire returns true if this pod holds the lease for ruleID. Fail-closed on
// Redis errors to prevent double-dispatch.
func (l *Leaser) Acquire(ctx context.Context, ruleID int64) bool {
	if l == nil || l.client == nil {
		return true
	}
	key := fmt.Sprintf("alerting:lease:%d", ruleID)
	ok, err := l.client.SetNX(ctx, key, l.podID, l.ttl).Result()
	if err != nil {
		return false
	}
	return ok
}

func newPodID() string {
	var b [8]byte
	_, _ = rand.Read(b[:]) //nolint:errcheck // crypto/rand on modern kernels never fails
	return hex.EncodeToString(b[:])
}
