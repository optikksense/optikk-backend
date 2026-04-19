package sketch

import (
	"encoding/hex"
	"hash/fnv"
	"strconv"
	"strings"
	"time"
)

const keyPrefix = "optikk:sk:"

// HashDim collapses a dimension tuple (e.g. "service|operation|endpoint|method")
// to a short hex string. Stable across processes; only used as part of a Redis
// key, so a 64-bit FNV-1a is enough — collisions within a single tenant are
// astronomically rare and a collision only costs a merged sketch between two
// tuples, which is still a correct sketch just over the wrong partition.
func HashDim(dim string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(dim))
	out := make([]byte, 16)
	hex.Encode(out, h.Sum(nil))
	return string(out)
}

// Key is the Redis key for a single (kind, team, dim, minute-bucket) sketch.
type Key struct {
	Kind     Kind
	TeamID   string
	DimHash  string
	BucketTs time.Time // normalized to the kind's Bucket
}

// String renders the canonical Redis key. Keep the segments stable — the
// aggregator, store, and querier all parse/produce this format.
//
//	optikk:sk:<kindID>:<teamID>:<dimHash>:<unixSeconds>
func (k Key) String() string {
	var b strings.Builder
	b.Grow(64)
	b.WriteString(keyPrefix)
	b.WriteString(k.Kind.ID)
	b.WriteByte(':')
	b.WriteString(k.TeamID)
	b.WriteByte(':')
	b.WriteString(k.DimHash)
	b.WriteByte(':')
	b.WriteString(strconv.FormatInt(k.BucketTs.Unix(), 10))
	return b.String()
}

// TenantPrefix returns "<prefix><kindID>:<teamID>:" — the SCAN prefix used by
// LoadRange to enumerate every sketch for a tenant + kind.
func TenantPrefix(kind Kind, teamID string) string {
	return keyPrefix + kind.ID + ":" + teamID + ":"
}

// DimLabelKey is the Redis key for the reverse dim-hash → dim-string label,
// used so LoadRange can return map[dimString]{sketches}.
func DimLabelKey(kind Kind, teamID, dimHash string) string {
	return keyPrefix + "dim:" + kind.ID + ":" + teamID + ":" + dimHash
}

// BucketFloor aligns a timestamp down to the kind's bucket boundary.
func BucketFloor(kind Kind, ts time.Time) time.Time {
	return ts.UTC().Truncate(kind.Bucket)
}
