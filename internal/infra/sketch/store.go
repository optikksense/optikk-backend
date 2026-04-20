package sketch

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	goredis "github.com/redis/go-redis/v9"
)

// Store is the Redis-backed persistence layer for sketches. Writes merge with
// any existing bytes at the key; reads return one sketch per (dim, bucket)
// across the range.
type Store interface {
	WriteDigest(ctx context.Context, key Key, d *Digest, dim string) error
	WriteHLL(ctx context.Context, key Key, h *HLL, dim string) error
	LoadDigests(ctx context.Context, kind Kind, teamID string, startMs, endMs int64) (map[string][]*Digest, error)
	LoadDigestsBucketed(ctx context.Context, kind Kind, teamID string, startMs, endMs int64) (map[string]map[int64]*Digest, error)
	LoadHLLs(ctx context.Context, kind Kind, teamID string, startMs, endMs int64) (map[string][]*HLL, error)
}

// RedisStore implements Store against a single go-redis client.
type RedisStore struct {
	c *goredis.Client
}

func NewRedisStore(c *goredis.Client) *RedisStore { return &RedisStore{c: c} }

// WriteDigest atomically merges d into whatever is already at key and resets
// the TTL. Partition ownership in the Kafka consumer guarantees a single
// writer per key at a given moment, so GET→Merge→SET is safe without a Lua
// script; retries are handled at the caller.
func (s *RedisStore) WriteDigest(ctx context.Context, key Key, d *Digest, dim string) error {
	if s == nil || s.c == nil || d == nil {
		return errors.New("sketch: redis store not configured")
	}
	k := key.String()
	existing, err := s.c.Get(ctx, k).Bytes()
	if err != nil && !errors.Is(err, goredis.Nil) {
		return fmt.Errorf("sketch get: %w", err)
	}
	merged := d
	if len(existing) > 0 {
		prev, perr := UnmarshalDigest(existing)
		if perr == nil {
			if mErr := prev.MergeWith(d); mErr == nil {
				merged = prev
			}
		}
	}
	buf := MarshalDigest(merged)
	if len(buf) == 0 {
		return fmt.Errorf("sketch marshal: empty buffer")
	}
	if err := s.c.Set(ctx, k, buf, key.Kind.TTL).Err(); err != nil {
		return fmt.Errorf("sketch set: %w", err)
	}
	return s.writeDimLabel(ctx, key, dim)
}

// WriteHLL mirrors WriteDigest for cardinality sketches.
func (s *RedisStore) WriteHLL(ctx context.Context, key Key, h *HLL, dim string) error {
	if s == nil || s.c == nil || h == nil {
		return errors.New("sketch: redis store not configured")
	}
	k := key.String()
	existing, err := s.c.Get(ctx, k).Bytes()
	if err != nil && !errors.Is(err, goredis.Nil) {
		return fmt.Errorf("sketch hll get: %w", err)
	}
	merged := h
	if len(existing) > 0 {
		prev, perr := UnmarshalHLL(existing)
		if perr == nil {
			if mErr := prev.Merge(h); mErr == nil {
				merged = prev
			}
		}
	}
	buf, err := merged.MarshalBinary()
	if err != nil {
		return fmt.Errorf("sketch hll marshal: %w", err)
	}
	if err := s.c.Set(ctx, k, buf, key.Kind.TTL).Err(); err != nil {
		return fmt.Errorf("sketch hll set: %w", err)
	}
	return s.writeDimLabel(ctx, key, dim)
}

func (s *RedisStore) writeDimLabel(ctx context.Context, key Key, dim string) error {
	if dim == "" {
		return nil
	}
	return s.c.Set(ctx, DimLabelKey(key.Kind, key.TeamID, key.DimHash), dim, key.Kind.TTL).Err()
}

// LoadDigests returns {dimLabel → [sketches]} for every key in [startMs,endMs]
// matching (kind, teamID). Range filter is applied after SCAN because Redis
// has no native range query on the unix-second key suffix.
func (s *RedisStore) LoadDigests(ctx context.Context, kind Kind, teamID string, startMs, endMs int64) (map[string][]*Digest, error) {
	if s == nil || s.c == nil {
		return nil, errors.New("sketch: redis store not configured")
	}
	keys, err := s.scanKeys(ctx, kind, teamID, startMs, endMs)
	if err != nil || len(keys) == 0 {
		return nil, err
	}
	out := make(map[string][]*Digest, len(keys))
	values, err := s.mget(ctx, keys)
	if err != nil {
		return nil, err
	}
	for i, k := range keys {
		if values[i] == nil {
			continue
		}
		buf, ok := values[i].(string)
		if !ok || len(buf) == 0 {
			continue
		}
		d, derr := UnmarshalDigest([]byte(buf))
		if derr != nil {
			continue
		}
		dim := s.resolveDim(ctx, kind, teamID, dimHashFromKey(k))
		out[dim] = append(out[dim], d)
	}
	return out, nil
}

// LoadDigestsBucketed returns {dimLabel → {bucketUnix → digest}}. The bucket
// timestamp is preserved so callers can assemble time-series answers without
// a second round-trip. bucketUnix is the seconds-since-epoch of the bucket
// START (kind.Bucket-aligned). If the same (dim, bucket) has multiple
// writes, the last one wins on the map load (Redis WriteDigest has already
// merged into a single value server-side).
func (s *RedisStore) LoadDigestsBucketed(ctx context.Context, kind Kind, teamID string, startMs, endMs int64) (map[string]map[int64]*Digest, error) {
	if s == nil || s.c == nil {
		return nil, errors.New("sketch: redis store not configured")
	}
	keys, err := s.scanKeys(ctx, kind, teamID, startMs, endMs)
	if err != nil || len(keys) == 0 {
		return nil, err
	}
	values, err := s.mget(ctx, keys)
	if err != nil {
		return nil, err
	}
	out := make(map[string]map[int64]*Digest, len(keys))
	for i, k := range keys {
		if values[i] == nil {
			continue
		}
		buf, ok := values[i].(string)
		if !ok || len(buf) == 0 {
			continue
		}
		d, derr := UnmarshalDigest([]byte(buf))
		if derr != nil {
			continue
		}
		dim := s.resolveDim(ctx, kind, teamID, dimHashFromKey(k))
		ts := unixFromKey(k)
		byBucket := out[dim]
		if byBucket == nil {
			byBucket = make(map[int64]*Digest, 8)
			out[dim] = byBucket
		}
		byBucket[ts] = d
	}
	return out, nil
}

// LoadHLLs mirrors LoadDigests for cardinality sketches.
func (s *RedisStore) LoadHLLs(ctx context.Context, kind Kind, teamID string, startMs, endMs int64) (map[string][]*HLL, error) {
	if s == nil || s.c == nil {
		return nil, errors.New("sketch: redis store not configured")
	}
	keys, err := s.scanKeys(ctx, kind, teamID, startMs, endMs)
	if err != nil || len(keys) == 0 {
		return nil, err
	}
	values, err := s.mget(ctx, keys)
	if err != nil {
		return nil, err
	}
	out := make(map[string][]*HLL, len(keys))
	for i, k := range keys {
		if values[i] == nil {
			continue
		}
		buf, ok := values[i].(string)
		if !ok || len(buf) == 0 {
			continue
		}
		h, herr := UnmarshalHLL([]byte(buf))
		if herr != nil {
			continue
		}
		dim := s.resolveDim(ctx, kind, teamID, dimHashFromKey(k))
		out[dim] = append(out[dim], h)
	}
	return out, nil
}

// scanKeys enumerates every sketch key for (kind, teamID) whose bucket
// timestamp overlaps [startMs, endMs].
func (s *RedisStore) scanKeys(ctx context.Context, kind Kind, teamID string, startMs, endMs int64) ([]string, error) {
	prefix := TenantPrefix(kind, teamID)
	startSec := (startMs / 1000) - int64(kind.Bucket.Seconds())
	endSec := endMs / 1000
	var keys []string
	var cursor uint64
	for {
		batch, next, err := s.c.Scan(ctx, cursor, prefix+"*", 500).Result()
		if err != nil {
			return nil, fmt.Errorf("sketch scan: %w", err)
		}
		for _, k := range batch {
			ts := unixFromKey(k)
			if ts < startSec || ts > endSec {
				continue
			}
			keys = append(keys, k)
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return keys, nil
}

func (s *RedisStore) mget(ctx context.Context, keys []string) ([]any, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	return s.c.MGet(ctx, keys...).Result()
}

// dimHashFromKey extracts the <dimHash> segment from
// "optikk:sk:<kindID>:<teamID>:<dimHash>:<unix>".
func dimHashFromKey(k string) string {
	// Expected segments: 0=optikk, 1=sk, 2=kindID, 3=teamID, 4=dimHash, 5=unix.
	parts := strings.SplitN(k, ":", 6)
	if len(parts) < 5 {
		return ""
	}
	return parts[4]
}

func unixFromKey(k string) int64 {
	idx := strings.LastIndexByte(k, ':')
	if idx < 0 {
		return 0
	}
	ts, _ := strconv.ParseInt(k[idx+1:], 10, 64)
	return ts
}

func (s *RedisStore) resolveDim(ctx context.Context, kind Kind, teamID, dimHash string) string {
	if dimHash == "" {
		return ""
	}
	label, err := s.c.Get(ctx, DimLabelKey(kind, teamID, dimHash)).Result()
	if err != nil {
		return dimHash
	}
	return label
}

