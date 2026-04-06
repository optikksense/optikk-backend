package livetailredis

import "fmt"

// StreamPayloadField is the Redis Stream entry field name holding JSON (shared.Log + emit_ms).
const StreamPayloadField = "data"

// MaxStreamLen is approximate MAXLEN for XADD (~ trims old entries).
const MaxStreamLen = 2000

// SnapshotCount is how many recent stream entries are replayed on WebSocket subscribe (XREVRANGE).
const SnapshotCount = 20

// LogsStreamKey is the Redis Stream per team for logs live tail (XADD / XREAD).
func LogsStreamKey(teamID int64) string {
	return fmt.Sprintf("livetail:logs:stream:%d", teamID)
}

// SpansStreamKey is the Redis Stream per team for spans live tail (XADD / XREAD).
func SpansStreamKey(teamID int64) string {
	return fmt.Sprintf("livetail:spans:stream:%d", teamID)
}
