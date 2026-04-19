// Package cursor provides generic base64+JSON encoding for keyset pagination cursors.
package cursor

import (
	"encoding/base64"
	"encoding/json"
)

func Encode[T any](cur T) string {
	b, err := json.Marshal(cur)
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

func Decode[T any](raw string) (T, bool) {
	var zero T
	if raw == "" {
		return zero, false
	}
	b, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return zero, false
	}
	var cur T
	if err := json.Unmarshal(b, &cur); err != nil {
		return zero, false
	}
	return cur, true
}
