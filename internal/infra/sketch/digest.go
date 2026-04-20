package sketch

import (
	"fmt"

	"github.com/DataDog/sketches-go/ddsketch"
	"github.com/DataDog/sketches-go/ddsketch/mapping"
	"github.com/DataDog/sketches-go/ddsketch/store"
)

// relativeAccuracy is DDSketch's p95/p99 error bound. 0.01 = 1%. Matches
// Datadog's default; do not change without understanding the wire-format
// implications (index-mapping is tied to this value).
const relativeAccuracy = 0.01

// Digest is the canonical distribution sketch — a direct alias to
// github.com/DataDog/sketches-go/ddsketch. The public surface of this package
// is the library's public surface; no custom merge / compression / bucketing
// logic lives in optikk-backend.
type Digest = ddsketch.DDSketch

// NewDigest returns an empty sketch configured with the package's default
// relative accuracy. Panics only if the mapping constructor rejects the
// accuracy (a fixed constant, so in practice never).
func NewDigest() *Digest {
	d, err := ddsketch.NewDefaultDDSketch(relativeAccuracy)
	if err != nil {
		panic(fmt.Sprintf("ddsketch.NewDefaultDDSketch: %v", err))
	}
	return d
}

// MarshalDigest serializes a sketch to bytes for Redis storage. Uses the
// library's compact binary encoding (smaller than proto-marshal) and omits
// the index-mapping because every sketch in this package uses the fixed
// relativeAccuracy above.
func MarshalDigest(d *Digest) []byte {
	if d == nil {
		return nil
	}
	var b []byte
	d.Encode(&b, true) // omitIndexMapping=true
	return b
}

// UnmarshalDigest reconstructs a sketch from MarshalDigest bytes. The
// index-mapping is supplied externally because MarshalDigest omits it.
func UnmarshalDigest(b []byte) (*Digest, error) {
	if len(b) == 0 {
		return nil, fmt.Errorf("sketch: empty digest bytes")
	}
	m, err := mapping.NewLogarithmicMapping(relativeAccuracy)
	if err != nil {
		return nil, fmt.Errorf("sketch: build mapping: %w", err)
	}
	return ddsketch.DecodeDDSketch(b, store.DefaultProvider, m)
}
