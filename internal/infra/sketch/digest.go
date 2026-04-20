package sketch

import (
	"bytes"
	"fmt"

	tdigest "github.com/caio/go-tdigest/v4"
)

// Digest is a mergeable quantile sketch (t-digest) — the same algorithm
// ClickHouse uses for quantileTDigest. ≤1% error on p95/p99 for realistic
// distributions, ~1 KB serialized.
type Digest struct {
	td *tdigest.TDigest
}

// NewDigest returns an empty sketch with compression 100 — a good default
// accuracy/size tradeoff matching ClickHouse's internal compression.
func NewDigest() *Digest {
	td, err := tdigest.New(tdigest.Compression(100))
	if err != nil {
		// tdigest.New only errors for invalid options; compression=100 is fixed.
		panic(fmt.Sprintf("tdigest.New: %v", err))
	}
	return &Digest{td: td}
}

// Add records a single observation with the given weight (use weight=1 for
// unweighted samples).
func (d *Digest) Add(value float64, weight uint32) error {
	if weight == 0 {
		return nil
	}
	return d.td.AddWeighted(value, uint64(weight))
}

// Merge folds another digest into d. Associative and commutative — safe to
// call with sketches produced by different processes / time windows.
func (d *Digest) Merge(other *Digest) error {
	if other == nil || other.td == nil {
		return nil
	}
	return d.td.Merge(other.td)
}

// Quantile returns the approximate q-th quantile (q in [0,1]).
func (d *Digest) Quantile(q float64) float64 {
	if d == nil || d.td == nil {
		return 0
	}
	return d.td.Quantile(q)
}

// Quantiles returns approximate values for each q, in the same order.
func (d *Digest) Quantiles(qs ...float64) []float64 {
	out := make([]float64, len(qs))
	for i, q := range qs {
		out[i] = d.Quantile(q)
	}
	return out
}

// MarshalBinary serializes the digest for storage in Redis.
func (d *Digest) MarshalBinary() ([]byte, error) {
	return d.td.AsBytes()
}

// UnmarshalDigest reconstructs a digest from bytes produced by MarshalBinary.
func UnmarshalDigest(b []byte) (*Digest, error) {
	td, err := tdigest.FromBytes(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	return &Digest{td: td}, nil
}
