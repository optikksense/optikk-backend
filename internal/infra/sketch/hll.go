package sketch

import (
	"github.com/axiomhq/hyperloglog"
)

// HLL is a mergeable cardinality sketch (HyperLogLog++) — same algorithm class
// ClickHouse uses for uniq(). ≤1% error on cardinality estimates, ~2 KB
// serialized.
type HLL struct {
	sk *hyperloglog.Sketch
}

// NewHLL returns an empty cardinality sketch at the library's default
// precision (14-bit registers).
func NewHLL() *HLL {
	return &HLL{sk: hyperloglog.New14()}
}

// Add records one occurrence of the given element. Repeated calls with the
// same bytes do not increase the cardinality estimate.
func (h *HLL) Add(element []byte) {
	if len(element) == 0 {
		return
	}
	h.sk.Insert(element)
}

// AddString is a convenience wrapper for Add([]byte(s)).
func (h *HLL) AddString(s string) {
	if s == "" {
		return
	}
	h.sk.Insert([]byte(s))
}

// Merge folds another HLL into h. Associative and commutative.
func (h *HLL) Merge(other *HLL) error {
	if other == nil || other.sk == nil {
		return nil
	}
	return h.sk.Merge(other.sk)
}

// Estimate returns the approximate distinct-element count.
func (h *HLL) Estimate() uint64 {
	if h == nil || h.sk == nil {
		return 0
	}
	return h.sk.Estimate()
}

// MarshalBinary serializes for Redis storage.
func (h *HLL) MarshalBinary() ([]byte, error) {
	return h.sk.MarshalBinary()
}

// UnmarshalHLL reconstructs from bytes produced by MarshalBinary.
func UnmarshalHLL(b []byte) (*HLL, error) {
	sk := hyperloglog.New14()
	if err := sk.UnmarshalBinary(b); err != nil {
		return nil, err
	}
	return &HLL{sk: sk}, nil
}
