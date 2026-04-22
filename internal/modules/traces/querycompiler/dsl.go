package querycompiler

import "errors"

// FromDSL is the v1 stub for the future Datadog-style query DSL. Wired here
// so the parser seam is stable; implementation lands later.
func FromDSL(_ string, _ int64, _ int64, _ int64) (Filters, error) {
	return Filters{}, errors.New("querycompiler: DSL not implemented")
}
