package querycompiler

import "errors"

// FromDSL parses a single-line filter expression (e.g.
// `service:api level:ERROR @http.route:/v1/users`) into Filters. v1 stub —
// DSL support lands in a later slice. The seam is wired so the frontend's
// future "raw query" pane can call this without downstream schema change.
func FromDSL(_ string) (Filters, error) {
	return Filters{}, errors.New("querycompiler: DSL parser not implemented")
}
