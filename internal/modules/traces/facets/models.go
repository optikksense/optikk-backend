package facets

// FacetBucket is one (value, count) pair in a facet column.
type FacetBucket struct {
	Value string `json:"value"`
	Count uint64 `json:"count"`
}

// Facets is the per-dim top-N projection returned by POST /traces/facets.
type Facets struct {
	Service    []FacetBucket `json:"service,omitempty"`
	Operation  []FacetBucket `json:"operation,omitempty"`
	HTTPMethod []FacetBucket `json:"http_method,omitempty"`
	HTTPStatus []FacetBucket `json:"http_status,omitempty"`
	Status     []FacetBucket `json:"status,omitempty"`
}
