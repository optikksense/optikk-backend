package network

// ---------------------------------------------------------------------------
// HTTP response DTOs (API contract).
// ---------------------------------------------------------------------------

type MetricValue struct {
	Value float64 `json:"value"`
}

// ---------------------------------------------------------------------------
// Internal repository row types — raw rows out of CH.
// ---------------------------------------------------------------------------

type NetworkServiceRow struct {
	Service string  `ch:"service"`
	Value   float64 `ch:"value"`
}

type NetworkScalarRow struct {
	Value float64 `ch:"value"`
}
