package apm

import "github.com/Optikk-Org/optikk-backend/internal/shared/displaybucket"

type HistogramSummary struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
	Avg float64 `json:"avg"`
}

type ProcessMemory struct {
	RSS float64 `json:"rss"`
	VMS float64 `json:"vms"`
}

type (
	TimeBucket  = displaybucket.TimeBucket
	StateBucket = displaybucket.StateBucket
)
