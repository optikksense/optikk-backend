// Package analytics holds reusable analytics-grid helpers (aggregation spec
// builders + dynamic CH row scan / map). Used by log_analytics today and
// available to log_facets / log_trends if either grows rollup-aware aggs.
package analytics

import (
	"fmt"

	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/shared/models"
)

// Spec is one projected aggregation: alias = expr.
type Spec struct {
	Alias string
	Expr  string
}

// BuildAggsRollupVolume constructs Spec for aggs pushed onto logs_volume.
func BuildAggsRollupVolume(in []models.Aggregation) ([]Spec, bool) {
	if len(in) == 0 {
		return []Spec{{Alias: "count", Expr: "sum(log_count)"}}, true
	}
	out := make([]Spec, 0, len(in))
	for _, a := range in {
		alias := a.Alias
		if alias == "" {
			alias = a.Fn
		}
		switch a.Fn {
		case "count":
			out = append(out, Spec{Alias: alias, Expr: "sum(log_count)"})
		case "errors", "error_count":
			out = append(out, Spec{Alias: alias, Expr: "countIf(has_error OR toUInt16OrZero(response_status_code) >= 400)"})
		default:
			return nil, false
		}
	}
	return out, true
}

// BuildAggsRollupFacets constructs Spec for aggs pushed onto logs_facets.
func BuildAggsRollupFacets(in []models.Aggregation) ([]Spec, bool) {
	if len(in) == 0 {
		return []Spec{{Alias: "count", Expr: "sum(log_count)"}}, true
	}
	out := make([]Spec, 0, len(in))
	for _, a := range in {
		alias := a.Alias
		if alias == "" {
			alias = a.Fn
		}
		switch a.Fn {
		case "count":
			out = append(out, Spec{Alias: alias, Expr: "sum(log_count)"})
		case "uniq", "count_distinct":
			if a.Field != "trace_id" {
				return nil, false
			}
			out = append(out, Spec{Alias: alias, Expr: "uniqHLL12Merge(trace_id_hll)"})
		default:
			return nil, false
		}
	}
	return out, true
}

// BuildAggsRaw constructs Spec for raw logs reads (full fidelity).
func BuildAggsRaw(in []models.Aggregation) []Spec {
	if len(in) == 0 {
		return []Spec{{Alias: "count", Expr: "count()"}}
	}
	out := make([]Spec, 0, len(in))
	for _, a := range in {
		alias := a.Alias
		if alias == "" {
			alias = a.Fn
		}
		switch a.Fn {
		case "count":
			out = append(out, Spec{Alias: alias, Expr: "count()"})
		case "errors", "error_count":
			out = append(out, Spec{Alias: alias, Expr: "countIf(severity_bucket >= 4)"})
		case "uniq", "count_distinct":
			if a.Field == "" {
				continue
			}
			out = append(out, Spec{Alias: alias, Expr: fmt.Sprintf("uniq(%s)", safeField(a.Field))})
		case "min":
			if a.Field == "" {
				continue
			}
			out = append(out, Spec{Alias: alias, Expr: fmt.Sprintf("min(%s)", safeField(a.Field))})
		case "max":
			if a.Field == "" {
				continue
			}
			out = append(out, Spec{Alias: alias, Expr: fmt.Sprintf("max(%s)", safeField(a.Field))})
		case "sum":
			if a.Field == "" {
				continue
			}
			out = append(out, Spec{Alias: alias, Expr: fmt.Sprintf("sum(%s)", safeField(a.Field))})
		case "avg":
			if a.Field == "" {
				continue
			}
			out = append(out, Spec{Alias: alias, Expr: fmt.Sprintf("avg(%s)", safeField(a.Field))})
		}
	}
	return out
}

var allowedRawFields = map[string]bool{
	"severity_bucket":    true,
	"severity_number":    true,
	"service":            true,
	"host":               true,
	"pod":                true,
	"container":          true,
	"environment":        true,
	"trace_id":           true,
	"span_id":            true,
	"observed_timestamp": true,
}

func safeField(f string) string {
	if allowedRawFields[f] {
		return f
	}
	return "1"
}

// AliasSet returns the set of alias names so MapRow puts them in Values.
func AliasSet(aggs []Spec) map[string]bool {
	out := make(map[string]bool, len(aggs))
	for _, a := range aggs {
		out[a.Alias] = true
	}
	return out
}
