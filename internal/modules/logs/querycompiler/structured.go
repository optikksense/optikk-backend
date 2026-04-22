package querycompiler

import (
	"fmt"
	"strings"
)

// StructuredFilter is the v1 wire format. Clients send arrays of
// {field, op, value} entries. Unknown fields flow through as attribute
// filters (field starts with `@`). All non-attribute ops collapse to IN /
// NOT IN semantics; range ops live under `Search` + `@attr` variants.
type StructuredFilter struct {
	Field string `json:"field"`
	Op    string `json:"op"`
	Value string `json:"value"`
}

// FromStructured converts the wire filters to the compiler's internal
// Filters shape. The caller already has time range + teamID injected.
func FromStructured(entries []StructuredFilter, teamID int64, startMs, endMs int64) (Filters, error) {
	f := Filters{TeamID: teamID, StartMs: startMs, EndMs: endMs}
	for _, entry := range entries {
		if err := applyEntry(entry, &f); err != nil {
			return Filters{}, err
		}
	}
	return f, nil
}

func applyEntry(entry StructuredFilter, f *Filters) error {
	field := strings.ToLower(strings.TrimSpace(entry.Field))
	value := entry.Value
	op := strings.ToLower(strings.TrimSpace(entry.Op))
	if op == "" {
		op = "eq"
	}
	if strings.HasPrefix(field, "@") {
		f.Attributes = append(f.Attributes, AttrFilter{Op: op, Key: field[1:], Value: value})
		return nil
	}
	return applyScalar(field, op, value, f)
}

//nolint:gocyclo // flat switch on a finite field set; splitting hurts readability
func applyScalar(field, op, value string, f *Filters) error {
	isExclude := op == "neq" || op == "not"
	switch field {
	case "service", "service_name":
		assignList(&f.Services, &f.ExcludeServices, isExclude, value)
	case "level", "severity", "status", "severity_text":
		assignList(&f.Severities, &f.ExcludeSeverities, isExclude, value)
	case "host":
		assignList(&f.Hosts, &f.ExcludeHosts, isExclude, value)
	case "pod":
		f.Pods = append(f.Pods, value)
	case "container":
		f.Containers = append(f.Containers, value)
	case "environment", "env":
		f.Environments = append(f.Environments, value)
	case "trace_id":
		f.TraceID = value
	case "span_id":
		f.SpanID = value
	case "body", "message", "search":
		f.Search = value
		if op == "exact" {
			f.SearchMode = "exact"
		}
	default:
		return fmt.Errorf("querycompiler: unknown field %q", field)
	}
	return nil
}

func assignList(incl, excl *[]string, isExclude bool, value string) {
	if isExclude {
		*excl = append(*excl, value)
		return
	}
	*incl = append(*incl, value)
}
