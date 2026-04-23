package querycompiler

import (
	"fmt"
	"strconv"
	"strings"
)

type StructuredFilter struct {
	Field string `json:"field"`
	Op    string `json:"op"`
	Value string `json:"value"`
}

func FromStructured(entries []StructuredFilter, teamID, startMs, endMs int64) (Filters, error) {
	f := Filters{TeamID: teamID, StartMs: startMs, EndMs: endMs}
	for _, e := range entries {
		if err := applyEntry(e, &f); err != nil {
			return Filters{}, err
		}
	}
	return f, nil
}

func applyEntry(e StructuredFilter, f *Filters) error {
	field := strings.ToLower(strings.TrimSpace(e.Field))
	op := strings.ToLower(strings.TrimSpace(e.Op))
	if op == "" {
		op = "eq"
	}
	if strings.HasPrefix(field, "@") {
		f.Attributes = append(f.Attributes, AttrFilter{Op: op, Key: field[1:], Value: e.Value})
		return nil
	}
	return applyScalar(field, op, e.Value, f)
}

//nolint:gocyclo // flat switch
func applyScalar(field, op, value string, f *Filters) error {
	isExclude := op == "neq" || op == "not"
	switch field {
	case "service", "service_name", "root_service":
		assignList(&f.Services, &f.ExcludeServices, isExclude, value)
	case "operation", "operation_name", "root_operation", "name":
		f.Operations = append(f.Operations, value)
	case "span_kind", "kind":
		f.SpanKinds = append(f.SpanKinds, value)
	case "http_method", "http.method":
		f.HTTPMethods = append(f.HTTPMethods, strings.ToUpper(value))
	case "http_status", "http_status_code", "http.status":
		f.HTTPStatuses = append(f.HTTPStatuses, value)
	case "status", "status_code", "root_status":
		assignList(&f.Statuses, &f.ExcludeStatuses, isExclude, value)
	case "environment", "env":
		f.Environments = append(f.Environments, value)
	case "peer_service", "peer.service":
		f.PeerServices = append(f.PeerServices, value)
	case "trace_id":
		f.TraceID = value
	case "min_duration_ms":
		if n, err := strconv.ParseInt(value, 10, 64); err == nil {
			f.MinDurationNs = n * 1_000_000
		}
	case "max_duration_ms":
		if n, err := strconv.ParseInt(value, 10, 64); err == nil {
			f.MaxDurationNs = n * 1_000_000
		}
	case "has_error", "errors_only":
		b := value == "true" || value == "1"
		f.HasError = &b
	case "search", "query":
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
