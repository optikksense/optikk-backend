package queryparser

import "strings"

// TracesSchema resolves field names for the ClickHouse spans table.
type TracesSchema struct{}

var tracesFieldMap = map[string]FieldInfo{
	"service":        {Column: "s.service_name", Type: FieldString},
	"service_name":   {Column: "s.service_name", Type: FieldString},
	"operation":      {Column: "s.name", Type: FieldString},
	"operation_name": {Column: "s.name", Type: FieldString},
	"name":           {Column: "s.name", Type: FieldString},
	"status":         {Column: "s.status_code_string", Type: FieldString},
	"span.kind":      {Column: "s.kind_string", Type: FieldString},
	"kind":           {Column: "s.kind_string", Type: FieldString},
	"duration":       {Column: "s.duration_nano", Type: FieldNumber},
	"trace_id":       {Column: "s.trace_id", Type: FieldString},
	"span_id":        {Column: "s.span_id", Type: FieldString},

	// HTTP
	"http.method":      {Column: "s.http_method", Type: FieldString},
	"http.url":         {Column: "s.http_url", Type: FieldString},
	"http.host":        {Column: "s.http_host", Type: FieldString},
	"http.status_code": {Column: "s.response_status_code", Type: FieldString},
	"http.route":       {Column: "s.mat_http_route", Type: FieldString},
	"http.target":      {Column: "s.mat_http_target", Type: FieldString},
	"http.scheme":      {Column: "s.mat_http_scheme", Type: FieldString},

	// Database
	"db.system":    {Column: "s.mat_db_system", Type: FieldString},
	"db.name":      {Column: "s.mat_db_name", Type: FieldString},
	"db.operation": {Column: "s.mat_db_operation", Type: FieldString},
	"db.statement": {Column: "s.mat_db_statement", Type: FieldString},

	// RPC
	"rpc.system":           {Column: "s.mat_rpc_system", Type: FieldString},
	"rpc.service":          {Column: "s.mat_rpc_service", Type: FieldString},
	"rpc.method":           {Column: "s.mat_rpc_method", Type: FieldString},
	"rpc.grpc.status_code": {Column: "s.mat_rpc_grpc_status_code", Type: FieldString},

	// Messaging
	"messaging.system":      {Column: "s.mat_messaging_system", Type: FieldString},
	"messaging.operation":   {Column: "s.mat_messaging_operation", Type: FieldString},
	"messaging.destination": {Column: "s.mat_messaging_destination", Type: FieldString},

	// Infrastructure
	"host.name":      {Column: "s.mat_host_name", Type: FieldString},
	"k8s.pod.name":   {Column: "s.mat_k8s_pod_name", Type: FieldString},
	"net.peer.name":  {Column: "s.mat_net_peer_name", Type: FieldString},
	"peer.service":   {Column: "s.mat_peer_service", Type: FieldString},
	"exception.type": {Column: "s.mat_exception_type", Type: FieldString},

	// Error
	"has_error":      {Column: "s.has_error", Type: FieldBool},
	"status_message": {Column: "s.status_message", Type: FieldString},
}

func (TracesSchema) Resolve(field string) (FieldInfo, bool) {
	lower := strings.ToLower(field)

	if info, ok := tracesFieldMap[lower]; ok {
		return info, true
	}

	// Custom attributes: @key maps to JSON attributes column.
	if strings.HasPrefix(field, "@") {
		attrKey := field[1:]
		return FieldInfo{
			Column: "JSONExtractString(s.attributes, '" + escapeSingleQuote(attrKey) + "')",
			Type:   FieldString,
		}, true
	}

	return FieldInfo{}, false
}

func (TracesSchema) FreeTextColumns() []string {
	return []string{
		"s.trace_id",
		"s.service_name",
		"s.name",
		"s.status_message",
	}
}

func (TracesSchema) TableAlias() string { return "s" }
