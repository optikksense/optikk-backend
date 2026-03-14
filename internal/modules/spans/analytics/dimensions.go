package analytics

// allowedDimensions maps user-facing dimension names to ClickHouse column expressions.
// Only materialized columns are allowed to keep GROUP BY queries fast.
var allowedDimensions = map[string]Dimension{
	"service.name":           {Name: "service.name", Column: "s.service_name", Description: "Service name"},
	"operation":              {Name: "operation", Column: "s.name", Description: "Span / operation name"},
	"span.kind":              {Name: "span.kind", Column: "s.kind_string", Description: "Span kind (SERVER, CLIENT, etc.)"},
	"status":                 {Name: "status", Column: "s.status_code_string", Description: "Span status code"},
	"http.route":             {Name: "http.route", Column: "s.mat_http_route", Description: "HTTP route"},
	"http.status_code":       {Name: "http.status_code", Column: "s.mat_http_status_code", Description: "HTTP status code"},
	"http.method":            {Name: "http.method", Column: "s.http_method", Description: "HTTP method"},
	"http.target":            {Name: "http.target", Column: "s.mat_http_target", Description: "HTTP target"},
	"http.scheme":            {Name: "http.scheme", Column: "s.mat_http_scheme", Description: "HTTP scheme"},
	"db.system":              {Name: "db.system", Column: "s.mat_db_system", Description: "Database system"},
	"db.name":                {Name: "db.name", Column: "s.mat_db_name", Description: "Database name"},
	"db.operation":           {Name: "db.operation", Column: "s.mat_db_operation", Description: "Database operation"},
	"rpc.system":             {Name: "rpc.system", Column: "s.mat_rpc_system", Description: "RPC system"},
	"rpc.service":            {Name: "rpc.service", Column: "s.mat_rpc_service", Description: "RPC service"},
	"rpc.method":             {Name: "rpc.method", Column: "s.mat_rpc_method", Description: "RPC method"},
	"rpc.grpc.status_code":   {Name: "rpc.grpc.status_code", Column: "s.mat_rpc_grpc_status_code", Description: "gRPC status code"},
	"messaging.system":       {Name: "messaging.system", Column: "s.mat_messaging_system", Description: "Messaging system"},
	"messaging.operation":    {Name: "messaging.operation", Column: "s.mat_messaging_operation", Description: "Messaging operation"},
	"messaging.destination":  {Name: "messaging.destination", Column: "s.mat_messaging_destination", Description: "Messaging destination"},
	"peer.service":           {Name: "peer.service", Column: "s.mat_peer_service", Description: "Peer service"},
	"net.peer.name":          {Name: "net.peer.name", Column: "s.mat_net_peer_name", Description: "Network peer name"},
	"exception.type":         {Name: "exception.type", Column: "s.mat_exception_type", Description: "Exception type"},
	"host.name":              {Name: "host.name", Column: "s.mat_host_name", Description: "Host name"},
	"k8s.pod.name":           {Name: "k8s.pod.name", Column: "s.mat_k8s_pod_name", Description: "Kubernetes pod name"},
	"response_status_code":   {Name: "response_status_code", Column: "s.response_status_code", Description: "Response status code"},
}

// AllDimensions returns the list of available dimensions for the API.
func AllDimensions() []Dimension {
	dims := make([]Dimension, 0, len(allowedDimensions))
	for _, d := range allowedDimensions {
		dims = append(dims, d)
	}
	return dims
}

// LookupDimension returns the column expression for a dimension name, or empty if invalid.
func LookupDimension(name string) (string, bool) {
	d, ok := allowedDimensions[name]
	if !ok {
		return "", false
	}
	return d.Column, true
}
