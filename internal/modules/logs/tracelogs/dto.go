package tracelogs

type TraceMetaRowDTO struct {
	TraceStart    string `ch:"trace_start"`
	TraceEnd      string `ch:"trace_end"`
	ServiceName   string `ch:"service_name"`
	HTTPMethod    string `ch:"http_method"`
	HTTPURL       string `ch:"http_url"`
	OperationName string `ch:"operation_name"`
}
