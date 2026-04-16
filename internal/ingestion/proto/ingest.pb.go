package proto

import (
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/runtime/protoimpl"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var File_ingest_proto protoreflect.FileDescriptor

type LogRow struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	TeamID              uint32                 `protobuf:"varint,1,opt,name=team_id,json=teamId,proto3" json:"team_id,omitempty"`
	TsBucketStart       uint32                 `protobuf:"varint,2,opt,name=ts_bucket_start,json=tsBucketStart,proto3" json:"ts_bucket_start,omitempty"`
	Timestamp           *timestamppb.Timestamp `protobuf:"bytes,3,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	ObservedTimestamp   uint64                 `protobuf:"varint,4,opt,name=observed_timestamp,json=observedTimestamp,proto3" json:"observed_timestamp,omitempty"`
	TraceID             string                 `protobuf:"bytes,5,opt,name=trace_id,json=traceId,proto3" json:"trace_id,omitempty"`
	SpanID              string                 `protobuf:"bytes,6,opt,name=span_id,json=spanId,proto3" json:"span_id,omitempty"`
	TraceFlags          uint32                 `protobuf:"varint,7,opt,name=trace_flags,json=traceFlags,proto3" json:"trace_flags,omitempty"`
	SeverityText        string                 `protobuf:"bytes,8,opt,name=severity_text,json=severityText,proto3" json:"severity_text,omitempty"`
	SeverityNumber      uint32                 `protobuf:"varint,9,opt,name=severity_number,json=severityNumber,proto3" json:"severity_number,omitempty"`
	Body                string                 `protobuf:"bytes,10,opt,name=body,proto3" json:"body,omitempty"`
	AttributesString    map[string]string      `protobuf:"bytes,11,rep,name=attributes_string,json=attributesString,proto3" json:"attributes_string,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	AttributesNumber    map[string]float64     `protobuf:"bytes,12,rep,name=attributes_number,json=attributesNumber,proto3" json:"attributes_number,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"fixed64,2,opt,name=value,proto3"`
	AttributesBool      map[string]bool        `protobuf:"bytes,13,rep,name=attributes_bool,json=attributesBool,proto3" json:"attributes_bool,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"varint,2,opt,name=value,proto3"`
	Resource            map[string]string      `protobuf:"bytes,14,rep,name=resource,proto3" json:"resource,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	ResourceFingerprint string                 `protobuf:"bytes,15,opt,name=resource_fingerprint,json=resourceFingerprint,proto3" json:"resource_fingerprint,omitempty"`
	ScopeName           string                 `protobuf:"bytes,16,opt,name=scope_name,json=scopeName,proto3" json:"scope_name,omitempty"`
	ScopeVersion        string                 `protobuf:"bytes,17,opt,name=scope_version,json=scopeVersion,proto3" json:"scope_version,omitempty"`
	ScopeString         map[string]string      `protobuf:"bytes,18,rep,name=scope_string,json=scopeString,proto3" json:"scope_string,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
}

func (x *LogRow) ProtoReflect() protoreflect.Message { return nil } // simplified for this environment
func (x *LogRow) String() string                     { return "" }
func (*LogRow) ProtoMessage()                        {}

type SpanRow struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	TsBucketStart       uint64                 `protobuf:"varint,1,opt,name=ts_bucket_start,json=tsBucketStart,proto3" json:"ts_bucket_start,omitempty"`
	TeamID              uint32                 `protobuf:"varint,2,opt,name=team_id,json=teamId,proto3" json:"team_id,omitempty"`
	Timestamp           *timestamppb.Timestamp `protobuf:"bytes,3,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	TraceID             string                 `protobuf:"bytes,4,opt,name=trace_id,json=traceId,proto3" json:"trace_id,omitempty"`
	SpanID              string                 `protobuf:"bytes,5,opt,name=span_id,json=spanId,proto3" json:"span_id,omitempty"`
	ParentSpanID        string                 `protobuf:"bytes,6,opt,name=parent_span_id,json=parentSpanId,proto3" json:"parent_span_id,omitempty"`
	TraceState          string                 `protobuf:"bytes,7,opt,name=trace_state,json=traceState,proto3" json:"trace_state,omitempty"`
	Flags               uint32                 `protobuf:"varint,8,opt,name=flags,proto3" json:"flags,omitempty"`
	Name                string                 `protobuf:"bytes,9,opt,name=name,proto3" json:"name,omitempty"`
	Kind                int32                  `protobuf:"varint,10,opt,name=kind,proto3" json:"kind,omitempty"`
	KindString          string                 `protobuf:"bytes,11,opt,name=kind_string,json=kindString,proto3" json:"kind_string,omitempty"`
	DurationNano        uint64                 `protobuf:"varint,12,opt,name=duration_nano,json=durationNano,proto3" json:"duration_nano,omitempty"`
	HasError            bool                   `protobuf:"varint,13,opt,name=has_error,json=hasError,proto3" json:"has_error,omitempty"`
	IsRemote            bool                   `protobuf:"varint,14,opt,name=is_remote,json=isRemote,proto3" json:"is_remote,omitempty"`
	StatusCode          int32                  `protobuf:"varint,15,opt,name=status_code,json=statusCode,proto3" json:"status_code,omitempty"`
	StatusCodeString    string                 `protobuf:"bytes,16,opt,name=status_code_string,json=statusCodeString,proto3" json:"status_code_string,omitempty"`
	StatusMessage       string                 `protobuf:"bytes,17,opt,name=status_message,json=statusMessage,proto3" json:"status_message,omitempty"`
	HTTPUrl             string                 `protobuf:"bytes,18,opt,name=http_url,json=httpUrl,proto3" json:"http_url,omitempty"`
	HTTPMethod          string                 `protobuf:"bytes,19,opt,name=http_method,json=httpMethod,proto3" json:"http_method,omitempty"`
	HTTPHost            string                 `protobuf:"bytes,20,opt,name=http_host,json=httpHost,proto3" json:"http_host,omitempty"`
	ExternalHTTPUrl     string                 `protobuf:"bytes,21,opt,name=external_http_url,json=externalHttpUrl,proto3" json:"external_http_url,omitempty"`
	ExternalHTTPMethod  string                 `protobuf:"bytes,22,opt,name=external_http_method,json=externalHttpMethod,proto3" json:"external_http_method,omitempty"`
	ResponseStatusCode  string                 `protobuf:"bytes,23,opt,name=response_status_code,json=responseStatusCode,proto3" json:"response_status_code,omitempty"`
	Attributes          map[string]string      `protobuf:"bytes,24,rep,name=attributes,proto3" json:"attributes,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	Events              []string               `protobuf:"bytes,25,rep,name=events,proto3" json:"events,omitempty"`
	Links               string                 `protobuf:"bytes,26,opt,name=links,proto3" json:"links,omitempty"`
	ExceptionType       string                 `protobuf:"bytes,27,opt,name=exception_type,json=exceptionType,proto3" json:"exception_type,omitempty"`
	ExceptionMessage    string                 `protobuf:"bytes,28,opt,name=exception_message,json=exceptionMessage,proto3" json:"exception_message,omitempty"`
	ExceptionStacktrace string                 `protobuf:"bytes,29,opt,name=exception_stacktrace,json=exceptionStacktrace,proto3" json:"exception_stacktrace,omitempty"`
	ExceptionEscaped    bool                   `protobuf:"varint,30,opt,name=exception_escaped,json=exceptionEscaped,proto3" json:"exception_escaped,omitempty"`
}

func (x *SpanRow) ProtoReflect() protoreflect.Message { return nil }
func (x *SpanRow) String() string                     { return "" }
func (*SpanRow) ProtoMessage()                        {}

type MetricRow struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	TeamID              uint32                 `protobuf:"varint,1,opt,name=team_id,json=teamId,proto3" json:"team_id,omitempty"`
	Env                 string                 `protobuf:"bytes,2,opt,name=env,proto3" json:"env,omitempty"`
	MetricName          string                 `protobuf:"bytes,3,opt,name=metric_name,json=metricName,proto3" json:"metric_name,omitempty"`
	MetricType          string                 `protobuf:"bytes,4,opt,name=metric_type,json=metricType,proto3" json:"metric_type,omitempty"`
	Temporality         string                 `protobuf:"bytes,5,opt,name=temporality,proto3" json:"temporality,omitempty"`
	IsMonotonic         bool                   `protobuf:"varint,6,opt,name=is_monotonic,json=isMonotonic,proto3" json:"is_monotonic,omitempty"`
	Unit                string                 `protobuf:"bytes,7,opt,name=unit,proto3" json:"unit,omitempty"`
	Description         string                 `protobuf:"bytes,8,opt,name=description,proto3" json:"description,omitempty"`
	ResourceFingerprint uint64                 `protobuf:"varint,9,opt,name=resource_fingerprint,json=resourceFingerprint,proto3" json:"resource_fingerprint,omitempty"`
	Timestamp           *timestamppb.Timestamp `protobuf:"bytes,10,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	Value               float64                `protobuf:"fixed64,11,opt,name=value,proto3" json:"value,omitempty"`
	HistSum             float64                `protobuf:"fixed64,12,opt,name=hist_sum,json=histSum,proto3" json:"hist_sum,omitempty"`
	HistCount           uint64                 `protobuf:"varint,13,opt,name=hist_count,json=histCount,proto3" json:"hist_count,omitempty"`
	HistBuckets         []float64              `protobuf:"fixed64,14,rep,packed,name=hist_buckets,json=histBuckets,proto3" json:"hist_buckets,omitempty"`
	HistCounts          []uint64               `protobuf:"varint,15,rep,packed,name=hist_counts,json=histCounts,proto3" json:"hist_counts,omitempty"`
	Attributes          map[string]string      `protobuf:"bytes,16,rep,name=attributes,proto3" json:"attributes,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
}

func (x *MetricRow) ProtoReflect() protoreflect.Message { return nil }
func (x *MetricRow) String() string                     { return "" }
func (*MetricRow) ProtoMessage()                        {}

type TelemetryBatch struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	TeamID int64 `protobuf:"varint,1,opt,name=team_id,json=teamId,proto3" json:"team_id,omitempty"`
	// Types that are assignable to Data:
	//	*TelemetryBatch_Logs
	//	*TelemetryBatch_Spans
	//	*TelemetryBatch_Metrics
	Data isTelemetryBatch_Data `protobuf_oneof:"data"`
}

func (x *TelemetryBatch) ProtoReflect() protoreflect.Message { return nil }
func (x *TelemetryBatch) String() string                     { return "" }
func (*TelemetryBatch) ProtoMessage()                        {}

type isTelemetryBatch_Data interface {
	isTelemetryBatch_Data()
}

type TelemetryBatch_Logs struct {
	Logs *LogBatch `protobuf:"bytes,2,opt,name=logs,proto3,oneof"`
}
type TelemetryBatch_Spans struct {
	Spans *SpanBatch `protobuf:"bytes,3,opt,name=spans,proto3,oneof"`
}
type TelemetryBatch_Metrics struct {
	Metrics *MetricBatch `protobuf:"bytes,4,opt,name=metrics,proto3,oneof"`
}

func (*TelemetryBatch_Logs) isTelemetryBatch_Data()    {}
func (*TelemetryBatch_Spans) isTelemetryBatch_Data()   {}
func (*TelemetryBatch_Metrics) isTelemetryBatch_Data() {}

type LogBatch struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Rows []*LogRow `protobuf:"bytes,1,rep,name=rows,proto3" json:"rows,omitempty"`
}

func (x *LogBatch) ProtoReflect() protoreflect.Message { return nil }
func (x *LogBatch) String() string                     { return "" }
func (*LogBatch) ProtoMessage()                        {}

type SpanBatch struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Rows []*SpanRow `protobuf:"bytes,1,rep,name=rows,proto3" json:"rows,omitempty"`
}

func (x *SpanBatch) ProtoReflect() protoreflect.Message { return nil }
func (x *SpanBatch) String() string                     { return "" }
func (*SpanBatch) ProtoMessage()                        {}

type MetricBatch struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Rows []*MetricRow `protobuf:"bytes,1,rep,name=rows,proto3" json:"rows,omitempty"`
}

func (x *MetricBatch) ProtoReflect() protoreflect.Message { return nil }
func (x *MetricBatch) String() string                     { return "" }
func (*MetricBatch) ProtoMessage()                        {}
