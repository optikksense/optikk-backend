package grpc

import (
	"encoding/json"
	"testing"
	"time"

	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricsdatapb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	trace "go.opentelemetry.io/proto/otlp/trace/v1"
)

func TestMapTraceRowsEmitsResourceRowPerResourceBlock(t *testing.T) {
	req := &tracepb.ExportTraceServiceRequest{
		ResourceSpans: []*trace.ResourceSpans{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						protoStringKV("service.name", "checkout"),
						protoStringKV("deployment.environment", "prod"),
						protoStringKV("host.name", "node-a"),
						protoStringKV("host.type", "n2-standard"),
						protoStringKV("k8s.namespace.name", "payments"),
						protoStringKV("k8s.pod.name", "checkout-0"),
						protoStringKV("k8s.deployment.name", "checkout"),
						protoStringKV("k8s.cluster.name", "cluster-a"),
						protoStringKV("cloud.provider", "gcp"),
						protoStringKV("cloud.region", "us-central1"),
						protoStringKV("telemetry.sdk.language", "go"),
						protoStringKV("telemetry.sdk.version", "1.34.0"),
					},
				},
				ScopeSpans: []*trace.ScopeSpans{
					{
						Spans: []*trace.Span{
							{
								TraceId:           []byte("trace-1-trace-1-"),
								SpanId:            []byte("span-001"),
								Name:              "GET /checkout",
								Kind:              trace.Span_SPAN_KIND_SERVER,
								StartTimeUnixNano: 1710000300000000000,
								EndTimeUnixNano:   1710000305000000000,
								Status:            &trace.Status{Code: trace.Status_STATUS_CODE_UNSET},
							},
							{
								TraceId:           []byte("trace-1-trace-1-"),
								SpanId:            []byte("span-002"),
								ParentSpanId:      []byte("span-001"),
								Name:              "db call",
								Kind:              trace.Span_SPAN_KIND_CLIENT,
								StartTimeUnixNano: 1710000600000000000,
								EndTimeUnixNano:   1710000603000000000,
								Status:            &trace.Status{Code: trace.Status_STATUS_CODE_UNSET},
							},
						},
					},
				},
			},
		},
	}

	rows := MapTraceRows("team-1", req)
	if got := len(rows.Spans); got != 2 {
		t.Fatalf("expected 2 span rows, got %d", got)
	}
	if got := len(rows.Resources); got != 1 {
		t.Fatalf("expected 1 resource row, got %d", got)
	}

	resource := rows.Resources[0].Values
	if got := resource[3]; got != "checkout" {
		t.Fatalf("expected service name checkout, got %v", got)
	}
	if got := resource[4]; got != "prod" {
		t.Fatalf("expected environment prod, got %v", got)
	}
	if got := resource[5]; got != "node-a" {
		t.Fatalf("expected host node-a, got %v", got)
	}
	if got := resource[13]; got != "go" {
		t.Fatalf("expected sdk language go, got %v", got)
	}
	if got := resource[14]; got != "1.34.0" {
		t.Fatalf("expected sdk version 1.34.0, got %v", got)
	}

	wantBucket := time.Unix(0, 1710000300000000000).Unix() / 300 * 300
	if got := resource[1]; got != wantBucket {
		t.Fatalf("expected seen_at_ts_bucket_start %d, got %v", wantBucket, got)
	}

	var labels map[string]string
	if err := json.Unmarshal([]byte(resource[15].(string)), &labels); err != nil {
		t.Fatalf("unmarshal labels: %v", err)
	}
	if labels["service.name"] != "checkout" || labels["cloud.region"] != "us-central1" {
		t.Fatalf("unexpected labels payload: %#v", labels)
	}
}

func TestMapTraceRowsSkipsEmptyResourceAttributes(t *testing.T) {
	req := &tracepb.ExportTraceServiceRequest{
		ResourceSpans: []*trace.ResourceSpans{
			{
				ScopeSpans: []*trace.ScopeSpans{
					{
						Spans: []*trace.Span{
							{
								TraceId:           []byte("trace-1-trace-1-"),
								SpanId:            []byte("span-001"),
								Name:              "GET /checkout",
								Kind:              trace.Span_SPAN_KIND_SERVER,
								StartTimeUnixNano: 1710000300000000000,
								EndTimeUnixNano:   1710000305000000000,
								Status:            &trace.Status{Code: trace.Status_STATUS_CODE_UNSET},
							},
						},
					},
				},
			},
		},
	}

	rows := MapTraceRows("team-1", req)
	if got := len(rows.Spans); got != 1 {
		t.Fatalf("expected 1 span row, got %d", got)
	}
	if got := len(rows.Resources); got != 0 {
		t.Fatalf("expected 0 resource rows, got %d", got)
	}
}

func TestMapMetrics(t *testing.T) {
	req := &metricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricsdatapb.ResourceMetrics{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						protoStringKV("service.name", "payment-service"),
					},
				},
				ScopeMetrics: []*metricsdatapb.ScopeMetrics{
					{
						Metrics: []*metricsdatapb.Metric{
							{
								Name: "process.cpu.usage",
								Unit: "1",
								Data: &metricsdatapb.Metric_Gauge{
									Gauge: &metricsdatapb.Gauge{
										DataPoints: []*metricsdatapb.NumberDataPoint{
											{
												TimeUnixNano: 1710000000000000000,
												Value: &metricsdatapb.NumberDataPoint_AsDouble{
													AsDouble: 0.72,
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	rows := MapMetrics("team123", req)
	if len(rows) == 0 {
		t.Fatal("expected more than 0 rows")
	}
}

func protoStringKV(key, value string) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key: key,
		Value: &commonpb.AnyValue{
			Value: &commonpb.AnyValue_StringValue{StringValue: value},
		},
	}
}
