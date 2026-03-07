package otlp

import (
	"encoding/json"
	"testing"
	"time"
)

func TestMapTraceRowsEmitsResourceRowPerResourceBlock(t *testing.T) {
	req := ExportTraceServiceRequest{
		ResourceSpans: []ResourceSpans{
			{
				Resource: Resource{Attributes: []KeyValue{
					stringKV("service.name", "checkout"),
					stringKV("deployment.environment", "prod"),
					stringKV("host.name", "node-a"),
					stringKV("host.type", "n2-standard"),
					stringKV("k8s.namespace.name", "payments"),
					stringKV("k8s.pod.name", "checkout-0"),
					stringKV("k8s.deployment.name", "checkout"),
					stringKV("k8s.cluster.name", "cluster-a"),
					stringKV("cloud.provider", "gcp"),
					stringKV("cloud.region", "us-central1"),
					stringKV("telemetry.sdk.language", "go"),
					stringKV("telemetry.sdk.version", "1.34.0"),
				}},
				ScopeSpans: []ScopeSpans{
					{
						Spans: []OTLPSpan{
							{
								TraceID:           "trace-1",
								SpanID:            "span-1",
								Name:              "GET /checkout",
								Kind:              2,
								StartTimeUnixNano: "1710000300000000000",
								EndTimeUnixNano:   "1710000305000000000",
								Status:            SpanStatus{Code: 0},
							},
							{
								TraceID:           "trace-1",
								SpanID:            "span-2",
								ParentSpanID:      "span-1",
								Name:              "db call",
								Kind:              3,
								StartTimeUnixNano: "1710000600000000000",
								EndTimeUnixNano:   "1710000603000000000",
								Status:            SpanStatus{Code: 0},
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
	if got := resource[0]; got == "" {
		t.Fatal("expected resource fingerprint to be populated")
	}
	if got := resource[2]; got != "team-1" {
		t.Fatalf("expected team id team-1, got %v", got)
	}
	if got := resource[3]; got != "checkout" {
		t.Fatalf("expected service name checkout, got %v", got)
	}
	if got := resource[4]; got != "prod" {
		t.Fatalf("expected environment prod, got %v", got)
	}
	if got := resource[5]; got != "node-a" {
		t.Fatalf("expected host name node-a, got %v", got)
	}
	if got := resource[8]; got != "checkout-0" {
		t.Fatalf("expected pod name checkout-0, got %v", got)
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
	req := ExportTraceServiceRequest{
		ResourceSpans: []ResourceSpans{
			{
				Resource: Resource{},
				ScopeSpans: []ScopeSpans{
					{
						Spans: []OTLPSpan{
							{
								TraceID:           "trace-1",
								SpanID:            "span-1",
								Name:              "GET /checkout",
								Kind:              2,
								StartTimeUnixNano: "1710000300000000000",
								EndTimeUnixNano:   "1710000305000000000",
								Status:            SpanStatus{Code: 0},
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

func stringKV(key, value string) KeyValue {
	return KeyValue{
		Key: key,
		Value: AnyValue{
			StringValue: &value,
		},
	}
}
