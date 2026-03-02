package telemetry

import (
	"encoding/hex"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

func TestTranslateProtoSpans(t *testing.T) {
	teamUUID := "test-team-uuid"
	traceID, _ := hex.DecodeString("4bf92f3577b34da6a3ce929d0e0e4736")
	spanID, _ := hex.DecodeString("00f067aa0ba902b7")

	req := &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "test-service"}}},
						{Key: "host.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "test-host"}}},
					},
				},
				ScopeSpans: []*tracepb.ScopeSpans{
					{
						Spans: []*tracepb.Span{
							{
								TraceId:           traceID,
								SpanId:            spanID,
								Name:              "test-operation",
								Kind:              tracepb.Span_SPAN_KIND_SERVER,
								StartTimeUnixNano: 1709000000000000000,
								EndTimeUnixNano:   1709000001000000000,
								Attributes: []*commonpb.KeyValue{
									{Key: "http.method", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "GET"}}},
									{Key: "http.status_code", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 200}}},
								},
								Status: &tracepb.Status{Code: tracepb.Status_STATUS_CODE_OK},
							},
						},
					},
				},
			},
		},
	}

	spans := TranslateProtoSpans(teamUUID, req, nil)

	assert.Len(t, spans, 1)
	s := spans[0]
	assert.Equal(t, teamUUID, s.TeamUUID)
	assert.Equal(t, "4bf92f3577b34da6a3ce929d0e0e4736", s.TraceID)
	assert.Equal(t, "00f067aa0ba902b7", s.SpanID)
	assert.Equal(t, "test-operation", s.OperationName)
	assert.Equal(t, "test-service", s.ServiceName)
	assert.Equal(t, "SERVER", s.SpanKind)
	assert.Equal(t, int64(1000), s.DurationMs)
	assert.Equal(t, "OK", s.Status)
	assert.Equal(t, "GET", s.HTTPMethod)
	assert.Equal(t, 200, s.HTTPStatusCode)
	assert.Equal(t, "test-host", s.Host)
}

func TestHexEncode(t *testing.T) {
	tests := []struct {
		input  []byte
		expect string
	}{
		{[]byte{0x01, 0x02}, "0102"},
		{nil, ""},
		{[]byte{0, 0, 0}, ""}, // Should return empty for all-zero IDs
		{[]byte{0, 1, 0}, "000100"},
	}

	for _, tc := range tests {
		assert.Equal(t, tc.expect, hexEncode(tc.input))
	}
}

func TestUnixNanoToTime(t *testing.T) {
	ns := uint64(1709000000000000000)
	tm := unixNanoToTime(ns)
	assert.Equal(t, int64(1709000000), tm.Unix())

	now := time.Now()
	assert.WithinDuration(t, now, unixNanoToTime(0), time.Second)
}

func TestFirstNonEmpty(t *testing.T) {
	assert.Equal(t, "a", firstNonEmpty("", "  ", "a", "b"))
	assert.Equal(t, "b", firstNonEmpty(" ", "b"))
	assert.Equal(t, "", firstNonEmpty("", " "))
}
