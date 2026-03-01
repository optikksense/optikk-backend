package telemetry

import "testing"

func TestTranslateSpansFallsBackToMicrometerHTTPAttributes(t *testing.T) {
	payload := OTLPTracesPayload{
		ResourceSpans: []OTLPResourceSpans{
			{
				Resource: OTLPResource{
					Attributes: []OTLPAttribute{
						stringAttr("service.name", "optic-spring-mongo-kafka"),
						stringAttr("host.name", "demo-host"),
					},
				},
				ScopeSpans: []OTLPScopeSpans{
					{
						Spans: []OTLPSpan{
							{
								TraceID:           "trace-1",
								SpanID:            "span-1",
								Name:              "http post /api/activities",
								Kind:              2,
								StartTimeUnixNano: "1000000000",
								EndTimeUnixNano:   "2000000000",
								Attributes: []OTLPAttribute{
									stringAttr("method", "POST"),
									stringAttr("uri", "/api/activities"),
									stringAttr("status", "400"),
								},
							},
						},
					},
				},
			},
		},
	}

	spans := TranslateSpans("team-1", payload)
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}

	span := spans[0]
	if span.HTTPMethod != "POST" {
		t.Fatalf("expected HTTP method POST, got %q", span.HTTPMethod)
	}
	if span.HTTPURL != "/api/activities" {
		t.Fatalf("expected HTTP url /api/activities, got %q", span.HTTPURL)
	}
	if span.HTTPStatusCode != 400 {
		t.Fatalf("expected HTTP status code 400, got %d", span.HTTPStatusCode)
	}
	if span.Status != "ERROR" {
		t.Fatalf("expected span status ERROR, got %q", span.Status)
	}
	if span.StatusMessage != "HTTP 400" {
		t.Fatalf("expected status message HTTP 400, got %q", span.StatusMessage)
	}
}

func TestParseHTTPStatusCodeSupportsMicrometerStatusKey(t *testing.T) {
	code := parseHTTPStatusCode(map[string]string{"status": "201"})
	if code != 201 {
		t.Fatalf("expected 201, got %d", code)
	}
}

func stringAttr(key, value string) OTLPAttribute {
	return OTLPAttribute{
		Key: key,
		Value: OTLPAnyValue{
			StringValue: &value,
		},
	}
}
