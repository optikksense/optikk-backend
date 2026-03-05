package grpc

import (
"testing"
"google.golang.org/protobuf/encoding/protojson"
metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
)

func TestMapMetrics(t *testing.T) {
	j := `{
  "resourceMetrics": [
    {
      "resource": {
        "attributes": [
          { "key": "service.name",           "value": { "stringValue": "payment-service" } }
        ]
      },
      "scopeMetrics": [
        {
          "scope": { "name": "payment.instrumentation", "version": "1.0.0" },
          "metrics": [
            {
              "name": "process.cpu.usage",
              "unit": "1",
              "gauge": {
                "dataPoints": [
                  {
                    "timeUnixNano": "1741132860000000000",
                    "asDouble": 0.72
                  }
                ]
              }
            }
          ]
        }
      ]
    }
  ]
}`

	var req metricspb.ExportMetricsServiceRequest
	if err := protojson.Unmarshal([]byte(j), &req); err != nil {
		t.Fatal(err)
	}

	rows := MapMetrics("team123", &req)
	if len(rows) == 0 {
		t.Fatal("expected more than 0 rows")
	}
	t.Logf("Rows: %d", len(rows))
}
