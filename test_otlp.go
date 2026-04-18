package main

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	metricsv1 "go.opentelemetry.io/proto/otlp/metrics/v1"
)

func main() {
	conn, err := grpc.Dial("localhost:4317", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	ctx := metadata.AppendToOutgoingContext(context.Background(), "x-api-key", "valid-team-api-key-if-auth-is-disabled-or-mocked")
	// NOTE: auth middleware checks DB. If there is an auth check, we need to pass a valid key.
	// But let's just try sending something.

	logsClient := logspb.NewLogsServiceClient(conn)
	_, err = logsClient.Export(ctx, &logspb.ExportLogsServiceRequest{
		ResourceLogs: []*logv1.ResourceLogs{
			{
				ScopeLogs: []*logv1.ScopeLogs{
					{
						LogRecords: []*logv1.LogRecord{
							{
								TimeUnixNano:   uint64(time.Now().UnixNano()),
								SeverityText:   "INFO",
								SeverityNumber: logv1.SeverityNumber_SEVERITY_NUMBER_INFO,
								Body:           &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "test log"}},
							},
						},
					},
				},
			},
		},
	})
	if err != nil {
		log.Printf("logs export failed: %v", err)
	} else {
		log.Println("logs export success")
	}

	metricsClient := metricspb.NewMetricsServiceClient(conn)
	_, err = metricsClient.Export(ctx, &metricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricsv1.ResourceMetrics{
			{
				ScopeMetrics: []*metricsv1.ScopeMetrics{
					{
						Metrics: []*metricsv1.Metric{
							{
								Name: "test.metric",
								Data: &metricsv1.Metric_Gauge{
									Gauge: &metricsv1.Gauge{
										DataPoints: []*metricsv1.NumberDataPoint{
											{
												TimeUnixNano: uint64(time.Now().UnixNano()),
												Value:        &metricsv1.NumberDataPoint_AsDouble{AsDouble: 42.0},
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
	})
	if err != nil {
		log.Printf("metrics export failed: %v", err)
	} else {
		log.Println("metrics export success")
	}
}
