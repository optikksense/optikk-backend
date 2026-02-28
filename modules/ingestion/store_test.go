package telemetry

import "testing"

func TestShouldPersistMetric_AllowsAnyMetricName(t *testing.T) {
	testNames := []string{
		"",
		"system.cpu.utilization",
		"custom.metric.from.internet",
		"go_gc_duration_seconds",
		"node_cpu_seconds_total",
	}
	for _, name := range testNames {
		if !shouldPersistMetric(name) {
			t.Fatalf("expected metric %q to be persisted", name)
		}
	}
}
