// Package observability holds the Kafka-client cross-cuts: franz-go hook
// instrumentation, OTel trace-context propagation via record headers, and
// the lag poller that publishes `optikk_kafka_consumer_lag_records`.
package observability

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
)

// This file bridges the OTel W3C TraceContext propagator to Kafka
// record headers so a trace that starts at the producer side (e.g. the
// OTLP ingest gRPC handler) links up to the consumer-side span that
// persists the batch to ClickHouse.
//
// The propagator installed in internal/infra/otel/provider.go writes
// exactly `traceparent` (+ optional `tracestate`) as TextMap entries;
// kgo.Record.Headers preserve them byte-for-byte end-to-end.

// writeCarrier adapts *kgo.Record for the propagator's Inject path. On
// Set we overwrite any existing header with the same key so a reused
// record doesn't accumulate stale traceparents.
type writeCarrier struct{ record *kgo.Record }

func (c writeCarrier) Get(key string) string {
	for _, h := range c.record.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c writeCarrier) Set(key, value string) {
	for i := range c.record.Headers {
		if c.record.Headers[i].Key == key {
			c.record.Headers[i].Value = []byte(value)
			return
		}
	}
	c.record.Headers = append(c.record.Headers, kgo.RecordHeader{
		Key: key, Value: []byte(value),
	})
}

func (c writeCarrier) Keys() []string {
	out := make([]string, 0, len(c.record.Headers))
	for _, h := range c.record.Headers {
		out = append(out, h.Key)
	}
	return out
}

// readCarrier adapts a []kgo.RecordHeader slice for the propagator's
// Extract path. Set is a no-op — Extract never calls it.
type readCarrier struct{ headers []kgo.RecordHeader }

func (c readCarrier) Get(key string) string {
	for _, h := range c.headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c readCarrier) Set(string, string) {}

func (c readCarrier) Keys() []string {
	out := make([]string, 0, len(c.headers))
	for _, h := range c.headers {
		out = append(out, h.Key)
	}
	return out
}

// InjectTraceContext writes the active OTel span context from ctx into
// record.Headers as W3C `traceparent` / `tracestate` entries. Safe on a
// record with no existing headers or on one that already has unrelated
// headers — existing keys are preserved.
func InjectTraceContext(ctx context.Context, record *kgo.Record) {
	if record == nil {
		return
	}
	otel.GetTextMapPropagator().Inject(ctx, writeCarrier{record: record})
}

// ExtractTraceContext returns parent decorated with any trace context
// found in headers. When the producer side never injected, the returned
// ctx is indistinguishable from parent so `tracer.Start(ctx, …)` simply
// creates a fresh root span.
func ExtractTraceContext(parent context.Context, headers []kgo.RecordHeader) context.Context {
	return otel.GetTextMapPropagator().Extract(parent, readCarrier{headers: headers})
}
