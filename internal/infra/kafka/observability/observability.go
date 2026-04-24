package observability

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/metrics"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// obsHooks implements the franz-go hook interfaces we care about. Each
// hook is a plain method; embedding only the interfaces we need keeps the
// compiler honest about coverage.
type obsHooks struct{}

// compile-time: ensure we satisfy every interface we claim to implement.
var (
	_ kgo.HookProduceRecordUnbuffered = (*obsHooks)(nil)
	_ kgo.HookFetchRecordUnbuffered   = (*obsHooks)(nil)
	_ kgo.HookBrokerConnect           = (*obsHooks)(nil)
	_ kgo.HookGroupManageError        = (*obsHooks)(nil)
)

func (obsHooks) OnProduceRecordUnbuffered(r *kgo.Record, err error) {
	result := "ok"
	if err != nil {
		result = "err"
		slog.Warn("kafka produce failed",
			slog.String("topic", r.Topic),
			slog.Int("partition", int(r.Partition)),
			slog.Any("error", err),
		)
	}
	metrics.KafkaProduced.WithLabelValues(r.Topic, result).Inc()
	if !r.Timestamp.IsZero() {
		metrics.KafkaProduceDuration.WithLabelValues(r.Topic).Observe(time.Since(r.Timestamp).Seconds())
	}
}

func (obsHooks) OnFetchRecordUnbuffered(r *kgo.Record, _ bool) {
	metrics.KafkaConsumed.WithLabelValues(r.Topic).Inc()
}

func (obsHooks) OnBrokerConnect(_ kgo.BrokerMetadata, _ time.Duration, _ net.Conn, err error) {
	result := "ok"
	if err != nil {
		result = "err"
	}
	metrics.KafkaBrokerConnects.WithLabelValues(result).Inc()
}

func (obsHooks) OnGroupManageError(err error) {
	if err == nil {
		return
	}
	metrics.KafkaRebalances.WithLabelValues("manage_error").Inc()
	slog.Warn("kafka group manage error", slog.Any("error", err))
}

// Hooks returns the single hook option used by both producer + consumer
// client constructors. Exposed so client.go can opt in via kgo.WithHooks.
func Hooks() kgo.Opt { return kgo.WithHooks(obsHooks{}) }

// LagPoller periodically samples committed-vs-end offsets for a consumer
// group and publishes the delta as `optikk_kafka_consumer_lag_records`.
// Call Run(ctx) from a goroutine; it exits when ctx is cancelled. The
// poller is scoped to a single (group, topic) pair — one per ingest
// consumer. Uses raw `kmsg` protocol requests so we avoid the `kadm`
// top-level dep.
type LagPoller struct {
	client   *kgo.Client
	groupID  string
	topic    string
	interval time.Duration
}

func NewLagPoller(client *kgo.Client, groupID, topic string) *LagPoller {
	return &LagPoller{
		client:   client,
		groupID:  groupID,
		topic:    topic,
		interval: 15 * time.Second,
	}
}

// Run blocks until ctx is Done, emitting gauge updates each tick.
func (p *LagPoller) Run(ctx context.Context) {
	tick := time.NewTicker(p.interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			p.sample(ctx)
		}
	}
}

func (p *LagPoller) sample(ctx context.Context) {
	lag, err := fetchGroupLag(ctx, p.client, p.groupID, p.topic)
	if err != nil {
		slog.DebugContext(ctx, "kafka lag poll failed",
			slog.String("group", p.groupID),
			slog.String("topic", p.topic),
			slog.Any("error", err),
		)
		return
	}
	for k, delta := range lag {
		metrics.KafkaConsumerLag.
			WithLabelValues(k.topic, strconv.Itoa(int(k.partition)), p.groupID).
			Set(float64(delta))
	}
}

type partKey struct {
	topic     string
	partition int32
}

// fetchGroupLag issues three raw Kafka protocol requests against the
// existing consumer client:
//
//  1. MetadataRequest — enumerate partitions for `topic`.
//  2. OffsetFetchRequest — committed offset per (group, topic, partition).
//  3. ListOffsetsRequest (timestamp=-1) — end-offset (HWM) per partition.
//
// Lag per partition = endOffset - committedOffset. Partitions assigned
// but never committed get -1 from the broker; we treat those as "lag =
// endOffset" to surface back-pressure on newly-joined consumers.
func fetchGroupLag(ctx context.Context, client *kgo.Client, groupID, topic string) (map[partKey]int64, error) {
	partitions, err := fetchPartitions(ctx, client, topic)
	if err != nil {
		return nil, fmt.Errorf("metadata: %w", err)
	}
	if len(partitions) == 0 {
		return nil, nil
	}
	committed, err := fetchCommittedOffsets(ctx, client, groupID, topic, partitions)
	if err != nil {
		return nil, fmt.Errorf("offset-fetch: %w", err)
	}
	end, err := fetchEndOffsets(ctx, client, topic, partitions)
	if err != nil {
		return nil, fmt.Errorf("list-offsets: %w", err)
	}
	return diffLag(topic, partitions, committed, end), nil
}

// fetchPartitions resolves the live partition list for `topic` via a
// MetadataRequest routed by franz-go.
func fetchPartitions(ctx context.Context, client *kgo.Client, topic string) ([]int32, error) {
	req := kmsg.NewPtrMetadataRequest()
	t := kmsg.NewMetadataRequestTopic()
	t.Topic = kmsg.StringPtr(topic)
	req.Topics = append(req.Topics, t)
	resp, err := req.RequestWith(ctx, client)
	if err != nil {
		return nil, err
	}
	for _, tr := range resp.Topics {
		if tr.Topic == nil || *tr.Topic != topic {
			continue
		}
		out := make([]int32, 0, len(tr.Partitions))
		for _, pr := range tr.Partitions {
			out = append(out, pr.Partition)
		}
		return out, nil
	}
	return nil, nil
}

// fetchCommittedOffsets returns group.committed[partition] for the given
// topic. Missing entries are encoded as -1.
func fetchCommittedOffsets(ctx context.Context, client *kgo.Client, groupID, topic string, partitions []int32) (map[int32]int64, error) {
	req := kmsg.NewPtrOffsetFetchRequest()
	req.Group = groupID
	rt := kmsg.NewOffsetFetchRequestTopic()
	rt.Topic = topic
	rt.Partitions = append(rt.Partitions, partitions...)
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, client)
	if err != nil {
		return nil, err
	}
	out := make(map[int32]int64, len(partitions))
	for _, tr := range resp.Topics {
		if tr.Topic != topic {
			continue
		}
		for _, pr := range tr.Partitions {
			out[pr.Partition] = pr.Offset
		}
	}
	return out, nil
}

// fetchEndOffsets uses ListOffsetsRequest with Timestamp=-1 to resolve the
// latest (high-water-mark) offset per partition.
func fetchEndOffsets(ctx context.Context, client *kgo.Client, topic string, partitions []int32) (map[int32]int64, error) {
	req := kmsg.NewPtrListOffsetsRequest()
	req.ReplicaID = -1 // regular consumer, not a replica
	rt := kmsg.NewListOffsetsRequestTopic()
	rt.Topic = topic
	for _, pid := range partitions {
		rp := kmsg.NewListOffsetsRequestTopicPartition()
		rp.Partition = pid
		rp.Timestamp = -1 // latest
		rt.Partitions = append(rt.Partitions, rp)
	}
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, client)
	if err != nil {
		return nil, err
	}
	out := make(map[int32]int64, len(partitions))
	for _, tr := range resp.Topics {
		if tr.Topic != topic {
			continue
		}
		for _, pr := range tr.Partitions {
			out[pr.Partition] = pr.Offset
		}
	}
	return out, nil
}

// diffLag computes endOffset - committedOffset per partition. Committed
// offset = -1 means "never committed"; we fall back to endOffset so a
// brand-new consumer surfaces as lagging by the full topic backlog.
func diffLag(topic string, partitions []int32, committed, end map[int32]int64) map[partKey]int64 {
	out := make(map[partKey]int64, len(partitions))
	for _, p := range partitions {
		endOff, ok := end[p]
		if !ok {
			continue
		}
		commOff := committed[p]
		lag := endOff - commOff
		if commOff < 0 {
			lag = endOff
		}
		if lag < 0 {
			lag = 0
		}
		out[partKey{topic: topic, partition: p}] = lag
	}
	return out
}
