package observability

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/kafka/topics"
	"github.com/Optikk-Org/optikk-backend/internal/infra/metrics"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type obsHooks struct{}

// compile-time: ensure we satisfy every interface we claim to implement.
var (
	_ kgo.HookProduceRecordUnbuffered = (*obsHooks)(nil)
	_ kgo.HookFetchRecordUnbuffered   = (*obsHooks)(nil)
	_ kgo.HookBrokerConnect           = (*obsHooks)(nil)
	_ kgo.HookGroupManageError        = (*obsHooks)(nil)
)

func (obsHooks) OnProduceRecordUnbuffered(r *kgo.Record, err error) {
	signal := signalFromTopic(r.Topic)
	result := "ok"
	if err != nil {
		result = "err"
		slog.Debug("kafka produce failed",
			slog.String("topic", r.Topic),
			slog.Any("error", err),
		)
	}

	// Consolidate ingest tracking into one set of metrics. Topic-specific
	// counters are retired in favor of signal counters to keep dashboards clean.
	metrics.IngestRecordsTotal.WithLabelValues(signal, result).Inc()
	metrics.IngestRecordBytes.WithLabelValues(signal).Add(float64(len(r.Value)))

	if !r.Timestamp.IsZero() {
		metrics.KafkaProduceDuration.WithLabelValues(r.Topic).Observe(time.Since(r.Timestamp).Seconds())
	}
}

func signalFromTopic(topic string) string {
	switch {
	case topics.IsLogsTopic(topic):
		return "logs"
	case topics.IsMetricsTopic(topic):
		return "metrics"
	case topics.IsSpansTopic(topic):
		return "spans"
	default:
		return "unknown"
	}
}

func (obsHooks) OnFetchRecordUnbuffered(r *kgo.Record, _ bool) {
	// Consumed count is now tracked via signal; topic label is preserved
	// only for duration/lag which are infrastructure-heavy.
	signal := signalFromTopic(r.Topic)
	metrics.IngestRecordsTotal.WithLabelValues(signal, "ok").Inc()
}

func (obsHooks) OnBrokerConnect(meta kgo.BrokerMetadata, _ time.Duration, _ net.Conn, err error) {
	result := "ok"
	host := net.JoinHostPort(meta.Host, strconv.Itoa(int(meta.Port)))
	if err != nil {
		result = "err"
		slog.Warn("kafka broker connect failed",
			slog.String("host", host),
			slog.Any("error", err),
		)
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

func Hooks() kgo.Opt { return kgo.WithHooks(obsHooks{}) }

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
