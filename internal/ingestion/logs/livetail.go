package logs

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	kafkainfra "github.com/Optikk-Org/optikk-backend/internal/infra/kafka"
	"github.com/Optikk-Org/optikk-backend/internal/modules/livetail"
	"google.golang.org/protobuf/proto"
)

// Livetail is the live-tail side of the log pipeline. It joins a dedicated
// consumer group (<base>.logs.livetail) so its offsets advance independently
// from persistence. After unmarshal it publishes a wire-shaped payload to the
// existing hub (Redis stream) that the WS livetail handler reads.
type Livetail struct {
	kafka *kafkainfra.Consumer
	hub   livetail.Hub

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewLivetail(kafka *kafkainfra.Consumer, hub livetail.Hub) *Livetail {
	return &Livetail{kafka: kafka, hub: hub}
}

func (l *Livetail) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	l.cancel = cancel
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		l.run(ctx)
	}()
}

func (l *Livetail) Stop() error {
	if l.cancel != nil {
		l.cancel()
	}
	l.wg.Wait()
	l.kafka.Close()
	return nil
}

func (l *Livetail) run(ctx context.Context) {
	for {
		records, err := l.kafka.PollBatch(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			slog.Warn("logs livetail: poll error", slog.Any("error", err))
			continue
		}
		for _, r := range records {
			row := &Row{}
			if err := proto.Unmarshal(r.Value, row); err != nil {
				continue
			}
			l.hub.Publish(int64(row.GetTeamId()), wireLog(row, time.Now().UnixMilli()))
		}
		if err := l.kafka.Commit(ctx, records); err != nil {
			slog.Warn("logs livetail: commit failed", slog.Any("error", err))
		}
	}
}

// wireLog builds the livetail.WireLog JSON the WebSocket handler emits.
// The type itself lives in the livetail package to avoid an import cycle;
// this function is the single site that translates a Row into the wire shape.
func wireLog(r *Row, emitMs int64) livetail.WireLog {
	res := r.GetResource()
	return livetail.WireLog{
		Timestamp:         uint64(r.GetTimestampNs()), //nolint:gosec
		ObservedTimestamp: r.GetObservedTimestampNs(),
		SeverityText:      r.GetSeverityText(),
		SeverityNumber:    uint8(r.GetSeverityNumber()), //nolint:gosec
		Body:              r.GetBody(),
		TraceID:           r.GetTraceId(),
		SpanID:            r.GetSpanId(),
		TraceFlags:        r.GetTraceFlags(),
		ServiceName:       res["service.name"],
		Host:              res["host.name"],
		Pod:               res["k8s.pod.name"],
		Container:         res["k8s.container.name"],
		Environment:       res["deployment.environment"],
		AttributesString:  r.GetAttributesString(),
		AttributesNumber:  r.GetAttributesNumber(),
		AttributesBool:    r.GetAttributesBool(),
		ScopeName:         r.GetScopeName(),
		ScopeVersion:      r.GetScopeVersion(),
		Level:             r.GetSeverityText(),
		Message:           r.GetBody(),
		Service:           res["service.name"],
		EmitMs:            emitMs,
	}
}
