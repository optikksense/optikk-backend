package spans

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

// Livetail is the live-tail side of the span pipeline. Dedicated consumer
// group (<base>.spans.livetail) so its offsets do not interfere with
// persistence.
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
			slog.Warn("spans livetail: poll error", slog.Any("error", err))
			continue
		}
		for _, r := range records {
			row := &Row{}
			if err := proto.Unmarshal(r.Value, row); err != nil {
				continue
			}
			l.hub.Publish(int64(row.GetTeamId()), wireSpan(row, time.Now().UnixMilli()))
		}
		if err := l.kafka.Commit(ctx, records); err != nil {
			slog.Warn("spans livetail: commit failed", slog.Any("error", err))
		}
	}
}

// wireSpan builds the livetail.WireSpan JSON the WebSocket handler emits.
// The type itself lives in the livetail package to avoid an import cycle.
func wireSpan(r *Row, emitMs int64) livetail.WireSpan {
	attrs := r.GetAttributes()
	return livetail.WireSpan{
		SpanID:        r.GetSpanId(),
		TraceID:       r.GetTraceId(),
		ServiceName:   attrs["service.name"],
		Host:          attrs["host.name"],
		OperationName: r.GetName(),
		DurationMs:    float64(r.GetDurationNano()) / 1e6,
		Status:        r.GetStatusCodeString(),
		HTTPMethod:    r.GetHttpMethod(),
		HTTPStatus:    r.GetResponseStatusCode(),
		SpanKind:      r.GetKindString(),
		HasError:      r.GetHasError(),
		Timestamp:     time.Unix(0, r.GetTimestampNs()),
		EmitMs:        emitMs,
	}
}
