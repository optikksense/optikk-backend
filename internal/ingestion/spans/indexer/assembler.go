package indexer

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// Span is the neutral shape the Assembler consumes. Callers (the writer path)
// adapt their concrete Row type into this struct so the indexer doesn't
// depend on the outer `spans` package (import-cycle avoidance).
type Span struct {
	TeamID		uint32
	TraceID		string
	SpanID		string
	ParentSpanID	string
	Service		string
	Name		string
	StartMs		int64
	EndMs		int64
	IsRoot		bool
	IsError		bool
	HTTPMethod	string
	HTTPStatus	string
	StatusCode	string
	PeerService	string
	ErrorFp		string
	Environment	string
	TsBucketStart	uint64
}

// Assembler groups raw spans by trace_id, maintains a bounded pending set,
// and emits TraceIndexRow via the injected Emitter when a trace is complete.
// Completion: root observed AND 10s quiet, OR 60s hard timeout.
type Assembler struct {
	state		*state
	emitter		Emitter
	quiet		time.Duration
	hardTO		time.Duration
	sweepInt	time.Duration

	cancel	context.CancelFunc
	wg	sync.WaitGroup
}

// Config controls the bounded state + sweep cadence. Zero values pick sane
// defaults (100k entries, 10s quiet, 60s hard timeout, 5s sweep).
type Config struct {
	Capacity	int
	QuietWindow	time.Duration
	HardTimeout	time.Duration
	SweepEvery	time.Duration
}

func DefaultConfig() Config {
	return Config{
		Capacity:	100_000,
		QuietWindow:	10 * time.Second,
		HardTimeout:	60 * time.Second,
		SweepEvery:	5 * time.Second,
	}
}

func New(emitter Emitter, cfg Config) *Assembler {
	if cfg.QuietWindow <= 0 {
		cfg.QuietWindow = 10 * time.Second
	}
	if cfg.HardTimeout <= 0 {
		cfg.HardTimeout = 60 * time.Second
	}
	if cfg.SweepEvery <= 0 {
		cfg.SweepEvery = 5 * time.Second
	}
	return &Assembler{
		state:		newState(cfg.Capacity),
		emitter:	emitter,
		quiet:		cfg.QuietWindow,
		hardTO:		cfg.HardTimeout,
		sweepInt:	cfg.SweepEvery,
	}
}

// Observe folds a newly-arrived span into the pending entry for its trace.
// Evicted entries (from LRU overflow) are emitted with truncated=true.
func (a *Assembler) Observe(ctx context.Context, s Span) {
	now := time.Now()
	p, evicted := a.state.Upsert(s.TeamID, s.TraceID, now, s.TsBucketStart)
	mergeSpan(p, s)
	for _, e := range evicted {
		a.emit(ctx, e, true)
	}
}

// Start launches the sweep loop. Stop cancels it.
func (a *Assembler) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	a.cancel = cancel
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		ticker := time.NewTicker(a.sweepInt)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				drainCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				a.drain(drainCtx)
				return
			case <-ticker.C:
				a.sweep(context.Background())
			}
		}
	}()
}

func (a *Assembler) Stop() error {
	if a.cancel != nil {
		a.cancel()
	}
	a.wg.Wait()
	return nil
}

// Len exposes pending-trace count for metrics.
func (a *Assembler) Len() int	{ return a.state.Len() }

func (a *Assembler) sweep(ctx context.Context) {
	now := time.Now()
	done := a.state.Sweep(now, a.quiet, a.hardTO)
	for _, p := range done {
		// hard timeout path → truncated; quiet-after-root path → not truncated.
		truncated := now.Sub(p.firstSeen) >= a.hardTO && !p.rootSeen
		a.emit(ctx, p, truncated)
	}
}

// drain on shutdown emits whatever is in flight as truncated. Bound by
// ShutdownTimeout on the outer module.
func (a *Assembler) drain(ctx context.Context) {
	done := a.state.Sweep(time.Now().Add(1*time.Hour), 0, 0)
	for _, p := range done {
		a.emit(ctx, p, true)
	}
}

func (a *Assembler) emit(ctx context.Context, p *pending, truncated bool) {
	if p == nil {
		return
	}
	row := p.ToRow(truncated)
	if err := a.emitter.Emit(ctx, row); err != nil {
		slog.WarnContext(ctx, "indexer: emit failed",
			slog.String("trace_id", p.traceID), slog.Any("error", err))
	}
}

// mergeSpan folds one span into the pending aggregate.
func mergeSpan(p *pending, s Span) {
	p.spanCount++
	if s.IsError {
		p.errorCount++
		if p.errorFp == "" {
			p.errorFp = s.ErrorFp
		}
	}
	if s.Service != "" {
		p.services[s.Service] = struct{}{}
	}
	if s.PeerService != "" {
		p.peers[s.PeerService] = struct{}{}
	}
	if p.startMs == 0 || s.StartMs < p.startMs {
		p.startMs = s.StartMs
	}
	if s.EndMs > p.endMs {
		p.endMs = s.EndMs
	}
	if s.IsRoot {
		p.rootSeen = true
		p.rootService = s.Service
		p.rootOperation = s.Name
		p.rootStatus = s.StatusCode
		p.rootHTTPMethod = s.HTTPMethod
		p.rootHTTPStatus = s.HTTPStatus
		if s.Environment != "" {
			p.environment = s.Environment
		}
	}
}
