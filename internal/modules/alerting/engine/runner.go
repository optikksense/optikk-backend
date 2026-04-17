package engine

// Runner owns the three background goroutines (evaluator loop, dispatcher,
// outbox relay) behind a single Start/Stop lifecycle. Wired into
// registry.BackgroundRunner by engine.Module.
type Runner struct {
	evaluator  *EvaluatorLoop
	dispatcher *Dispatcher
	relay      *OutboxRelay
}

func NewRunner(loop *EvaluatorLoop, dispatcher *Dispatcher, relay *OutboxRelay) *Runner {
	return &Runner{evaluator: loop, dispatcher: dispatcher, relay: relay}
}

func (r *Runner) Start() {
	r.dispatcher.Start()
	r.relay.Start()
	r.evaluator.Start()
}

func (r *Runner) Stop() error {
	if err := r.evaluator.Stop(); err != nil {
		return err
	}
	if err := r.relay.Stop(); err != nil {
		return err
	}
	return r.dispatcher.Stop()
}
