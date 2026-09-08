package ingest

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jamespolk/go-log-aggregator/internal/config"
	"github.com/jamespolk/go-log-aggregator/internal/observability"
	"github.com/jamespolk/go-log-aggregator/internal/queue"
)

// Pipeline errors.
var (
	// errShed means the intake buffer was full. The batch was never queued, so the
	// agent is told to back off and resend.
	errShed = errors.New("ingest buffer is full")
	// errPipelineClosed means the node is shutting down. Retryable elsewhere:
	// nothing was acknowledged.
	errPipelineClosed = errors.New("ingest pipeline is closed")
)

// job is one batch on its way to the queue.
//
// reply is what keeps the ack honest. The handler hands the job to a publisher and
// then waits on this channel, so the bounded buffer decouples *who publishes* from
// *who received*, without ever decoupling the ack from durability. A fire-and-forget
// queue here would be much simpler and would silently break the one guarantee this
// service makes.
type job struct {
	subject string
	payload []byte
	// records is the count carried by this job, which is what the depth gauge
	// reports: records are what consume memory, batches are not comparable units.
	records  int
	enqueued time.Time
	// reply is buffered so a publisher never blocks on a handler that has given up.
	reply chan error
}

// pipeline is the bounded channel between the gRPC handlers and the queue, plus
// the pool of publishers that drains it.
//
// Two properties matter and they pull in opposite directions. The buffer must be
// bounded, because an unbounded one turns a slow broker into an OOM. And the
// handler must not block forever on a full buffer, because an agent waiting on an
// ack that will not come is worse than an agent told to slow down. So a full buffer
// sheds immediately — select with default, no timeout — and the agent gets
// OVERLOADED while the stream stays open.
type pipeline struct {
	queue queue.Publisher
	// pubCtx is the process context with cancellation stripped. Publishing must not
	// inherit a canceled client context — that would abandon a batch already halfway
	// to durable — but it should still carry the process's values, so this is
	// WithoutCancel rather than a fresh Background.
	pubCtx  context.Context
	in      chan *job
	log     *slog.Logger
	metrics *Metrics
	now     nowFunc

	// depth is queued records rather than queued jobs, matching the writer's gauge
	// so both ends of the pipeline are measured in the same unit.
	depth atomic.Int64

	workers int
	wg      sync.WaitGroup
	// quit is closed by close to stop the publishers once the handlers are gone.
	quit      chan struct{}
	closing   atomic.Bool
	closeOnce sync.Once
}

//nolint:gocritic // hugeParam: one copy per process; by value keeps it immutable
func newPipeline(ctx context.Context, cfg config.Ingest, q queue.Publisher, metrics *Metrics, log *slog.Logger, now nowFunc) *pipeline {
	// Clamped rather than trusted. Config validation already rejects a
	// non-positive count, but a pipeline with no publishers accepts batches and
	// never drains them, which presents as every agent hanging — much worse to
	// diagnose than one extra goroutine.
	workers := max(cfg.PublishWorkers, 1)

	return &pipeline{
		queue:   q,
		pubCtx:  context.WithoutCancel(ctx),
		in:      make(chan *job, cfg.BufferSize),
		log:     log,
		metrics: metrics,
		now:     now,
		workers: workers,
		quit:    make(chan struct{}),
	}
}

// start launches the publishers. It returns immediately.
func (p *pipeline) start() {
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.publish(i)
	}
	p.log.Info("ingest pipeline started",
		slog.Int("publishers", p.workers),
		slog.Int("buffer_size", cap(p.in)),
	)
}

// submit queues a job and waits for its result.
//
// The two selects are deliberately different. Enqueueing uses default, so a full
// buffer sheds instead of waiting: this is the load-shedding point named in the
// roadmap's backpressure chain, and the whole reason it exists is that there is
// still an unacknowledged client here to say "slow down" to. Waiting for the reply
// then blocks as long as the client is willing to wait, because the reply is what
// makes the ack truthful.
func (p *pipeline) submit(ctx context.Context, j *job) error {
	if p.closing.Load() {
		return errPipelineClosed
	}

	j.enqueued = p.now()
	j.reply = make(chan error, 1)

	p.addDepth(j.records)
	select {
	case p.in <- j:
	default:
		p.addDepth(-j.records)
		return errShed
	}

	select {
	case err := <-j.reply:
		return err
	case <-ctx.Done():
		// The client is gone. The publisher still owns the job and will finish it —
		// possibly making it durable — which is harmless: an agent that never saw an
		// ack resends, and storage deduplicates the replay.
		return ctx.Err()
	}
}

// publish is one publisher: take a job, publish it, report the result.
func (p *pipeline) publish(id int) {
	defer p.wg.Done()

	log := p.log.With(slog.Int("publisher", id))

	for {
		select {
		case j := <-p.in:
			p.run(j)
		case <-p.quit:
			// Drain what is left rather than abandoning it. Every handler still
			// waiting on a reply is holding an agent's stream open, and close is only
			// called once the handlers are gone — so this loop finishes the jobs whose
			// handlers already left, and their replies fall on the floor unread.
			p.drain(log)
			return
		}
	}
}

func (p *pipeline) run(j *job) {
	p.addDepth(-j.records)
	p.observeWait(j)

	// pubCtx, not the handler's context: the publish is bounded by the queue's own
	// publish timeout, and a canceled client must not abort a batch that is already
	// halfway to being durable.
	_, err := p.queue.Publish(p.pubCtx, j.subject, j.payload)
	j.reply <- err
}

func (p *pipeline) drain(log *slog.Logger) {
	drained := 0
	for {
		select {
		case j := <-p.in:
			p.run(j)
			drained++
		default:
			if drained > 0 {
				log.Info("drained queued batches on shutdown", slog.Int("batches", drained))
			}
			return
		}
	}
}

// close stops accepting jobs, drains the publishers and waits for them.
//
// Callers must stop the gRPC server first. A handler blocked on a reply would
// otherwise be waiting on publishers that have already exited, and would hold the
// shutdown open until its context expired.
func (p *pipeline) close(ctx context.Context) error {
	p.closeOnce.Do(func() {
		p.closing.Store(true)
		close(p.quit)
	})

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		p.log.Info("ingest pipeline stopped")
		return nil
	case <-ctx.Done():
		return errors.Join(errPipelineClosed, ctx.Err())
	}
}

func (p *pipeline) addDepth(delta int) {
	depth := p.depth.Add(int64(delta))
	p.metrics.QueueDepth.WithLabelValues(observability.QueueIngest).Set(float64(depth))
}

func (p *pipeline) observeWait(j *job) {
	if j.enqueued.IsZero() {
		return
	}
	p.metrics.QueueWait.WithLabelValues(observability.QueueIngest).Observe(p.now().Sub(j.enqueued).Seconds())
}
