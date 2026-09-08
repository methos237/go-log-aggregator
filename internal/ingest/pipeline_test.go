package ingest

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/jamespolk/go-log-aggregator/internal/config"
	"github.com/jamespolk/go-log-aggregator/internal/observability"
	"github.com/jamespolk/go-log-aggregator/internal/queue/queuetest"
)

// newPipe builds a started pipeline with the given buffer and publisher counts.
func newPipe(t *testing.T, pub *queuetest.Publisher, metrics *Metrics, buffer, workers int) *pipeline {
	t.Helper()

	cfg := config.Ingest{BufferSize: buffer, PublishWorkers: workers}
	p := newPipeline(context.Background(), cfg, pub, metrics, slog.New(slog.DiscardHandler), time.Now)
	p.start()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := p.close(ctx); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	return p
}

func TestPipelinePublishesAndReports(t *testing.T) {
	t.Parallel()

	pub := &queuetest.Publisher{}
	p := newPipe(t, pub, NewMetrics(nil), 4, 2)

	if err := p.submit(context.Background(), &job{subject: "logs.dev.api", payload: []byte("x"), records: 1}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if pub.Count() != 1 {
		t.Errorf("published %d messages, want 1", pub.Count())
	}
}

// A publish failure has to come back to the submitter, not be swallowed by the
// publisher goroutine: the submitter is the one holding an agent's stream open.
func TestPipelineReportsPublishFailures(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("broker down")
	pub := &queuetest.Publisher{}
	pub.FailWith(sentinel)
	p := newPipe(t, pub, NewMetrics(nil), 4, 1)

	if err := p.submit(context.Background(), &job{records: 1}); !errors.Is(err, sentinel) {
		t.Fatalf("submit err = %v, want %v", err, sentinel)
	}
}

// The load-shedding point: a full buffer must refuse immediately rather than wait,
// because an agent told to slow down is better off than one blocked on an ack.
func TestPipelineShedsWhenTheBufferIsFull(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	inPublish := make(chan struct{}, 1)
	pub := &queuetest.Publisher{}
	pub.OnPublish(func(context.Context, string, []byte) error {
		inPublish <- struct{}{}
		<-release
		return nil
	})
	defer close(release)

	// One publisher, one buffer slot: the first job occupies the publisher, the
	// second fills the buffer, the third has nowhere to go.
	p := newPipe(t, pub, NewMetrics(nil), 1, 1)

	occupied := make(chan error, 1)
	go func() { occupied <- p.submit(context.Background(), &job{records: 1}) }()
	select {
	case <-inPublish:
	case <-time.After(5 * time.Second):
		t.Fatal("the first job never reached the publisher")
	}

	buffered := make(chan error, 1)
	go func() { buffered <- p.submit(context.Background(), &job{records: 1}) }()

	// Wait for the buffer slot to be taken, then the next submit must shed rather
	// than block.
	deadline := time.Now().Add(5 * time.Second)
	for len(p.in) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	shedStart := time.Now()
	err := p.submit(context.Background(), &job{records: 1})
	if !errors.Is(err, errShed) {
		t.Fatalf("submit err = %v, want errShed", err)
	}
	if elapsed := time.Since(shedStart); elapsed > time.Second {
		t.Errorf("shedding took %s; it must not wait for capacity", elapsed)
	}

	release <- struct{}{}
	release <- struct{}{}
	if err = <-occupied; err != nil {
		t.Errorf("first submit: %v", err)
	}
	if err = <-buffered; err != nil {
		t.Errorf("buffered submit: %v", err)
	}
}

// Depth is reported in records and must come back to zero, or the gauge drifts
// upward until it is useless.
func TestPipelineDepthReturnsToZero(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	pub := &queuetest.Publisher{}
	p := newPipe(t, pub, NewMetrics(reg), 8, 2)

	for i := 0; i < 5; i++ {
		if err := p.submit(context.Background(), &job{records: 3}); err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}

	if got := p.depth.Load(); got != 0 {
		t.Errorf("depth = %d, want 0 after every job completed", got)
	}
	if got := gauge(t, reg, "logagg_queue_depth", map[string]string{"queue": observability.QueueIngest}); got != 0 {
		t.Errorf("queue_depth gauge = %v, want 0", got)
	}
	// The wait histogram is what distinguishes "full but draining" from "stuck", so
	// it has to actually observe.
	if got := histogramCount(t, reg, "logagg_queue_wait_seconds", observability.QueueIngest); got != 5 {
		t.Errorf("queue_wait_seconds count = %d, want 5", got)
	}
}

func TestPipelineRefusesAfterClose(t *testing.T) {
	t.Parallel()

	pub := &queuetest.Publisher{}
	cfg := config.Ingest{BufferSize: 4, PublishWorkers: 1}
	p := newPipeline(context.Background(), cfg, pub, NewMetrics(nil), slog.New(slog.DiscardHandler), time.Now)
	p.start()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Refusing rather than blocking forever is what turns a shutdown into a
	// retryable answer for the agent.
	if err := p.submit(context.Background(), &job{records: 1}); !errors.Is(err, errPipelineClosed) {
		t.Fatalf("submit after close = %v, want errPipelineClosed", err)
	}
	if err := p.close(ctx); err != nil {
		t.Errorf("close is not idempotent: %v", err)
	}
}

// Queued work must be finished on shutdown, not abandoned: those batches have not
// been acked, but finishing them is free and avoids a pointless redelivery.
func TestPipelineDrainsQueuedJobsOnClose(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	inPublish := make(chan struct{}, 1)
	pub := &queuetest.Publisher{}
	pub.OnPublish(func(context.Context, string, []byte) error {
		select {
		case inPublish <- struct{}{}:
		default:
		}
		<-release
		return nil
	})

	p := newPipeline(context.Background(), config.Ingest{BufferSize: 4, PublishWorkers: 1}, pub,
		NewMetrics(nil), slog.New(slog.DiscardHandler), time.Now)
	p.start()

	// One job holds the publisher; two more sit in the buffer with no handler
	// waiting on them, which is the state a hard client disconnect leaves behind.
	go func() { _ = p.submit(context.Background(), &job{records: 1}) }()
	<-inPublish
	p.in <- &job{records: 1, reply: make(chan error, 1)}
	p.in <- &job{records: 1, reply: make(chan error, 1)}

	closed := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		closed <- p.close(ctx)
	}()

	close(release)
	if err := <-closed; err != nil {
		t.Fatalf("close: %v", err)
	}
	if pub.Count() != 3 {
		t.Errorf("published %d batches, want 3: queued work was abandoned", pub.Count())
	}
}

func gauge(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) float64 {
	t.Helper()

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			if !matches(m.GetLabel(), labels) {
				continue
			}
			return m.GetGauge().GetValue()
		}
	}
	t.Fatalf("gauge %s%v was not found", name, labels)
	return 0
}

func histogramCount(t *testing.T, reg *prometheus.Registry, name, queueLabel string) uint64 {
	t.Helper()

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			if !matches(m.GetLabel(), map[string]string{"queue": queueLabel}) {
				continue
			}
			return m.GetHistogram().GetSampleCount()
		}
	}
	t.Fatalf("histogram %s{queue=%q} was not found", name, queueLabel)
	return 0
}

// matches reports whether every wanted label is present with the wanted value.
func matches(labels []*dto.LabelPair, want map[string]string) bool {
	for name, value := range want {
		found := false
		for _, l := range labels {
			if l.GetName() == name {
				if l.GetValue() != value {
					return false
				}
				found = true
			}
		}
		if !found {
			return false
		}
	}
	return true
}
