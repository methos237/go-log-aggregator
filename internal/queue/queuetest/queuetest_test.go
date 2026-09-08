package queuetest

import (
	"context"
	"errors"
	"testing"

	"github.com/jamespolk/go-log-aggregator/internal/queue"
)

func TestPublisherRecordsMessages(t *testing.T) {
	t.Parallel()

	var p Publisher
	ack, err := p.Publish(context.Background(), "logs.dev.api", []byte("one"))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if ack.Sequence != 1 {
		t.Errorf("Sequence = %d, want 1", ack.Sequence)
	}

	got := p.Published()
	if len(got) != 1 || got[0].Subject != "logs.dev.api" || string(got[0].Payload) != "one" {
		t.Fatalf("Published() = %+v, want one logs.dev.api message", got)
	}
}

// The caller marshals into a buffer it owns and may reuse, so the double has to
// copy or a test would assert against mutated payloads.
func TestPublisherCopiesPayloads(t *testing.T) {
	t.Parallel()

	var p Publisher
	buf := []byte("original")
	if _, err := p.Publish(context.Background(), "logs.dev.api", buf); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	copy(buf, "mutated!")

	if got := string(p.Published()[0].Payload); got != "original" {
		t.Errorf("recorded payload = %q, want %q", got, "original")
	}
}

func TestPublisherFailWith(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("broker down")
	var p Publisher
	p.FailWith(sentinel)

	if _, err := p.Publish(context.Background(), "logs.dev.api", nil); !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want %v", err, sentinel)
	}
	if p.Count() != 0 {
		t.Errorf("a failed publish was recorded")
	}

	p.FailWith(nil)
	if _, err := p.Publish(context.Background(), "logs.dev.api", nil); err != nil {
		t.Fatalf("Publish after clearing the error: %v", err)
	}
}

func TestPublisherHonorsContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var p Publisher
	if _, err := p.Publish(ctx, "logs.dev.api", nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestPublisherHookRunsBeforeRecording(t *testing.T) {
	t.Parallel()

	var p Publisher
	var sawCount int
	p.OnPublish(func(context.Context, string, []byte) error {
		sawCount = p.Count()
		return nil
	})

	if _, err := p.Publish(context.Background(), "logs.dev.api", nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if sawCount != 0 {
		t.Errorf("hook saw %d recorded messages, want 0: it must run before recording", sawCount)
	}
	if p.Count() != 1 {
		t.Errorf("Count() = %d, want 1", p.Count())
	}
}

// The double renders subjects through the production function, so a test that
// asserts on a subject is asserting on real behavior.
func TestPublisherSubjectMatchesProduction(t *testing.T) {
	t.Parallel()

	var p Publisher
	if got, want := p.Subject("dev", "api.v2"), queue.Subject("logs", "dev", "api.v2"); got != want {
		t.Errorf("Subject() = %q, want %q", got, want)
	}

	p.Prefix = "records"
	if got, want := p.Subject("dev", "api"), "records.dev.api"; got != want {
		t.Errorf("Subject() with custom prefix = %q, want %q", got, want)
	}
}
