// Package queuetest provides an in-memory queue.Publisher for tests.
//
// It exists so the ingest path can be tested without a broker. That matters more
// than the usual convenience argument: the property worth testing on that path is
// ordering — no agent is acknowledged before its batch is durable — and ordering
// is far easier to assert against a publisher that can be told to block or fail on
// command than against a real JetStream.
package queuetest

import (
	"context"
	"sync"

	"github.com/jamespolk/go-log-aggregator/internal/queue"
)

// Publisher records everything published to it.
//
// The zero value is ready to use and behaves as a broker that accepts everything.
type Publisher struct {
	mu        sync.Mutex
	published []Message

	// err, when set, is returned by every Publish instead of accepting the message.
	err error
	// hook, when set, runs before the message is recorded. It can block, which is
	// how a test holds a publish open long enough to observe that no ack was sent.
	hook func(ctx context.Context, subject string, payload []byte) error
}

// Message is one recorded publish.
type Message struct {
	Subject string
	Payload []byte
}

var _ queue.Publisher = (*Publisher)(nil)

// Publish records the message.
func (p *Publisher) Publish(ctx context.Context, subject string, payload []byte) error {
	p.mu.Lock()
	hook, err := p.hook, p.err
	p.mu.Unlock()

	// Called outside the lock so a blocking hook does not also block Published.
	if hook != nil {
		if hookErr := hook(ctx, subject, payload); hookErr != nil {
			return hookErr
		}
	}
	if err != nil {
		return err
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	// Copied because the caller owns the buffer it marshaled into and may reuse it.
	p.published = append(p.published, Message{Subject: subject, Payload: append([]byte(nil), payload...)})
	return nil
}

// Subject renders a subject using the same scheme as the real publisher, so a
// test that asserts on subjects is asserting on production behavior.
func (p *Publisher) Subject(env, service string) string {
	return queue.Subject("logs", env, service)
}

// FailWith makes every subsequent Publish return err. A nil err restores normal
// behavior.
func (p *Publisher) FailWith(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.err = err
}

// OnPublish installs a hook that runs before each message is recorded. A nil hook
// removes it.
func (p *Publisher) OnPublish(hook func(ctx context.Context, subject string, payload []byte) error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.hook = hook
}

// Published returns a snapshot of everything accepted so far.
func (p *Publisher) Published() []Message {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]Message(nil), p.published...)
}

// Count returns how many messages were accepted.
func (p *Publisher) Count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.published)
}
