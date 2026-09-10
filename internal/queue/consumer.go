package queue

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/nats-io/nats.go/jetstream"
)

// Message is one delivery from the queue.
//
// The three terminal calls are distinct on purpose, because they encode three
// different judgements about the same batch:
//
//   - Ack: durable elsewhere now, remove it from the stream.
//   - Nak: this attempt failed but another could work; redeliver it.
//   - Term: this batch will never succeed; stop redelivering and count the loss.
//
// Collapsing Term into Nak is the tempting mistake. A payload that does not decode
// fails identically forever, so redelivering it burns the consumer's ack budget on
// a message that can never leave the stream — a poison message that stalls every
// healthy batch behind it.
type Message interface {
	Data() []byte
	Subject() string
	// Redeliveries is how many times this message has been delivered, counting the
	// current delivery. One means a first attempt.
	Redeliveries() uint64
	Ack() error
	Nak() error
	Term() error
}

// Handler processes one message and is responsible for terminating it — exactly one
// of Ack, Nak or Term, on every path.
type Handler func(Message)

// Subscription is a running consumer. Stop must be called to release it.
type Subscription interface {
	Stop()
}

// Consume binds to the shared durable consumer and delivers messages to h.
//
// One durable consumer shared by every replica rather than one consumer per node.
// That is what makes horizontal scaling distribute the backlog instead of
// duplicating it: work-queue retention plus a shared durable means each message
// goes to exactly one collector, and a node that dies simply stops competing for
// messages while its unacked ones return to the pool.
func (c *Conn) Consume(ctx context.Context, h Handler) (Subscription, error) {
	stream, err := c.js.Stream(ctx, c.cfg.StreamName)
	if err != nil {
		return nil, fmt.Errorf("look up stream %s: %w", c.cfg.StreamName, err)
	}

	cons, err := stream.CreateOrUpdateConsumer(ctx, c.consumerConfig())
	if err != nil {
		return nil, fmt.Errorf("create consumer %s: %w", c.cfg.Durable, err)
	}

	sub, err := cons.Consume(func(msg jetstream.Msg) {
		m := &message{msg: msg}
		if m.Redeliveries() > 1 {
			c.metrics.Redeliveries.Inc()
		}
		c.metrics.Consumed.Inc()
		h(m)
	})
	if err != nil {
		return nil, fmt.Errorf("consume from %s: %w", c.cfg.Durable, err)
	}

	c.log.Info("queue consumer started",
		slog.String("durable", c.cfg.Durable),
		slog.Int("max_ack_pending", c.cfg.MaxAckPending),
		slog.Duration("ack_wait", c.cfg.AckWait),
	)
	return sub, nil
}

// consumerConfig is the durable pull consumer the writers bind to.
//
// AckExplicitPolicy is the only correct choice here: the whole design acks a
// message after the write lands, so an implicit or none policy would drop the
// backlog on the floor the moment it was delivered.
//
// MaxDeliver is left unlimited. A batch reaching this consumer has already been
// validated at ingest, so a write that keeps failing means the database is
// unreachable or the schema is wrong — conditions that get fixed, after which the
// backlog should still be there. Bounding redelivery would instead discard records
// during exactly the outage the queue exists to survive. Genuinely unprocessable
// messages are handled by Term, not by a delivery ceiling.
func (c *Conn) consumerConfig() jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		Durable:       c.cfg.Durable,
		Description:   "Writers draining log records into TimescaleDB.",
		FilterSubject: c.cfg.SubjectPrefix + ".>",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       c.cfg.AckWait,
		MaxAckPending: c.cfg.MaxAckPending,
		MaxDeliver:    -1,
	}
}

// message adapts jetstream.Msg to Message.
type message struct {
	msg jetstream.Msg
}

func (m *message) Data() []byte    { return m.msg.Data() }
func (m *message) Subject() string { return m.msg.Subject() }
func (m *message) Ack() error      { return m.msg.Ack() }
func (m *message) Nak() error      { return m.msg.Nak() }
func (m *message) Term() error     { return m.msg.Term() }

// Redeliveries reports the delivery count, or 1 when the metadata is unavailable.
//
// Unavailable metadata means the message did not come from a stream, which cannot
// happen on this path; reporting a first delivery is the reading that avoids
// inflating the redelivery metric on a message that has no history.
func (m *message) Redeliveries() uint64 {
	meta, err := m.msg.Metadata()
	if err != nil {
		return 1
	}
	return meta.NumDelivered
}
