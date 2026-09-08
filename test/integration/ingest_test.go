//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	logaggv1 "github.com/jamespolk/go-log-aggregator/api/proto/logagg/v1"
	"github.com/jamespolk/go-log-aggregator/internal/ingest"
)

// batchOf builds a batch of n records for one stream, numbered from firstSeq.
func batchOf(id, service string, firstSeq int64, n int) *logaggv1.LogBatch {
	now := time.Now()
	records := make([]*logaggv1.LogRecord, 0, n)
	for i := 0; i < n; i++ {
		records = append(records, &logaggv1.LogRecord{
			TimeUnixNano: now.Add(time.Duration(i) * time.Microsecond).UnixNano(),
			Seq:          firstSeq + int64(i),
			Level:        logaggv1.Level_LEVEL_INFO,
			Message:      fmt.Sprintf("%s record %d", service, firstSeq+int64(i)),
		})
	}
	return &logaggv1.LogBatch{
		BatchId: id,
		Labels:  &logaggv1.LabelSet{Service: service, Host: "test-host", Env: "test"},
		Records: records,
	}
}

func dial(t *testing.T, addr string) *ingest.Client {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	client, err := ingest.Dial(ctx, ingest.ClientConfig{Addr: addr, DialTimeout: 10 * time.Second})
	require.NoError(t, err, "dial %s", addr)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// The phase's headline claim: a record handed to the gRPC front door ends up in the
// hypertable, through a real broker and a real database, with nothing in between
// faked.
func TestIngestDeliversRecordsToTimescale(t *testing.T) {
	t.Parallel()

	pool, opt := migratedDBWithStream(t)
	c := startCollector(t, opt, nil)
	defer c.stop(t)

	client := dial(t, c.addr)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const (
		service = "ingest-happy-path"
		batches = 20
		perID   = 50
	)
	for i := 0; i < batches; i++ {
		ack, err := client.SendBatch(ctx,
			batchOf(fmt.Sprintf("b%d", i), service, int64(i*perID), perID))
		require.NoError(t, err, "send batch %d", i)
		require.Equal(t, logaggv1.AckCode_ACK_CODE_ACCEPTED, ack.GetCode(),
			"batch %d: %s", i, ack.GetDetail())
		require.EqualValues(t, perID, ack.GetAccepted(), "batch %d accepted count", i)
	}

	want := batches * perID
	waitFor(t, 30*time.Second, "records to reach the hypertable", func() bool {
		return countRows(ctx, t, pool, service) == want
	})

	// The stream dimension row has to exist exactly once, or the label set was
	// fingerprinted inconsistently somewhere along the path.
	var streams int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM streams WHERE service = $1`, service).Scan(&streams))
	require.Equal(t, 1, streams, "one label set must produce one stream row")
}

// The exit criterion that matters most: kill the collector mid-stream and no record
// it acknowledged may be missing afterwards.
//
// The mechanism being tested is the deferred ack. Ingest acks the agent once
// JetStream has the batch, and the queue message is acked only once the rows are in
// the hypertable — so a crash in between leaves the message unacked, the broker
// redelivers it to the next collector, and the dedup index absorbs anything that was
// written twice.
func TestKillingTheCollectorLosesNoAckedRecord(t *testing.T) {
	t.Parallel()

	pool, opt := migratedDBWithStream(t)
	const service = "ingest-kill-mid-stream"

	// A slow writer widens the window this test needs: batches sit acked-to-the-agent
	// but unwritten when the kill lands, which is precisely the dangerous state.
	opt.writerBatchSize = 100000
	opt.writerFlush = 30 * time.Second

	first := startCollector(t, opt, nil)
	client := dial(t, first.addr)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	stream, err := client.Stream(ctx)
	require.NoError(t, err, "open stream")

	const (
		batches = 40
		perID   = 25
	)

	// Track exactly what the collector promised, which is the only thing this test is
	// allowed to demand back. Guarded because the reader runs while the main goroutine
	// polls it.
	promised := &promises{seqs: make(map[int64]bool)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			ack, recvErr := stream.Recv()
			if recvErr != nil {
				return
			}
			if ack.GetCode() != logaggv1.AckCode_ACK_CODE_ACCEPTED {
				continue
			}
			var idx int
			if _, serr := fmt.Sscanf(ack.GetBatchId(), "b%d", &idx); serr != nil {
				continue
			}
			promised.add(idx, perID)
		}
	}()

	for i := 0; i < batches; i++ {
		if err = stream.Send(batchOf(fmt.Sprintf("b%d", i), service, int64(i*perID), perID)); err != nil {
			break
		}
	}

	// Wait until a decent number of batches are acknowledged, then kill with those
	// batches still unwritten.
	waitFor(t, 30*time.Second, "batches to be acknowledged", func() bool {
		return promised.batches() >= batches/2
	})

	first.kill(t)
	<-done

	acked := promised.snapshot()
	require.NotEmpty(t, acked, "no batch was acknowledged, so the test proves nothing")
	t.Logf("acknowledged %d batches (%d records) before the kill", promised.batches(), len(acked))

	// Almost nothing should have been written yet, or the slow-writer setup did not
	// produce the window this test needs.
	t.Logf("rows present immediately after the kill: %d", countRows(ctx, t, pool, service))

	// A replacement collector binds to the same durable consumer and drains the
	// backlog. Its writer flushes promptly.
	recoveryOpt := opt
	recoveryOpt.writerBatchSize = 500
	recoveryOpt.writerFlush = 100 * time.Millisecond
	second := startCollector(t, recoveryOpt, nil)
	defer second.stop(t)

	// Redelivery waits for AckWait to expire on the messages the dead collector held.
	waitFor(t, 90*time.Second, "every acknowledged record to be recovered", func() bool {
		present := seqsPresent(ctx, t, pool, service)
		for seq := range acked {
			if !present[seq] {
				return false
			}
		}
		return true
	})

	present := seqsPresent(ctx, t, pool, service)
	missing := make([]int64, 0)
	for seq := range acked {
		if !present[seq] {
			missing = append(missing, seq)
		}
	}
	require.Empty(t, missing, "records were acknowledged and then lost")

	// Redelivery duplicates rows at the storage layer; the dedup index is what keeps
	// that from being visible. This asserts the claim in the README: at-least-once
	// delivery, deduplicated in storage.
	var duplicates int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT count(*) FROM (
			SELECT stream_id, seq, time FROM logs GROUP BY stream_id, seq, time HAVING count(*) > 1
		) dupes`).Scan(&duplicates))
	require.Zero(t, duplicates, "the dedup index let a redelivered record through twice")
}

// Overload must produce visible shedding rather than unbounded memory growth, which
// is the third of the phase's exit criteria.
func TestOverloadShedsInsteadOfGrowing(t *testing.T) {
	t.Parallel()

	_, opt := migratedDBWithStream(t)
	const service = "ingest-overload"

	// A one-slot buffer with a single publisher: with enough concurrent senders,
	// somebody has to be turned away.
	opt.bufferSize = 1
	opt.publishWorkers = 1

	c := startCollector(t, opt, nil)
	defer c.stop(t)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	const senders = 24
	overloaded := make(chan struct{}, senders)
	stop := make(chan struct{})
	defer close(stop)

	for i := 0; i < senders; i++ {
		go func(id int) {
			client, err := ingest.Dial(ctx, ingest.ClientConfig{Addr: c.addr, DialTimeout: 10 * time.Second})
			if err != nil {
				return
			}
			defer func() { _ = client.Close() }()

			stream, err := client.Stream(ctx)
			if err != nil {
				return
			}
			go func() {
				for {
					ack, recvErr := stream.Recv()
					if recvErr != nil {
						return
					}
					if ack.GetCode() == logaggv1.AckCode_ACK_CODE_OVERLOADED {
						select {
						case overloaded <- struct{}{}:
						default:
						}
					}
				}
			}()

			for seq := int64(0); ; seq += 200 {
				select {
				case <-stop:
					return
				default:
				}
				batch := batchOf(fmt.Sprintf("s%d-%d", id, seq), service, seq, 200)
				if err = stream.Send(batch); err != nil {
					return
				}
			}
		}(i)
	}

	select {
	case <-overloaded:
		// Shedding happened, and it was reported in band: the stream is still open
		// and the agent was told to slow down rather than being disconnected.
	case <-ctx.Done():
		require.Fail(t, "no batch was shed under overload; backpressure is not reaching the client")
	}
}

// A stream that ends cleanly must be reported as a clean end, not as a client that
// vanished — otherwise every well-behaved agent shows up as an error in the logs.
func TestClosingAStreamEndsItCleanly(t *testing.T) {
	t.Parallel()

	_, opt := migratedDBWithStream(t)
	c := startCollector(t, opt, nil)
	defer c.stop(t)

	client := dial(t, c.addr)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stream, err := client.Stream(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(batchOf("b0", "ingest-clean-close", 0, 5)))

	ack, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, logaggv1.AckCode_ACK_CODE_ACCEPTED, ack.GetCode(), ack.GetDetail())

	require.NoError(t, stream.CloseSend())
	_, err = stream.Recv()
	require.True(t, errors.Is(err, io.EOF), "want io.EOF after CloseSend, got %v", err)
}

// promises records which record sequence numbers the collector acknowledged.
type promises struct {
	mu      sync.Mutex
	seqs    map[int64]bool
	batchen int
}

func (p *promises) add(batchIdx, perBatch int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := 0; i < perBatch; i++ {
		p.seqs[int64(batchIdx*perBatch+i)] = true
	}
	p.batchen++
}

func (p *promises) batches() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.batchen
}

func (p *promises) snapshot() map[int64]bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[int64]bool, len(p.seqs))
	for seq := range p.seqs {
		out[seq] = true
	}
	return out
}
