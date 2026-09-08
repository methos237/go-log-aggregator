// Command loadgen pushes synthetic log records into a collector.
//
// This is the minimal version: enough traffic to drive the ingest path end to end
// and to make backpressure visible. The configurable rate, cardinality and message
// size version belongs to phase 8, along with the published numbers — so treat any
// throughput this prints as a smoke-test signal, not a benchmark.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	logaggv1 "github.com/jamespolk/go-log-aggregator/api/proto/logagg/v1"
	"github.com/jamespolk/go-log-aggregator/internal/ingest"
	"github.com/jamespolk/go-log-aggregator/internal/version"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "loadgen: %v\n", err)
		os.Exit(1)
	}
}

// options are the knobs this version exposes.
type options struct {
	addr        string
	records     int
	batchSize   int
	streams     int
	senders     int
	messageSize int
	env         string
	certFile    string
	keyFile     string
	caFile      string
}

func run() error {
	var opt options
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.StringVar(&opt.addr, "addr", "127.0.0.1:9095", "collector ingest address")
	flag.IntVar(&opt.records, "records", 100000, "total records to send")
	flag.IntVar(&opt.batchSize, "batch-size", 500, "records per batch")
	flag.IntVar(&opt.streams, "streams", 8, "distinct label sets to spread records across")
	flag.IntVar(&opt.senders, "senders", 4, "concurrent streams to the collector")
	flag.IntVar(&opt.messageSize, "message-size", 120, "approximate bytes per log line")
	flag.StringVar(&opt.env, "env", "dev", "env label on generated records")
	flag.StringVar(&opt.certFile, "tls-cert", "", "client certificate (mTLS)")
	flag.StringVar(&opt.keyFile, "tls-key", "", "client key (mTLS)")
	flag.StringVar(&opt.caFile, "tls-ca", "", "CA that signed the collector certificate")
	flag.Parse()

	if *showVersion {
		fmt.Println(version.String("loadgen"))
		return nil
	}
	if err := opt.validate(); err != nil {
		return err
	}

	// Ctrl-C stops sending and still prints the tally, which is what makes this
	// usable for "run it until backpressure shows up and then look".
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	client, err := ingest.Dial(ctx, ingest.ClientConfig{
		Addr:     opt.addr,
		CertFile: opt.certFile,
		KeyFile:  opt.keyFile,
		CAFile:   opt.caFile,
	})
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()

	fmt.Printf("sending %d records in batches of %d across %d streams via %d senders to %s\n",
		opt.records, opt.batchSize, opt.streams, opt.senders, opt.addr)

	tally := &tally{}
	started := time.Now()
	if err = send(ctx, client, &opt, tally); err != nil {
		tally.report(time.Since(started))
		return err
	}
	tally.report(time.Since(started))

	// A non-zero exit on rejections makes this usable as a check rather than as
	// something whose output has to be read. Shed records are excluded: overload is
	// the backpressure chain working as designed, and failing the run for it would
	// make the correct behavior indistinguishable from a broken collector.
	if refused := tally.refused.Load(); refused > 0 {
		return fmt.Errorf("%d records were refused for reasons other than backpressure", refused)
	}
	return nil
}

func (o *options) validate() error {
	var errs []error
	if o.records < 1 {
		errs = append(errs, fmt.Errorf("records must be at least 1, got %d", o.records))
	}
	if o.batchSize < 1 {
		errs = append(errs, fmt.Errorf("batch size must be at least 1, got %d", o.batchSize))
	}
	if o.streams < 1 {
		errs = append(errs, fmt.Errorf("streams must be at least 1, got %d", o.streams))
	}
	if o.senders < 1 {
		errs = append(errs, fmt.Errorf("senders must be at least 1, got %d", o.senders))
	}
	if o.messageSize < 1 {
		errs = append(errs, fmt.Errorf("message size must be at least 1, got %d", o.messageSize))
	}
	return errors.Join(errs...)
}

// send fans the work out across senders, each on its own gRPC stream.
func send(ctx context.Context, client *ingest.Client, opt *options, t *tally) error {
	batches := (opt.records + opt.batchSize - 1) / opt.batchSize

	// Handed out atomically rather than sliced up front, so a sender that hits
	// backpressure does not leave its share unsent while others finish early.
	var next atomic.Int64

	var wg sync.WaitGroup
	errs := make([]error, opt.senders)
	for i := 0; i < opt.senders; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			errs[id] = (&sender{
				id:     id,
				opt:    opt,
				tally:  t,
				next:   &next,
				total:  int64(batches),
				client: client,
			}).run(ctx)
		}(i)
	}
	wg.Wait()

	// context.Canceled is the Ctrl-C path, which is a normal way to stop.
	filtered := make([]error, 0, len(errs))
	for _, err := range errs {
		if err != nil && !errors.Is(err, context.Canceled) {
			filtered = append(filtered, err)
		}
	}
	return errors.Join(filtered...)
}

// sender owns one gRPC stream and pipelines batches on it.
type sender struct {
	id     int
	opt    *options
	tally  *tally
	next   *atomic.Int64
	total  int64
	client *ingest.Client
}

// run sends batches and reads acks concurrently.
//
// The two must be concurrent, not sequential. gRPC flow control means the server
// blocks writing acks once the client stops reading them, and a blocked server stops
// reading batches, so a send-everything-then-read-everything client deadlocks as
// soon as the ack window fills. Reading in a second goroutine also means an ack
// waits for a JetStream publish without stalling the next batch, so the number this
// prints is throughput rather than round-trip latency.
func (s *sender) run(ctx context.Context) error {
	stream, err := s.client.Stream(ctx)
	if err != nil {
		return err
	}

	// The reader ends when the server closes the stream, which happens after
	// CloseSend below, so no expected count has to be communicated between them.
	acks := make(chan error, 1)
	go func() { acks <- s.readAcks(stream) }()

	for {
		i := s.next.Add(1) - 1
		if i >= s.total || ctx.Err() != nil {
			break
		}
		if err = stream.Send(s.batch(i)); err != nil {
			// Usually stale: the real reason is the status waiting on Recv.
			break
		}
	}

	if err = stream.CloseSend(); err != nil {
		return fmt.Errorf("sender %d: close send: %w", s.id, err)
	}
	if ackErr := <-acks; ackErr != nil {
		return fmt.Errorf("sender %d: %w", s.id, ackErr)
	}
	return nil
}

// readAcks tallies acks until the server ends the stream.
func (s *sender) readAcks(stream *ingest.Stream) error {
	for {
		ack, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			// Clean end: the server saw CloseSend and answered everything.
			return nil
		}
		if err != nil {
			return fmt.Errorf("await ack: %w", err)
		}
		s.tally.record(ack)
	}
}

// batch builds batch i. Stream assignment is by batch rather than by record, so
// every batch carries exactly one label set — which is what the wire format is for.
func (s *sender) batch(i int64) *logaggv1.LogBatch {
	streamIdx := int(i) % s.opt.streams
	now := time.Now()

	records := make([]*logaggv1.LogRecord, 0, s.opt.batchSize)
	for j := 0; j < s.opt.batchSize; j++ {
		// Sequence numbers are unique per (stream, batch) so redelivered batches
		// deduplicate on replay instead of appearing as new records.
		seq := i*int64(s.opt.batchSize) + int64(j)
		records = append(records, &logaggv1.LogRecord{
			TimeUnixNano: now.Add(time.Duration(j) * time.Microsecond).UnixNano(),
			Seq:          seq,
			Level:        levels[int(seq)%len(levels)],
			Message:      message(seq, s.opt.messageSize),
		})
	}

	return &logaggv1.LogBatch{
		BatchId: fmt.Sprintf("loadgen-%d-%d", s.id, i),
		Labels: &logaggv1.LabelSet{
			Service: fmt.Sprintf("loadgen-svc-%d", streamIdx),
			Host:    fmt.Sprintf("loadgen-host-%d", streamIdx),
			Env:     s.opt.env,
		},
		Records: records,
	}
}

// levels are cycled so the level column and its index see a realistic spread rather
// than a single value that compresses away to nothing.
var levels = []logaggv1.Level{
	logaggv1.Level_LEVEL_DEBUG,
	logaggv1.Level_LEVEL_INFO,
	logaggv1.Level_LEVEL_INFO,
	logaggv1.Level_LEVEL_INFO,
	logaggv1.Level_LEVEL_WARN,
	logaggv1.Level_LEVEL_ERROR,
}

// filler is padding for generated messages. Not random: random bytes would defeat
// the column compression this system relies on and make any size comparison a
// measurement of the compressor rather than of the write path.
const filler = "lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor "

func message(seq int64, size int) string {
	prefix := fmt.Sprintf("synthetic record seq=%d ", seq)
	if len(prefix) >= size {
		return prefix
	}
	out := make([]byte, 0, size)
	out = append(out, prefix...)
	for len(out) < size {
		out = append(out, filler...)
	}
	return string(out[:size])
}

// tally counts acks by code.
type tally struct {
	accepted atomic.Int64
	rejected atomic.Int64
	// shed and refused split the rejected records by whether the collector was
	// applying backpressure or actually failing, because only the second is a reason
	// for this tool to exit non-zero.
	shed       atomic.Int64
	refused    atomic.Int64
	overloaded atomic.Int64
	invalid    atomic.Int64
	internal   atomic.Int64
	batches    atomic.Int64
}

func (t *tally) record(ack *logaggv1.Ack) {
	t.batches.Add(1)
	t.accepted.Add(int64(ack.GetAccepted()))
	t.rejected.Add(int64(ack.GetRejected()))

	switch ack.GetCode() {
	case logaggv1.AckCode_ACK_CODE_ACCEPTED:
	case logaggv1.AckCode_ACK_CODE_OVERLOADED:
		t.overloaded.Add(1)
		t.shed.Add(int64(ack.GetRejected()))
	case logaggv1.AckCode_ACK_CODE_INVALID:
		t.invalid.Add(1)
		t.refused.Add(int64(ack.GetRejected()))
	case logaggv1.AckCode_ACK_CODE_INTERNAL, logaggv1.AckCode_ACK_CODE_UNSPECIFIED:
		t.internal.Add(1)
		t.refused.Add(int64(ack.GetRejected()))
	}
}

func (t *tally) report(took time.Duration) {
	accepted := t.accepted.Load()
	rate := float64(accepted) / took.Seconds()

	fmt.Printf("\n%d batches acked in %s\n", t.batches.Load(), took.Round(time.Millisecond))
	fmt.Printf("  accepted  %d records (%.0f/sec)\n", accepted, rate)
	fmt.Printf("  rejected  %d records (%d shed under backpressure, %d refused)\n",
		t.rejected.Load(), t.shed.Load(), t.refused.Load())
	// Overloaded is not a failure of this tool, it is the backpressure chain
	// working, so it is reported separately from genuine errors.
	fmt.Printf("  acks      overloaded=%d invalid=%d internal=%d\n",
		t.overloaded.Load(), t.invalid.Load(), t.internal.Load())
}
