// Command loadgen pushes synthetic log records into a collector.
//
// A run is reproducible from its flags alone. The send schedule is a pure function
// of the batch index (see schedule), and every message is a pure function of its
// sequence number (see message), so two runs with the same flags put the same bytes
// on the wire on the same timeline. That is what makes a before/after comparison in
// docs/benchmarks/ a comparison of the collector and not of the load.
//
// Two ways to bound a run: -records sends a fixed count as fast as the collector
// accepts it (or at -rate), which is the throughput measurement; -duration sends at
// -rate for a fixed time, which is the latency measurement, since ack latency only
// means something when the client is not saturating the link.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"os"
	"os/signal"
	"slices"
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

// options are the knobs this tool exposes.
type options struct {
	addr        string
	records     int
	batchSize   int
	streams     int
	senders     int
	messageSize int
	rate        int
	duration    time.Duration
	ramp        time.Duration
	env         string
	out         string
	certFile    string
	keyFile     string
	caFile      string
}

func run() error {
	var opt options
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.StringVar(&opt.addr, "addr", "127.0.0.1:9095", "collector ingest address")
	flag.IntVar(&opt.records, "records", 100000, "total records to send (ignored when -duration is set)")
	flag.IntVar(&opt.batchSize, "batch-size", 500, "records per batch")
	flag.IntVar(&opt.streams, "streams", 8, "distinct label sets to spread records across")
	flag.IntVar(&opt.senders, "senders", 4, "concurrent streams to the collector")
	flag.IntVar(&opt.messageSize, "message-size", 120, "approximate bytes per log line")
	flag.IntVar(&opt.rate, "rate", 0, "target records/second across all senders; 0 sends as fast as the collector accepts")
	flag.DurationVar(&opt.duration, "duration", 0, "send for this long instead of a fixed record count")
	flag.DurationVar(&opt.ramp, "ramp", 0, "grow the rate linearly from zero to -rate over this long")
	flag.StringVar(&opt.env, "env", "dev", "env label on generated records")
	flag.StringVar(&opt.out, "out", "", "write the run summary as JSON to this file")
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

	fmt.Println(opt.describe())

	tally := &tally{}
	started := time.Now()
	sendErr := send(ctx, client, &opt, tally)
	sum := tally.summarize(&opt, time.Since(started))
	sum.print()
	if opt.out != "" {
		if err := sum.write(opt.out); err != nil {
			return errors.Join(sendErr, err)
		}
	}
	if sendErr != nil {
		return sendErr
	}

	// A non-zero exit on rejections makes this usable as a check rather than as
	// something whose output has to be read. Shed records are excluded: overload is
	// the backpressure chain working as designed, and failing the run for it would
	// make the correct behavior indistinguishable from a broken collector.
	if sum.Refused > 0 {
		return fmt.Errorf("%d records were refused for reasons other than backpressure", sum.Refused)
	}
	return nil
}

func (o *options) validate() error {
	var errs []error
	for _, f := range []struct {
		name  string
		value int
	}{
		{"records", o.records},
		{"batch size", o.batchSize},
		{"streams", o.streams},
		{"senders", o.senders},
		{"message size", o.messageSize},
	} {
		if f.value < 1 {
			errs = append(errs, fmt.Errorf("%s must be at least 1, got %d", f.name, f.value))
		}
	}
	if o.rate < 0 {
		errs = append(errs, fmt.Errorf("rate must not be negative, got %d", o.rate))
	}
	if o.duration < 0 {
		errs = append(errs, fmt.Errorf("duration must not be negative, got %s", o.duration))
	}
	if o.ramp < 0 {
		errs = append(errs, fmt.Errorf("ramp must not be negative, got %s", o.ramp))
	}
	if o.ramp > 0 && o.rate == 0 {
		errs = append(errs, errors.New("ramp needs a target rate: set -rate"))
	}
	if o.duration > 0 && o.rate == 0 {
		// Unbounded rate for a fixed time is a valid stress test, but it is not
		// what -duration is for, and silently producing meaningless latency numbers
		// is worse than asking.
		errs = append(errs, errors.New("duration needs a target rate: set -rate, or use -records for an unpaced run"))
	}
	return errors.Join(errs...)
}

func (o *options) describe() string {
	pace := "unpaced"
	if o.rate > 0 {
		pace = fmt.Sprintf("%d records/s", o.rate)
		if o.ramp > 0 {
			pace += fmt.Sprintf(" after a %s ramp", o.ramp)
		}
	}
	bound := fmt.Sprintf("%d records", o.records)
	if o.duration > 0 {
		bound = fmt.Sprintf("for %s", o.duration)
	}
	return fmt.Sprintf("sending %s in batches of %d across %d streams via %d senders to %s, %s",
		bound, o.batchSize, o.streams, o.senders, o.addr, pace)
}

// schedule is the pure function from "records issued before this batch" to the
// offset from the start at which the batch may be sent. Pure, so every sender
// computes it independently with no shared limiter state, and so a run's timeline
// is a property of its flags rather than of scheduling luck.
type schedule struct {
	rate float64 // records per second; 0 means unpaced
	ramp time.Duration
}

func (p schedule) releaseAt(records int64) time.Duration {
	if p.rate <= 0 {
		return 0
	}
	n := float64(records)
	ramp := p.ramp.Seconds()
	// During the ramp the rate grows linearly, so records accumulate quadratically:
	// n = rate * t² / (2 * ramp). Solving for t gives the release time. Past the
	// ramp the rate is constant and the remaining records are spaced evenly.
	rampRecords := p.rate * ramp / 2
	if n < rampRecords {
		return time.Duration(math.Sqrt(2*n*ramp/p.rate) * float64(time.Second))
	}
	return time.Duration((ramp + (n-rampRecords)/p.rate) * float64(time.Second))
}

// send fans the work out across senders, each on its own gRPC stream.
func send(ctx context.Context, client *ingest.Client, opt *options, t *tally) error {
	total := int64((opt.records + opt.batchSize - 1) / opt.batchSize)

	// -duration bounds sending, not the stream: the stream stays on the parent
	// context so the acks for everything already sent are still read once the
	// senders stop. Canceling the stream instead would count in-flight batches as
	// lost when they were merely unawaited.
	sendCtx := ctx
	if opt.duration > 0 {
		total = math.MaxInt64
		var cancel context.CancelFunc
		sendCtx, cancel = context.WithTimeout(ctx, opt.duration)
		defer cancel()
	}

	// Handed out atomically rather than sliced up front, so a sender that hits
	// backpressure does not leave its share unsent while others finish early.
	var next atomic.Int64
	start := time.Now()
	sched := schedule{rate: float64(opt.rate), ramp: opt.ramp}

	var wg sync.WaitGroup
	errs := make([]error, opt.senders)
	for i := 0; i < opt.senders; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			errs[id] = (&sender{
				id:       id,
				opt:      opt,
				tally:    t,
				next:     &next,
				total:    total,
				start:    start,
				sched:    sched,
				client:   client,
				inflight: make(map[string]time.Time),
			}).run(ctx, sendCtx)
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
	start  time.Time
	sched  schedule
	client *ingest.Client

	// inflight maps a batch ID to when it was sent, so the ack reader can turn each
	// ack into a latency. Batch IDs rather than positions because nothing in the
	// protocol promises acks arrive in send order.
	mu       sync.Mutex
	inflight map[string]time.Time
}

// run sends batches and reads acks concurrently.
//
// The two must be concurrent, not sequential. gRPC flow control means the server
// blocks writing acks once the client stops reading them, and a blocked server stops
// reading batches, so a send-everything-then-read-everything client deadlocks as
// soon as the ack window fills. Reading in a second goroutine also means an ack
// waits for a JetStream publish without stalling the next batch, so the number this
// prints is throughput rather than round-trip latency.
func (s *sender) run(ctx, sendCtx context.Context) error {
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
		if i >= s.total || s.pace(sendCtx, i) != nil {
			break
		}
		batch := s.batch(i)
		s.mu.Lock()
		s.inflight[batch.GetBatchId()] = time.Now()
		s.mu.Unlock()
		if err = stream.Send(batch); err != nil {
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

// pace blocks until batch i's release time, or until sending is stopped.
func (s *sender) pace(ctx context.Context, i int64) error {
	at := s.start.Add(s.sched.releaseAt(i * int64(s.opt.batchSize)))
	wait := time.Until(at)
	if wait <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
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
		now := time.Now()
		s.mu.Lock()
		sent, ok := s.inflight[ack.GetBatchId()]
		delete(s.inflight, ack.GetBatchId())
		s.mu.Unlock()
		s.tally.record(ack)
		if ok {
			s.tally.observe(now.Sub(sent))
		}
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

// vocabulary is what generated messages are built from. Words are drawn with a
// Zipf distribution, so a handful appear in nearly every record and the tail is
// rare — the shape real logs have, and the shape a substring-search benchmark needs:
// a common needle has to match most rows and a rare one few, or the comparison of
// ILIKE against a trigram index (roadmap §2.4) measures nothing.
//
// Not random bytes, and not a fixed filler either. Random bytes would defeat the
// column compression this system relies on and turn any size comparison into a
// measurement of the compressor; a fixed filler makes every row match every search.
var vocabulary = []string{
	"request", "handled", "in", "ms", "for", "user", "session", "cache", "hit", "miss",
	"retry", "upstream", "connection", "timeout", "queue", "depth", "flush", "batch",
	"committed", "rows", "stream", "ack", "publish", "consumer", "redelivery", "chunk",
	"compress", "index", "scan", "plan", "latency", "p99", "budget", "throttle", "shed",
	"overload", "backoff", "jitter", "lease", "renew", "expired", "token", "refresh",
	"rotate", "checkpoint", "spool", "cursor", "offset", "gap", "duplicate", "dedup",
	"conflict", "deadlock", "panic", "recovered", "shutdown", "drain", "healthy",
	"degraded", "unreachable", "refused", "reset", "eof", "corrupt",
}

// message renders the record with sequence number seq at roughly size bytes. It is
// deterministic in seq, so a replayed or regenerated record is byte-identical.
func message(seq int64, size int) string {
	prefix := fmt.Sprintf("synthetic record seq=%d", seq)
	if len(prefix) >= size {
		return prefix
	}
	r := rand.New(rand.NewPCG(uint64(seq), 0x6c6f6761)) //nolint:gosec // synthetic text, not a secret
	words := rand.NewZipf(r, 1.1, 1, uint64(len(vocabulary)-1))
	out := make([]byte, 0, size+16)
	out = append(out, prefix...)
	for len(out) < size {
		out = append(out, ' ')
		out = append(out, vocabulary[words.Uint64()]...)
	}
	return string(out[:size])
}

// tally counts acks by code and collects ack latencies.
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

	mu        sync.Mutex
	latencies []time.Duration
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

func (t *tally) observe(d time.Duration) {
	t.mu.Lock()
	t.latencies = append(t.latencies, d)
	t.mu.Unlock()
}

// summary is what a run reports, printed for a human and written as JSON for the
// benchmark harness. Durations are in the unit their name says, so the JSON needs
// no reader-side conversion.
type summary struct {
	Addr        string  `json:"addr"`
	Records     int     `json:"records"`
	BatchSize   int     `json:"batch_size"`
	Streams     int     `json:"streams"`
	Senders     int     `json:"senders"`
	MessageSize int     `json:"message_size"`
	Rate        int     `json:"rate"`
	DurationSec float64 `json:"duration_sec"`
	RampSec     float64 `json:"ramp_sec"`

	TookSec       float64 `json:"took_sec"`
	Batches       int64   `json:"batches"`
	Accepted      int64   `json:"accepted"`
	Rejected      int64   `json:"rejected"`
	Shed          int64   `json:"shed"`
	Refused       int64   `json:"refused"`
	Overloaded    int64   `json:"acks_overloaded"`
	Invalid       int64   `json:"acks_invalid"`
	Internal      int64   `json:"acks_internal"`
	RecordsPerSec float64 `json:"records_per_sec"`
	AckP50Ms      float64 `json:"ack_p50_ms"`
	AckP95Ms      float64 `json:"ack_p95_ms"`
	AckP99Ms      float64 `json:"ack_p99_ms"`
	AckMaxMs      float64 `json:"ack_max_ms"`
}

func (t *tally) summarize(opt *options, took time.Duration) summary {
	t.mu.Lock()
	slices.Sort(t.latencies)
	lat := t.latencies
	t.mu.Unlock()

	accepted := t.accepted.Load()
	records := opt.records
	if opt.duration > 0 {
		records = 0 // -records is ignored under -duration; do not report a bound that was not applied
	}
	return summary{
		Addr:        opt.addr,
		Records:     records,
		BatchSize:   opt.batchSize,
		Streams:     opt.streams,
		Senders:     opt.senders,
		MessageSize: opt.messageSize,
		Rate:        opt.rate,
		DurationSec: opt.duration.Seconds(),
		RampSec:     opt.ramp.Seconds(),

		TookSec:       took.Seconds(),
		Batches:       t.batches.Load(),
		Accepted:      accepted,
		Rejected:      t.rejected.Load(),
		Shed:          t.shed.Load(),
		Refused:       t.refused.Load(),
		Overloaded:    t.overloaded.Load(),
		Invalid:       t.invalid.Load(),
		Internal:      t.internal.Load(),
		RecordsPerSec: float64(accepted) / took.Seconds(),
		AckP50Ms:      ms(percentile(lat, 0.50)),
		AckP95Ms:      ms(percentile(lat, 0.95)),
		AckP99Ms:      ms(percentile(lat, 0.99)),
		AckMaxMs:      ms(percentile(lat, 1)),
	}
}

// percentile is the nearest-rank percentile of an already sorted slice.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(p*float64(len(sorted)))) - 1
	return sorted[max(0, min(rank, len(sorted)-1))]
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func (s *summary) print() {
	fmt.Printf("\n%d batches acked in %.3fs\n", s.Batches, s.TookSec)
	fmt.Printf("  accepted  %d records (%.0f/sec)\n", s.Accepted, s.RecordsPerSec)
	fmt.Printf("  rejected  %d records (%d shed under backpressure, %d refused)\n",
		s.Rejected, s.Shed, s.Refused)
	// Overloaded is not a failure of this tool, it is the backpressure chain
	// working, so it is reported separately from genuine errors.
	fmt.Printf("  acks      overloaded=%d invalid=%d internal=%d\n", s.Overloaded, s.Invalid, s.Internal)
	// Ack latency is send-to-ack at the client: ingest plus the JetStream publish,
	// not the write to the database. Under an unpaced run it mostly measures
	// queueing in gRPC flow control, which is why -duration insists on -rate.
	fmt.Printf("  ack lat   p50=%.1fms p95=%.1fms p99=%.1fms max=%.1fms\n",
		s.AckP50Ms, s.AckP95Ms, s.AckP99Ms, s.AckMaxMs)
}

func (s *summary) write(path string) error {
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, append(b, '\n'), 0o644); err != nil { //nolint:gosec // a report, not a secret
		return fmt.Errorf("write summary: %w", err)
	}
	return nil
}
