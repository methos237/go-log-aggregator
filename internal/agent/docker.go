package agent

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/client"

	"github.com/jamespolk/go-log-aggregator/internal/backoff"
)

// DockerStream selects which of a container's output streams a source
// reads. Docker multiplexes stdout and stderr over one connection to the
// daemon, but this source deliberately does not follow that lead and demux
// both into one Source: see NewDockerSource's doc comment for why. Every
// DockerSource therefore names exactly one of the two constants below.
type DockerStream string

// The two streams a container can produce. There is no "both" -- see
// DockerStream's doc comment.
const (
	DockerStdout DockerStream = "stdout"
	DockerStderr DockerStream = "stderr"
)

// DockerConfig configures a DockerSource.
type DockerConfig struct {
	// Container is the container ID or name Docker's API accepts. It is
	// also embedded verbatim in Name(), so whatever the operator wrote here
	// is what stays stable across restarts -- Docker's own ID resolution is
	// never consulted for that purpose, the same reasoning the tail source
	// applies to its configured path.
	Container string
	// Stream selects stdout or stderr. NewDockerSource rejects anything
	// other than the two DockerStream constants; there is no default.
	Stream DockerStream
	// Client is the Docker API client to use. Nil builds one from the
	// environment (DOCKER_HOST and friends via client.FromEnv). Set this in
	// tests to a fake implementing DockerClient. A caller that sets this in
	// production keeps ownership of it -- see DockerSource.Close.
	Client DockerClient
	// Resume is where to pick the stream back up. Its Offset is the Unix
	// nanosecond instant to resume after (see Cursor's field docs on this
	// source's line and the DockerSource.Run doc comment on the duplicates
	// resuming can produce). The zero Cursor starts from the beginning of
	// whatever log history the daemon has retained for the container.
	Resume Cursor
	// Metrics records timestamp fallbacks and reconnects. Nil builds an
	// unregistered set via NewMetrics(nil), which is what tests want;
	// production wiring supplies one built against the real registry.
	Metrics *Metrics
	// Logger reports connect failures at warn level, including the
	// container and the error. Nil means no logging, which is what tests
	// want; production wiring supplies the process logger. A permanently
	// wrong container name would otherwise retry forever in total silence —
	// no logger reaches a Source anywhere else in this package, since
	// reconnects is normally the visible signal, but a container that never
	// existed is a configuration error the operator needs to see directly.
	Logger *slog.Logger
}

// DockerClient is the slice of the Docker API this source needs, so tests
// can substitute a fake instead of requiring a live daemon.
//
// *client.Client (github.com/moby/moby/client v0.5.0 -- the module this
// project already carries indirectly, pulled in by testcontainers-go; see
// go.mod) satisfies this interface directly, with no wrapper, because the
// method below is copied from its signature verbatim, options type and
// result type included. Do not "simplify" ContainerLogs's result type to
// io.ReadCloser: client.ContainerLogsResult is a distinct named interface
// (it only happens to have the same method set), and Go's interface
// satisfaction is exact on declared types for this purpose only insofar as
// the method sets match -- but a real *client.Client method literally
// returns ContainerLogsResult, so this interface must ask for that same
// type or the real client stops satisfying it and a wrapper becomes
// necessary again.
type DockerClient interface {
	// ContainerLogs opens the log stream for one container. Options select
	// exactly one of ShowStdout/ShowStderr in this source's usage; see
	// connect.
	ContainerLogs(ctx context.Context, container string, options client.ContainerLogsOptions) (client.ContainerLogsResult, error)
	// Close releases the client's connections. Called only on a client this
	// source built itself; see DockerSource.Close.
	Close() error
}

// DockerSource streams one container's stdout or stderr as lines.
//
// It is one source per (container, stream), never one source demuxing both
// streams from a single connection. That demuxing design is the tempting
// one -- Docker already multiplexes stdout and stderr together, so a
// single source reading both looks like it saves a connection -- but it
// forces the two streams to share one cursor and one Name, and Line has no
// field to carry which stream a given message came from. The
// stdout/stderr distinction is real information an operator filters logs
// on, and collapsing it away to save one extra API connection (which
// Docker handles fine) is not a trade this package makes. Two independent
// sources instead give each stream its own Name and its own independently
// checkpointed Cursor, which is what Source exists for.
type DockerSource struct {
	container string
	stream    DockerStream

	client     DockerClient
	ownsClient bool

	resume Cursor

	// assembler is driven only inside readOnce's read goroutine, and only
	// one such goroutine is ever alive at a time: Run never starts a new
	// connection's goroutine until the previous one's lines channel has
	// closed, which happens only after that goroutine has stopped touching
	// assembler for good. So, like stdin.go's identical situation, no lock
	// is required here despite reconnects reusing the same assembler
	// across many goroutines over the life of one DockerSource.
	assembler *lineAssembler

	closeOnce sync.Once
	closeErr  error

	metrics *Metrics
	// logger reports connect failures; nil means no logging. See
	// DockerConfig.Logger.
	logger *slog.Logger
}

// NewDockerSource validates cfg and returns a DockerSource ready to Run.
//
// cfg.Client nil builds a client from the environment using the same
// negotiation client.New applies by default (see client.New's doc comment);
// that client is this source's to close in Close. A cfg.Client the caller
// set is never closed here -- see Close.
func NewDockerSource(cfg *DockerConfig) (*DockerSource, error) {
	if cfg == nil {
		return nil, errors.New("docker source: nil config")
	}
	if cfg.Container == "" {
		return nil, errors.New("docker source: empty container")
	}
	switch cfg.Stream {
	case DockerStdout, DockerStderr:
	default:
		return nil, fmt.Errorf("docker source: invalid stream %q, want %q or %q", cfg.Stream, DockerStdout, DockerStderr)
	}

	cli := cfg.Client
	ownsClient := false
	if cli == nil {
		c, err := client.New(client.FromEnv)
		if err != nil {
			return nil, fmt.Errorf("docker source: build client from environment: %w", err)
		}
		cli = c
		ownsClient = true
	}

	metrics := cfg.Metrics
	if metrics == nil {
		metrics = NewMetrics(nil)
	}

	return &DockerSource{
		container:  cfg.Container,
		stream:     cfg.Stream,
		client:     cli,
		ownsClient: ownsClient,
		resume:     cfg.Resume,
		assembler:  newLineAssembler(),
		metrics:    metrics,
		logger:     cfg.Logger,
	}, nil
}

// Name returns "docker/<container>/<stream>", the checkpoint key and the
// source label on metrics -- see Source's doc comment. It is built from the
// configured container, never from any ID or name Docker resolves it to.
func (s *DockerSource) Name() string {
	return fmt.Sprintf("docker/%s/%s", s.container, s.stream)
}

// Run streams the container's chosen output stream until ctx is canceled.
//
// A container that stops, restarts, or a connection the daemon drops is not
// treated as a clean end of input the way stdin's true EOF is: Run
// reconnects with backoff instead of returning, because a restarting
// container is the ordinary case this source exists to survive, not an
// error. Run therefore returns only ctx.Err(); it never returns nil.
//
// Each reconnect (whether the very first connection resuming from
// cfg.Resume, or a later one resuming from the last line this run of Run
// saw) asks Docker for logs Since the most recent line's timestamp. Docker's
// Since is inclusive at second granularity on some daemon versions, so a
// reconnect can redeliver the last line or two already emitted before the
// interruption. That is an accepted, bounded duplicate, not a bug: storage
// dedups on (stream_id, seq, time), and this source's Start and Offset
// reproduce exactly from the same timestamp on replay, which is exactly what
// makes that dedup work (see the Cursor fields set in toLine). Do not try to
// close this gap by dropping a line whose timestamp is <= the last one
// seen -- that trades a bounded, already-absorbed duplicate for the risk of
// silently dropping a genuinely distinct line that happens to share a
// timestamp with the one before it, which is a worse failure than the
// duplicate it would prevent.
func (s *DockerSource) Run(ctx context.Context, out chan<- Line) error {
	since := s.resume.Offset

	attempt := 0
	for {
		newSince, connected, err := s.readOnce(ctx, out, since)
		since = newSince

		if ctx.Err() != nil {
			return ctx.Err()
		}

		if err != nil && !connected && s.logger != nil {
			// A connect failure, not an ordinary disconnect after a stream
			// that worked for a while: see DockerConfig.Logger's doc comment
			// on why this is the one case in this package worth logging.
			s.logger.Warn("docker source: connect failed",
				slog.String("container", s.container),
				slog.String("stream", string(s.stream)),
				slog.Any("error", err))
		}

		if connected {
			// This connection ran for a while before its stream ended, so
			// whatever caused it is unrelated to any earlier failure:
			// do not let backoff keep growing across unrelated incidents.
			attempt = 0
		}
		attempt++
		s.metrics.DockerReconnects.Inc()

		// Full jitter, so every DockerSource in an agent watching containers
		// on one host does not reconnect in lockstep after a daemon restart.
		if !backoff.Sleep(ctx, backoff.Delay(attempt, dockerBackoffBase, dockerBackoffMax)) {
			return ctx.Err()
		}
	}
}

// readOnce runs one connection to completion: it connects, reads and
// forwards lines until the stream ends or ctx is canceled, and reports the
// timestamp to resume from next plus whether the connection was ever
// established at all (connect's own failure counts as not connected, which
// is what lets Run tell "the daemon refused the connection" apart from "the
// connection worked and then the container went away" for backoff
// purposes).
func (s *DockerSource) readOnce(ctx context.Context, out chan<- Line, since int64) (newSince int64, connected bool, err error) {
	raw, err := s.connect(ctx, since)
	if err != nil {
		return since, false, err
	}

	done := make(chan struct{})
	defer close(done)
	defer func() { _ = raw.Close() }()

	lines := make(chan Line, lineHandoff)
	errCh := make(chan error, 1)
	go s.readLines(done, raw, lines, errCh)

	for {
		select {
		case line, ok := <-lines:
			if !ok {
				// readLines's deferred close(lines) is its last act before
				// returning, so by the time ok is false the goroutine has
				// already stopped touching s.assembler for good.
				return since, true, <-errCh
			}
			since = line.Cursor.Offset
			select {
			case out <- line:
			case <-ctx.Done():
				return since, true, ctx.Err()
			}
		case <-ctx.Done():
			return since, true, ctx.Err()
		}
	}
}

// connect opens one connection to the container's chosen stream via the
// Docker API and returns the raw response body.
//
// It deliberately does no more than that: detecting the stream's framing
// (see isFramedStream) means peeking at its first bytes, which blocks until
// the container actually says something. Doing that here, synchronously,
// before readOnce has set up its ctx-driven shutdown, would leave a
// container that has gone briefly quiet with nothing watching ctx at all --
// exactly the deadlock this design avoids by pushing every blocking read,
// framing peek included, onto readLines's goroutine instead. See readOnce
// and readLines.
func (s *DockerSource) connect(ctx context.Context, since int64) (io.ReadCloser, error) {
	opts := client.ContainerLogsOptions{
		ShowStdout: s.stream == DockerStdout,
		ShowStderr: s.stream == DockerStderr,
		Timestamps: true,
		Follow:     true,
	}
	if since > 0 {
		// RFC3339Nano is one of the formats the client's own Since parser
		// (github.com/moby/moby/client/internal/timestamp.GetTimestamp)
		// accepts; it converts it to the daemon's "sec.nsec" form itself.
		opts.Since = time.Unix(0, since).UTC().Format(time.RFC3339Nano)
	}

	result, err := s.client.ContainerLogs(ctx, s.container, opts)
	if err != nil {
		return nil, fmt.Errorf("docker source %s: container logs: %w", s.Name(), err)
	}
	return result, nil
}

// demux runs stdcopy.StdCopy against src, discarding whichever of
// stdout/stderr this source did not request (Docker's demultiplexed stream
// still carries only the frames for the stream requested via
// ShowStdout/ShowStderr, but StdCopy's signature always wants both
// destinations), and writes the requested stream's bytes to pw.
//
// pw.CloseWithError propagates src's terminal error (io.EOF included) to
// the paired *io.PipeReader's Read, which is what lets readLines see the
// same "stream ended" signal it would see reading a plain, unframed reader
// directly. A nil werr (the io.EOF case) closes the pipe cleanly, exactly
// like CloseWithError's documented behavior for a nil error.
func (s *DockerSource) demux(src io.Reader, pw *io.PipeWriter) {
	var werr error
	if s.stream == DockerStdout {
		_, werr = stdcopy.StdCopy(pw, io.Discard, src)
	} else {
		_, werr = stdcopy.StdCopy(io.Discard, pw, src)
	}
	_ = pw.CloseWithError(werr)
}

// isFramedStream peeks at the first 8 bytes of br -- without consuming them,
// so whichever branch connect takes still reads the stream from its true
// start -- to tell whether it carries stdcopy's multiplexing header.
func isFramedStream(br *bufio.Reader) bool {
	header, err := br.Peek(8)
	if err != nil {
		// Fewer than 8 bytes were ever produced before EOF or an error:
		// too little to carry a stdcopy header, and too little to matter --
		// whatever arrived is passed through as-is rather than guessed at.
		return false
	}
	return isStdcopyHeader(header)
}

// isStdcopyHeader reports whether b looks like the 8-byte frame header
// stdcopy.StdCopy expects: stream type, three zero bytes, then a 4-byte
// big-endian size (see stdcopy's package doc for the exact layout). Docker
// never tells this source directly whether a container's output is
// multiplexed, so this is a heuristic, not a certainty -- but this source
// always enables ContainerLogsOptions.Timestamps, so an unframed line's
// first byte is always an ASCII digit (the timestamp's leading year digit),
// never a byte in [0,3], which is what keeps a real timestamped line from
// ever being mistaken for a frame header.
func isStdcopyHeader(b []byte) bool {
	switch stdcopy.StdType(b[0]) {
	case stdcopy.Stdin, stdcopy.Stdout, stdcopy.Stderr, stdcopy.Systemerr:
	default:
		return false
	}
	return b[1] == 0 && b[2] == 0 && b[3] == 0
}

// readLines turns raw's bytes into Lines and sends them until the stream
// ends or errors, then reports that error on errCh and returns.
//
// This mirrors stdin.go's read goroutine, for the same reason: raw's Read
// blocks and does not unblock on context cancellation, so readOnce cannot do
// any of this on its own goroutine. It spawns this one instead and selects
// between the lines it sends and ctx.Done(), closing done and the
// connection to unblock this goroutine when it needs to stop -- see
// readOnce.
//
// Detecting the stream's framing happens here too, not in connect, because
// the peek it requires (see isFramedStream) is itself a blocking read on
// raw -- see connect's doc comment for why that must not happen anywhere
// outside this goroutine.
func (s *DockerSource) readLines(done <-chan struct{}, raw io.Reader, lines chan<- Line, errCh chan<- error) {
	defer close(lines)

	br := bufio.NewReader(raw)
	if isFramedStream(br) {
		pr, pw := io.Pipe()
		go s.demux(br, pw)
		// Closing the read end is what lets the demux goroutine exit, and it has
		// to happen on every return path including the done one. StdCopy parks in
		// pw.Write once nobody is reading, and closing raw does not release it:
		// it is blocked writing, not reading. Without this, every reconnect of a
		// framed stream leaks a goroutine for the life of the process.
		defer func() { _ = pr.Close() }()
		br = bufio.NewReader(pr)
	}

	for {
		line, consumed, _, err := s.assembler.next(br)
		// terminated is not consulted: there is no byte offset to resume
		// mid-line from here (see Run's doc comment on what Offset means
		// for this source), so holding a trailing unterminated fragment
		// across a reconnect would buy nothing -- the next connection
		// starts at a whole-line boundary Docker picks from a timestamp,
		// not by continuing this buffer. So, like stdin's true EOF, any
		// fragment held when this stream ends is emitted as-is.
		if consumed > 0 {
			l := s.toLine(line)
			select {
			case lines <- l:
			case <-done:
				return
			}
		}

		if err != nil {
			errCh <- err
			return
		}
	}
}

// toLine parses raw's leading Docker timestamp into a Line, stripping it
// from Bytes -- it is metadata, not part of the message (see Line's doc
// comment in source.go).
func (s *DockerSource) toLine(raw []byte) Line {
	rest, ts, ok := splitDockerTimestamp(raw)
	if !ok {
		// No parseable timestamp: never drop the line over it. Fall back to
		// observation time and count the fallback, so an operator can tell
		// a misconfigured log driver (or a daemon that ignored
		// Timestamps) from ordinary operation.
		ts = time.Now()
		s.metrics.DockerTimestampParseFailures.Inc()
		rest = raw
	}

	nanos := ts.UnixNano()
	return Line{
		Source: s.Name(),
		Time:   ts,
		Bytes:  rest,
		Cursor: Cursor{
			// Start equals Offset: there is no byte range to distinguish
			// them for a source addressed by timestamp instead of by file
			// position, and checkpoint.go's validateCursor permits Start ==
			// Offset, rejecting only Start > Offset.
			Start: nanos,
			// Offset means "resume after this instant" for this source,
			// not a byte position -- see Run's doc comment for what
			// resuming from it can redeliver.
			Offset: nanos,
			// File and Head stay zero: there is no backing file (see
			// FileID.IsZero and Fingerprint.IsZero in source.go).
		},
	}
}

// splitDockerTimestamp splits a Docker-timestamped log line into its
// message and parsed time. Docker's format, produced whenever
// ContainerLogsOptions.Timestamps is set (as this source always sets it),
// is exactly "<RFC3339Nano><space><message>" -- one space, no exceptions --
// so the first space is the only valid split point.
func splitDockerTimestamp(line []byte) (rest []byte, ts time.Time, ok bool) {
	idx := bytes.IndexByte(line, ' ')
	if idx < 0 {
		return line, time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339Nano, string(line[:idx]))
	if err != nil {
		return line, time.Time{}, false
	}
	return line[idx+1:], t, true
}

// dockerBackoffBase and dockerBackoffMax bound the exponential-with-jitter
// delay between reconnect attempts. Base is short because a container
// restart is often quick and this source should notice promptly; max is
// long enough that a daemon that is genuinely down stops being hammered.
const (
	dockerBackoffBase = 200 * time.Millisecond
	dockerBackoffMax  = 30 * time.Second
)

// Close closes the client this source built from the environment.
//
// It is idempotent and safe to call after Run has returned, or twice in a
// row. An injected client (cfg.Client set) is never closed here: Close has
// no way to know whether the caller shares that client with other sources
// or other code, and closing someone else's client out from under them is
// exactly the kind of surprise a Source must not spring.
func (s *DockerSource) Close() error {
	s.closeOnce.Do(func() {
		if s.ownsClient {
			s.closeErr = s.client.Close()
		}
	})
	return s.closeErr
}
