package agent

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/client"

	"github.com/jamespolk/go-log-aggregator/internal/model"
)

// fakeLogsResponse scripts one ContainerLogs call's result.
type fakeLogsResponse struct {
	rc  io.ReadCloser
	err error
}

// fakeDockerClient is a DockerClient that needs no daemon: it returns
// scripted responses in call order and records the options each call
// received, so tests can assert on both without a network round trip.
type fakeDockerClient struct {
	mu        sync.Mutex
	responses []fakeLogsResponse
	calls     []client.ContainerLogsOptions
	// calledCh, if non-nil, receives each call's options as it happens, so a
	// test can synchronize on "the fake was called" instead of sleeping.
	calledCh chan client.ContainerLogsOptions

	closeN   int
	closeErr error
}

func (f *fakeDockerClient) ContainerLogs(_ context.Context, _ string, options client.ContainerLogsOptions) (client.ContainerLogsResult, error) {
	f.mu.Lock()
	f.calls = append(f.calls, options)
	idx := len(f.calls) - 1
	var resp fakeLogsResponse
	haveResp := idx < len(f.responses)
	if haveResp {
		resp = f.responses[idx]
	}
	f.mu.Unlock()

	if f.calledCh != nil {
		f.calledCh <- options
	}

	if !haveResp {
		// Beyond the scripted responses: an already-ended, empty stream
		// rather than a block, so a source that reconnects more times than
		// a test scripted does not hang the test forever.
		return io.NopCloser(strings.NewReader("")), nil
	}
	if resp.err != nil {
		return nil, resp.err
	}
	return resp.rc, nil
}

func (f *fakeDockerClient) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closeN++
	return f.closeErr
}

func (f *fakeDockerClient) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// frame builds one stdcopy-framed message: the 8-byte header stdcopy.StdCopy
// expects, followed by payload.
func frame(streamType stdcopy.StdType, payload string) []byte {
	b := make([]byte, 8+len(payload))
	b[0] = byte(streamType)
	binary.BigEndian.PutUint32(b[4:8], uint32(len(payload)))
	copy(b[8:], payload)
	return b
}

// fixedTime is a moment whose nanosecond component has no trailing zero, so
// time.RFC3339Nano's zero-suppressing formatter always emits all 9
// fractional digits. That keeps a formatted timestamp's length predictable
// across tests that need to compute it (e.g. the truncation test).
func fixedTime() time.Time {
	return time.Date(2024, 1, 2, 3, 4, 5, 123456789, time.UTC)
}

// tsLine formats one Docker-style timestamped log line: "<RFC3339Nano>
// <message>\n", matching exactly what ContainerLogsOptions.Timestamps
// produces.
func tsLine(when time.Time, message string) string {
	return when.Format(time.RFC3339Nano) + " " + message + "\n"
}

func TestNewDockerSource_Validation(t *testing.T) {
	fake := &fakeDockerClient{}

	tests := []struct {
		name    string
		cfg     *DockerConfig
		wantErr bool
	}{
		{
			name:    "nil config",
			cfg:     nil,
			wantErr: true,
		},
		{
			name:    "empty container",
			cfg:     &DockerConfig{Container: "", Stream: DockerStdout, Client: fake},
			wantErr: true,
		},
		{
			name:    "empty stream",
			cfg:     &DockerConfig{Container: "web", Stream: "", Client: fake},
			wantErr: true,
		},
		{
			name:    "invalid stream",
			cfg:     &DockerConfig{Container: "web", Stream: DockerStream("combined"), Client: fake},
			wantErr: true,
		},
		{
			name:    "valid stdout",
			cfg:     &DockerConfig{Container: "web", Stream: DockerStdout, Client: fake},
			wantErr: false,
		},
		{
			name:    "valid stderr",
			cfg:     &DockerConfig{Container: "web", Stream: DockerStderr, Client: fake},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := NewDockerSource(tt.cfg)
			if tt.wantErr {
				if err == nil {
					t.Fatal("NewDockerSource() error = nil, want non-nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("NewDockerSource() error = %v, want nil", err)
			}
			if s == nil {
				t.Fatal("NewDockerSource() returned nil source with nil error")
			}
		})
	}
}

func TestDockerSource_Name(t *testing.T) {
	fake := &fakeDockerClient{}

	tests := []struct {
		stream DockerStream
		want   string
	}{
		{DockerStdout, "docker/myapp/stdout"},
		{DockerStderr, "docker/myapp/stderr"},
	}

	for _, tt := range tests {
		t.Run(string(tt.stream), func(t *testing.T) {
			s, err := NewDockerSource(&DockerConfig{Container: "myapp", Stream: tt.stream, Client: fake})
			if err != nil {
				t.Fatalf("NewDockerSource() error = %v", err)
			}
			if got := s.Name(); got != tt.want {
				t.Errorf("Name() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDockerSource_OwnsBuiltClient(t *testing.T) {
	fake := &fakeDockerClient{}

	injected, err := NewDockerSource(&DockerConfig{Container: "web", Stream: DockerStdout, Client: fake})
	if err != nil {
		t.Fatalf("NewDockerSource() error = %v", err)
	}
	if injected.ownsClient {
		t.Error("ownsClient = true for an injected client, want false")
	}

	// client.New only builds configuration -- it dials nothing -- so this
	// needs no daemon.
	built, err := NewDockerSource(&DockerConfig{Container: "web", Stream: DockerStdout})
	if err != nil {
		t.Fatalf("NewDockerSource() error = %v", err)
	}
	t.Cleanup(func() { _ = built.Close() })
	if !built.ownsClient {
		t.Error("ownsClient = false for a self-built client, want true")
	}
}

func TestDockerSource_RawFraming(t *testing.T) {
	base := fixedTime()
	line1 := tsLine(base, "first line")
	line2 := tsLine(base.Add(time.Second), "second line")

	fake := &fakeDockerClient{
		responses: []fakeLogsResponse{
			{rc: io.NopCloser(strings.NewReader(line1 + line2))},
		},
	}
	s, err := NewDockerSource(&DockerConfig{Container: "web", Stream: DockerStdout, Client: fake})
	if err != nil {
		t.Fatalf("NewDockerSource() error = %v", err)
	}
	defer func() { _ = s.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan Line, 4)
	errCh := make(chan error, 1)
	go func() { errCh <- s.Run(ctx, out) }()

	got1 := <-out
	got2 := <-out
	cancel()
	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Errorf("Run() error = %v, want context.Canceled", err)
	}

	if string(got1.Bytes) != "first line" {
		t.Errorf("line1.Bytes = %q, want %q", got1.Bytes, "first line")
	}
	if string(got2.Bytes) != "second line" {
		t.Errorf("line2.Bytes = %q, want %q", got2.Bytes, "second line")
	}
	if !got1.Time.Equal(base) {
		t.Errorf("line1.Time = %v, want %v", got1.Time, base)
	}
	if !got2.Time.Equal(base.Add(time.Second)) {
		t.Errorf("line2.Time = %v, want %v", got2.Time, base.Add(time.Second))
	}
}

func TestDockerSource_HeaderFramedStream(t *testing.T) {
	base := fixedTime()
	line1 := tsLine(base, "first line")
	line2 := tsLine(base.Add(time.Second), "second line")

	tests := []struct {
		name       string
		stream     DockerStream
		streamType stdcopy.StdType
	}{
		{"stdout", DockerStdout, stdcopy.Stdout},
		{"stderr", DockerStderr, stdcopy.Stderr},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var framed []byte
			framed = append(framed, frame(tt.streamType, line1)...)
			framed = append(framed, frame(tt.streamType, line2)...)

			fake := &fakeDockerClient{
				responses: []fakeLogsResponse{
					{rc: io.NopCloser(strings.NewReader(string(framed)))},
				},
			}
			s, err := NewDockerSource(&DockerConfig{Container: "web", Stream: tt.stream, Client: fake})
			if err != nil {
				t.Fatalf("NewDockerSource() error = %v", err)
			}
			defer func() { _ = s.Close() }()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			out := make(chan Line, 4)
			errCh := make(chan error, 1)
			go func() { errCh <- s.Run(ctx, out) }()

			got1 := <-out
			got2 := <-out
			cancel()
			<-errCh

			// The whole point of the test: no 8-byte header leaks into the
			// message. If framing detection were wrong, these bytes would
			// carry a binary prefix instead of matching exactly.
			if string(got1.Bytes) != "first line" {
				t.Errorf("line1.Bytes = %q, want %q", got1.Bytes, "first line")
			}
			if string(got2.Bytes) != "second line" {
				t.Errorf("line2.Bytes = %q, want %q", got2.Bytes, "second line")
			}
		})
	}
}

func TestDockerSource_TimestampParsedAndStripped(t *testing.T) {
	base := fixedTime()
	line := tsLine(base, "hello world")

	fake := &fakeDockerClient{
		responses: []fakeLogsResponse{{rc: io.NopCloser(strings.NewReader(line))}},
	}
	s, err := NewDockerSource(&DockerConfig{Container: "web", Stream: DockerStdout, Client: fake})
	if err != nil {
		t.Fatalf("NewDockerSource() error = %v", err)
	}
	defer func() { _ = s.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan Line, 1)
	errCh := make(chan error, 1)
	go func() { errCh <- s.Run(ctx, out) }()

	got := <-out
	cancel()
	<-errCh

	if string(got.Bytes) != "hello world" {
		t.Errorf("Bytes = %q, want %q (timestamp not stripped)", got.Bytes, "hello world")
	}
	if !got.Time.Equal(base) {
		t.Errorf("Time = %v, want %v", got.Time, base)
	}
	if got.Cursor.Start != base.UnixNano() {
		t.Errorf("Cursor.Start = %d, want %d", got.Cursor.Start, base.UnixNano())
	}
	if got.Cursor.Offset != base.UnixNano() {
		t.Errorf("Cursor.Offset = %d, want %d", got.Cursor.Offset, base.UnixNano())
	}
	if got.Cursor.File != (FileID{}) {
		t.Errorf("Cursor.File = %v, want zero", got.Cursor.File)
	}
	if got.Cursor.Head != (Fingerprint{}) {
		t.Errorf("Cursor.Head = %v, want zero", got.Cursor.Head)
	}
	if got := counterValue(t, s.metrics.DockerTimestampParseFailures); got != 0 {
		t.Errorf("DockerTimestampParseFailures = %v, want 0", got)
	}
}

func TestDockerSource_UnparseableTimestampFallsBackWithoutDropping(t *testing.T) {
	tests := []struct {
		name string
		line string
	}{
		{"malformed timestamp with a space", "not-a-real-timestamp hello world\n"},
		{"no space at all", "no-space-in-this-line-whatsoever\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &fakeDockerClient{
				responses: []fakeLogsResponse{{rc: io.NopCloser(strings.NewReader(tt.line))}},
			}
			s, err := NewDockerSource(&DockerConfig{Container: "web", Stream: DockerStdout, Client: fake})
			if err != nil {
				t.Fatalf("NewDockerSource() error = %v", err)
			}
			defer func() { _ = s.Close() }()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			out := make(chan Line, 1)
			errCh := make(chan error, 1)
			go func() { errCh <- s.Run(ctx, out) }()

			before := time.Now()
			got := <-out
			after := time.Now()
			cancel()
			<-errCh

			wantBytes := strings.TrimSuffix(tt.line, "\n")
			if string(got.Bytes) != wantBytes {
				t.Errorf("Bytes = %q, want %q (line must not be dropped or mangled)", got.Bytes, wantBytes)
			}
			if got.Time.Before(before) || got.Time.After(after) {
				t.Errorf("Time = %v, want within [%v, %v] (fallback to observation time)", got.Time, before, after)
			}
			if got := counterValue(t, s.metrics.DockerTimestampParseFailures); got != 1 {
				t.Errorf("DockerTimestampParseFailures = %v, want 1", got)
			}
		})
	}
}

func TestDockerSource_ResumeSendsSince(t *testing.T) {
	resume := time.Date(2024, 6, 1, 12, 0, 0, 500000000, time.UTC)

	calledCh := make(chan client.ContainerLogsOptions, 4)
	pr, pw := io.Pipe()
	t.Cleanup(func() { _ = pw.Close() })

	fake := &fakeDockerClient{
		calledCh:  calledCh,
		responses: []fakeLogsResponse{{rc: pr}}, // blocks: never written to, never closed by the fake.
	}
	s, err := NewDockerSource(&DockerConfig{
		Container: "web",
		Stream:    DockerStdout,
		Client:    fake,
		Resume:    Cursor{Start: resume.UnixNano(), Offset: resume.UnixNano()},
	})
	if err != nil {
		t.Fatalf("NewDockerSource() error = %v", err)
	}
	defer func() { _ = s.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan Line, 1)
	errCh := make(chan error, 1)
	go func() { errCh <- s.Run(ctx, out) }()

	var opts client.ContainerLogsOptions
	select {
	case opts = <-calledCh:
	case <-time.After(2 * time.Second):
		t.Fatal("ContainerLogs was not called in time")
	}
	cancel()
	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Errorf("Run() error = %v, want context.Canceled", err)
	}

	wantSince := resume.Format(time.RFC3339Nano)
	if opts.Since != wantSince {
		t.Errorf("options.Since = %q, want %q", opts.Since, wantSince)
	}
	if !opts.ShowStdout || opts.ShowStderr {
		t.Errorf("options = %+v, want ShowStdout only", opts)
	}
	if !opts.Timestamps {
		t.Error("options.Timestamps = false, want true")
	}
	if !opts.Follow {
		t.Error("options.Follow = false, want true")
	}
}

func TestDockerSource_ZeroResumeSendsNoSince(t *testing.T) {
	calledCh := make(chan client.ContainerLogsOptions, 4)
	pr, pw := io.Pipe()
	t.Cleanup(func() { _ = pw.Close() })

	fake := &fakeDockerClient{
		calledCh:  calledCh,
		responses: []fakeLogsResponse{{rc: pr}},
	}
	s, err := NewDockerSource(&DockerConfig{Container: "web", Stream: DockerStderr, Client: fake})
	if err != nil {
		t.Fatalf("NewDockerSource() error = %v", err)
	}
	defer func() { _ = s.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan Line, 1)
	errCh := make(chan error, 1)
	go func() { errCh <- s.Run(ctx, out) }()

	var opts client.ContainerLogsOptions
	select {
	case opts = <-calledCh:
	case <-time.After(2 * time.Second):
		t.Fatal("ContainerLogs was not called in time")
	}
	cancel()
	<-errCh

	if opts.Since != "" {
		t.Errorf("options.Since = %q, want empty for a zero Resume", opts.Since)
	}
	if opts.ShowStdout || !opts.ShowStderr {
		t.Errorf("options = %+v, want ShowStderr only", opts)
	}
}

func TestDockerSource_StreamEndRetries(t *testing.T) {
	base := fixedTime()
	line := tsLine(base, "after reconnect")

	fake := &fakeDockerClient{
		responses: []fakeLogsResponse{
			{rc: io.NopCloser(strings.NewReader(""))}, // ends immediately: must not be treated as a clean Run() end.
			{rc: io.NopCloser(strings.NewReader(line))},
		},
	}
	s, err := NewDockerSource(&DockerConfig{Container: "web", Stream: DockerStdout, Client: fake})
	if err != nil {
		t.Fatalf("NewDockerSource() error = %v", err)
	}
	defer func() { _ = s.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan Line, 1)
	errCh := make(chan error, 1)
	go func() { errCh <- s.Run(ctx, out) }()

	select {
	case got := <-out:
		if string(got.Bytes) != "after reconnect" {
			t.Errorf("Bytes = %q, want %q", got.Bytes, "after reconnect")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no line arrived after the first stream ended; reconnect did not happen")
	}
	cancel()
	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Errorf("Run() error = %v, want context.Canceled", err)
	}

	if got := fake.callCount(); got < 2 {
		t.Errorf("ContainerLogs called %d times, want at least 2 (a retry)", got)
	}
	if got := counterValue(t, s.metrics.DockerReconnects); got < 1 {
		t.Errorf("DockerReconnects = %v, want at least 1", got)
	}
}

// syncBuffer is a bytes.Buffer safe for one goroutine to write (the slog
// handler, from inside Run) while another reads (the test, polling for the
// warning to appear).
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestDockerSource_ConnectFailureIsLogged pins the one case this package
// makes an exception to "no logger reaches a Source": a container that
// cannot be connected to at all (as opposed to one whose stream merely
// ended) is a configuration error, not ordinary operation, and must not
// retry forever in total silence.
func TestDockerSource_ConnectFailureIsLogged(t *testing.T) {
	var buf syncBuffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	fake := &fakeDockerClient{
		responses: []fakeLogsResponse{{err: errors.New("no such container")}},
	}
	s, err := NewDockerSource(&DockerConfig{Container: "ghost", Stream: DockerStdout, Client: fake, Logger: logger})
	if err != nil {
		t.Fatalf("NewDockerSource() error = %v", err)
	}
	defer func() { _ = s.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan Line, 1)
	errCh := make(chan error, 1)
	go func() { errCh <- s.Run(ctx, out) }()

	deadline := time.After(2 * time.Second)
	for !strings.Contains(buf.String(), "connect failed") {
		select {
		case <-deadline:
			t.Fatal("connect failure was never logged")
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	<-errCh

	logged := buf.String()
	if !strings.Contains(logged, "ghost") {
		t.Errorf("log output = %q, want it to mention the container", logged)
	}
	if !strings.Contains(logged, "no such container") {
		t.Errorf("log output = %q, want it to mention the error", logged)
	}
}

func TestDockerSource_Cancellation(t *testing.T) {
	// A reader that never produces data and is never closed by the fake:
	// only ctx cancellation (which must close it) can unblock the read.
	pr, pw := io.Pipe()
	t.Cleanup(func() { _ = pw.Close() })

	fake := &fakeDockerClient{responses: []fakeLogsResponse{{rc: pr}}}
	s, err := NewDockerSource(&DockerConfig{Container: "web", Stream: DockerStdout, Client: fake})
	if err != nil {
		t.Fatalf("NewDockerSource() error = %v", err)
	}
	defer func() { _ = s.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	out := make(chan Line, 1)

	start := time.Now()
	errCh := make(chan error, 1)
	go func() { errCh <- s.Run(ctx, out) }()

	err = <-errCh
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Run() error = %v, want context.DeadlineExceeded", err)
	}
	if elapsed > 250*time.Millisecond {
		t.Errorf("Run() took %v to return after cancellation, want well under 250ms", elapsed)
	}
}

func TestDockerSource_Close(t *testing.T) {
	t.Run("owned client is closed", func(t *testing.T) {
		fake := &fakeDockerClient{}
		s := &DockerSource{client: fake, ownsClient: true, assembler: newLineAssembler()}

		if err := s.Close(); err != nil {
			t.Errorf("Close() error = %v, want nil", err)
		}
		if fake.closeN != 1 {
			t.Errorf("Close() called the client %d times, want 1", fake.closeN)
		}

		// Idempotent: a second Close must not close it again.
		if err := s.Close(); err != nil {
			t.Errorf("second Close() error = %v, want nil", err)
		}
		if fake.closeN != 1 {
			t.Errorf("Close() called the client %d times after a second Close, want 1", fake.closeN)
		}
	})

	t.Run("injected client is not closed", func(t *testing.T) {
		fake := &fakeDockerClient{}
		s := &DockerSource{client: fake, ownsClient: false, assembler: newLineAssembler()}

		if err := s.Close(); err != nil {
			t.Errorf("Close() error = %v, want nil", err)
		}
		if fake.closeN != 0 {
			t.Errorf("Close() called an injected client %d times, want 0", fake.closeN)
		}

		if err := s.Close(); err != nil {
			t.Errorf("second Close() error = %v, want nil", err)
		}
		if fake.closeN != 0 {
			t.Errorf("Close() called an injected client %d times after a second Close, want 0", fake.closeN)
		}
	})
}

func TestDockerSource_LongLineTruncated(t *testing.T) {
	base := fixedTime()
	tsStr := base.Format(time.RFC3339Nano)
	longMessage := strings.Repeat("x", model.MaxMessageLen+1000)
	line := tsStr + " " + longMessage + "\n"

	fake := &fakeDockerClient{
		responses: []fakeLogsResponse{{rc: io.NopCloser(strings.NewReader(line))}},
	}
	s, err := NewDockerSource(&DockerConfig{Container: "web", Stream: DockerStdout, Client: fake})
	if err != nil {
		t.Fatalf("NewDockerSource() error = %v", err)
	}
	defer func() { _ = s.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan Line, 1)
	errCh := make(chan error, 1)
	go func() { errCh <- s.Run(ctx, out) }()

	got := <-out
	cancel()
	<-errCh

	// The raw line (timestamp + space + message) is truncated to
	// model.MaxMessageLen by lineAssembler before the timestamp is split
	// off, so the message that remains is shorter than that by the
	// timestamp prefix's length -- not dropped, and not the full message.
	wantLen := model.MaxMessageLen - len(tsStr) - 1
	if len(got.Bytes) != wantLen {
		t.Fatalf("len(Bytes) = %d, want %d", len(got.Bytes), wantLen)
	}
	if string(got.Bytes) != longMessage[:wantLen] {
		t.Error("truncated content does not match the expected prefix")
	}
	if !got.Time.Equal(base) {
		t.Errorf("Time = %v, want %v (timestamp survives truncation since it comes first)", got.Time, base)
	}
}

func TestDockerSource_SlowConsumerNoDrops(t *testing.T) {
	base := fixedTime()
	const numLines = 20

	var sb strings.Builder
	for i := 0; i < numLines; i++ {
		sb.WriteString(tsLine(base.Add(time.Duration(i)*time.Second), fmt.Sprintf("line%d", i)))
	}

	fake := &fakeDockerClient{
		responses: []fakeLogsResponse{{rc: io.NopCloser(strings.NewReader(sb.String()))}},
	}
	s, err := NewDockerSource(&DockerConfig{Container: "web", Stream: DockerStdout, Client: fake})
	if err != nil {
		t.Fatalf("NewDockerSource() error = %v", err)
	}
	defer func() { _ = s.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan Line, 1) // small, to force backpressure.
	errCh := make(chan error, 1)
	go func() { errCh <- s.Run(ctx, out) }()

	var received []string
	for i := 0; i < numLines; i++ {
		line := <-out
		received = append(received, string(line.Bytes))
		time.Sleep(2 * time.Millisecond) // slow consumer.
	}
	cancel()
	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Errorf("Run() error = %v, want context.Canceled", err)
	}

	if len(received) != numLines {
		t.Fatalf("got %d lines, want %d", len(received), numLines)
	}
	for i := 0; i < numLines; i++ {
		want := fmt.Sprintf("line%d", i)
		if received[i] != want {
			t.Errorf("line %d: got %q, want %q", i, received[i], want)
		}
	}
}
