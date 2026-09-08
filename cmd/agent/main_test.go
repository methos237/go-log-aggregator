package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"

	logaggv1 "github.com/jamespolk/go-log-aggregator/api/proto/logagg/v1"
	"github.com/jamespolk/go-log-aggregator/internal/agent"
	"github.com/jamespolk/go-log-aggregator/internal/config"
	"github.com/jamespolk/go-log-aggregator/internal/ingest"
	"github.com/jamespolk/go-log-aggregator/internal/model"
	"github.com/jamespolk/go-log-aggregator/internal/observability"
)

// discardLogger is a *slog.Logger that writes nowhere, for tests that need
// one but do not want to assert on its output.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestFileService(t *testing.T) {
	tests := []struct {
		path string
		want string
	}{
		{"/var/log/app.log", "app"},
		{"/var/log/nested/worker.log", "worker"},
		{"app.log.1", "app.log"}, // only the last extension is stripped
		{"noext", "noext"},
		{"/var/log/archive.tar.gz", "archive.tar"},
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			if got := fileService(tt.path); got != tt.want {
				t.Errorf("fileService(%q) = %q, want %q", tt.path, got, tt.want)
			}
		})
	}
}

func TestContainerService(t *testing.T) {
	tests := []struct {
		container string
		stream    agent.DockerStream
		want      string
	}{
		{"web", agent.DockerStdout, "web-stdout"},
		{"web", agent.DockerStderr, "web-stderr"},
	}
	for _, tt := range tests {
		if got := containerService(tt.container, tt.stream); got != tt.want {
			t.Errorf("containerService(%q, %q) = %q, want %q", tt.container, tt.stream, got, tt.want)
		}
	}
}

// TestDockerSourceNameMatchesConstructedSource pins dockerSourceName's
// duplicated format against agent.DockerSource.Name()'s real, documented
// output: this function only exists so a checkpoint lookup can happen
// before construction (see its own doc comment), and if the two ever
// disagree, every container source would resume from the wrong checkpoint
// key silently.
func TestDockerSourceNameMatchesConstructedSource(t *testing.T) {
	for _, stream := range []agent.DockerStream{agent.DockerStdout, agent.DockerStderr} {
		// Client left nil: NewDockerSource then builds one from the
		// environment, which only assembles configuration and dials
		// nothing, so this needs no daemon -- the same premise
		// internal/agent's own TestDockerSource_OwnsBuiltClient relies on.
		src, err := agent.NewDockerSource(&agent.DockerConfig{Container: "web", Stream: stream})
		if err != nil {
			t.Fatalf("NewDockerSource(%q): %v", stream, err)
		}
		t.Cleanup(func() { _ = src.Close() })

		if got, want := dockerSourceName("web", stream), src.Name(); got != want {
			t.Errorf("dockerSourceName(%q, %q) = %q, want %q (agent.DockerSource.Name())", "web", stream, got, want)
		}
	}
}

// labelsEqual compares two LabelSets field by field: model.LabelSet embeds a
// map, so it is not comparable with == at all (a compile error, not merely a
// wrong answer), and none of these tests populate Extra, so a length check
// is enough to catch a surprise there without pulling in reflect.DeepEqual.
func labelsEqual(a, b model.LabelSet) bool {
	return a.Service == b.Service && a.Host == b.Host && a.Env == b.Env && len(a.Extra) == 0 && len(b.Extra) == 0
}

// testAgentConfig returns an Agent config with two files, one container and
// stdin enabled -- enough sources to exercise every label-derivation rule at
// once -- and a fresh checkpoint store with nothing in it.
func testAgentConfig(t *testing.T) (*config.Agent, *agent.CheckpointStore) {
	t.Helper()
	checkpoint := agent.NewCheckpointStore(filepath.Join(t.TempDir(), "checkpoint.json"))
	// Load is not called: Get on a store nothing was ever loaded into simply
	// reports "no cursor", which is exactly what a fresh agent should see.
	return &config.Agent{
		Files:      []string{"/var/log/app.log", "/var/log/nested/worker.log"},
		Containers: []string{"web"},
		Stdin:      true,
		Host:       "test-host",
		Env:        "test-env",
	}, checkpoint
}

// TestBuildSourcesLabelDerivation checks every documented derivation rule at
// once: a file's service is its base name with the extension stripped, a
// container's two streams get distinct "<container>-<stream>" services, and
// stdin's own Name() stands in for a rule the spec does not otherwise give
// it. It also checks the map buildSources returns names exactly the sources
// it built -- nothing more, nothing less.
func TestBuildSourcesLabelDerivation(t *testing.T) {
	cfg, checkpoint := testAgentConfig(t)
	metrics := agent.NewMetrics(nil)

	sources, labels, err := buildSources(cfg, checkpoint, metrics, discardLogger())
	if err != nil {
		t.Fatalf("buildSources: %v", err)
	}
	t.Cleanup(func() {
		for _, src := range sources {
			_ = src.Close()
		}
	})

	if got, want := len(sources), 5; got != want {
		t.Fatalf("len(sources) = %d, want %d (2 files + 2 container streams + stdin)", got, want)
	}
	if got, want := len(labels), len(sources); got != want {
		t.Fatalf("len(labels) = %d, want %d: every configured source and nothing else", got, want)
	}

	names := make(map[string]bool, len(sources))
	for _, src := range sources {
		names[src.Name()] = true
		if _, ok := labels[src.Name()]; !ok {
			t.Errorf("labels has no entry for %q, a source buildSources itself constructed", src.Name())
		}
	}
	for name := range labels {
		if !names[name] {
			t.Errorf("labels has an entry for %q, which is not one of the constructed sources", name)
		}
	}

	want := map[string]model.LabelSet{
		"/var/log/app.log":           {Service: "app", Host: "test-host", Env: "test-env"},
		"/var/log/nested/worker.log": {Service: "worker", Host: "test-host", Env: "test-env"},
		"docker/web/stdout":          {Service: "web-stdout", Host: "test-host", Env: "test-env"},
		"docker/web/stderr":          {Service: "web-stderr", Host: "test-host", Env: "test-env"},
		"stdin":                      {Service: "stdin", Host: "test-host", Env: "test-env"},
	}
	for name, wantLS := range want {
		gotLS, ok := labels[name]
		if !ok {
			t.Errorf("labels[%q] missing", name)
			continue
		}
		if !labelsEqual(gotLS, wantLS) {
			t.Errorf("labels[%q] = %+v, want %+v", name, gotLS, wantLS)
		}
	}
}

// TestBuildSourcesServiceOverrideCollapsesStreams checks the documented
// escape hatch: setting Service deliberately collapses every source into
// one label set, which is what makes them one stream downstream.
func TestBuildSourcesServiceOverrideCollapsesStreams(t *testing.T) {
	cfg, checkpoint := testAgentConfig(t)
	cfg.Service = "everything"
	metrics := agent.NewMetrics(nil)

	sources, labels, err := buildSources(cfg, checkpoint, metrics, discardLogger())
	if err != nil {
		t.Fatalf("buildSources: %v", err)
	}
	t.Cleanup(func() {
		for _, src := range sources {
			_ = src.Close()
		}
	})

	want := model.LabelSet{Service: "everything", Host: "test-host", Env: "test-env"}
	for name, got := range labels {
		if !labelsEqual(got, want) {
			t.Errorf("labels[%q] = %+v, want %+v (the override must collapse every source into one stream)", name, got, want)
		}
	}
}

// stubCollector is a minimal ingest server: it accepts every batch and acks
// it ACCEPTED, recording each batch it received so a test can assert on it.
// It needs no queue, no database and no TLS, which is what makes an
// end-to-end pipeline test practical without the machinery a real collector
// requires.
type stubCollector struct {
	logaggv1.UnimplementedLogServiceServer
	batches chan *logaggv1.LogBatch
}

func (s *stubCollector) Stream(stream grpc.BidiStreamingServer[logaggv1.LogBatch, logaggv1.Ack]) error {
	for {
		batch, err := stream.Recv()
		if err != nil {
			return nil //nolint:nilerr // a client closing the stream is not a server-side failure
		}
		s.batches <- batch
		ack := &logaggv1.Ack{
			BatchId:  batch.GetBatchId(),
			Code:     logaggv1.AckCode_ACK_CODE_ACCEPTED,
			Accepted: uint32(len(batch.GetRecords())), //nolint:gosec // test batches never approach uint32's range
		}
		if err := stream.Send(ack); err != nil {
			return err
		}
	}
}

// TestPipelineEndToEnd builds the real pipeline -- a TailSource on a temp
// file, a passthrough Joiner, an Extractor, and a Shipper -- against a
// stub collector, writes one line to the tailed file, and checks it arrives
// as a batch with the label derivation this package is responsible for.
// Then it cancels the context and checks runPipeline's shutdown sequence
// returns within a bound instead of hanging.
//
// This is the practical end-to-end test the task asks for: a stub gRPC
// server is enough to exercise the whole agent-side pipeline without a
// queue or a database, which a real collector would require.
func TestPipelineEndToEnd(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	stub := &stubCollector{batches: make(chan *logaggv1.LogBatch, 4)}
	grpcServer := grpc.NewServer()
	logaggv1.RegisterLogServiceServer(grpcServer, stub)
	go func() { _ = grpcServer.Serve(lis) }()
	t.Cleanup(grpcServer.Stop)

	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	if err = os.WriteFile(logPath, nil, 0o600); err != nil {
		t.Fatalf("create log file: %v", err)
	}

	cfg := &config.Agent{
		Files:             []string{logPath},
		Host:              "e2e-host",
		Env:               "e2e-env",
		IngestAddr:        lis.Addr().String(),
		CheckpointPath:    filepath.Join(dir, "checkpoint.json"),
		SpoolDir:          filepath.Join(dir, "spool"),
		SpoolMaxBytes:     1 << 20,
		SpoolSegmentBytes: 1 << 20,
		QueueCapacity:     16,
		PollInterval:      20 * time.Millisecond,
		MultilineTimeout:  time.Second,
		BatchRecords:      500,
		BatchBytes:        512 << 10,
		BatchDelay:        50 * time.Millisecond,
		AckWindow:         64,
		MinBackoff:        10 * time.Millisecond,
		MaxBackoff:        100 * time.Millisecond,
	}
	if err = cfg.Validate(); err != nil {
		t.Fatalf("test config is invalid: %v", err)
	}

	checkpoint := agent.NewCheckpointStore(filepath.Join(dir, "checkpoint.json"))
	spool, err := agent.NewSpool(agent.SpoolConfig{
		Dir: filepath.Join(dir, "spool"), MaxBytes: 1 << 20, SegmentBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("NewSpool: %v", err)
	}

	metrics := agent.NewMetrics(nil)
	sources, labels, err := buildSources(cfg, checkpoint, metrics, discardLogger())
	if err != nil {
		t.Fatalf("buildSources: %v", err)
	}
	joiner, err := buildJoiner(cfg, metrics)
	if err != nil {
		t.Fatalf("buildJoiner: %v", err)
	}
	extractor, err := buildExtractor(cfg, metrics)
	if err != nil {
		t.Fatalf("buildExtractor: %v", err)
	}
	shipper, err := agent.NewShipper(&agent.ShipperConfig{
		Ingest: ingest.ClientConfig{
			Addr:     cfg.IngestAddr,
			CertFile: cfg.CertFile,
			KeyFile:  cfg.KeyFile,
			CAFile:   cfg.CAFile,
		},
		Spool:           spool,
		Checkpoint:      checkpoint,
		Extractor:       extractor,
		Labels:          func(source string) (model.LabelSet, bool) { ls, ok := labels[source]; return ls, ok },
		DefaultLevel:    model.LevelUnspecified,
		MaxBatchRecords: cfg.BatchRecords,
		MaxBatchBytes:   cfg.BatchBytes,
		MaxBatchDelay:   cfg.BatchDelay,
		AckWindow:       cfg.AckWindow,
		MinBackoff:      cfg.MinBackoff,
		MaxBackoff:      cfg.MaxBackoff,
		Metrics:         metrics,
	})
	if err != nil {
		t.Fatalf("NewShipper: %v", err)
	}

	adminSrv := observability.NewAdminServer(config.Admin{Addr: "127.0.0.1:0"}, observability.NewMetrics("e2e-test"), discardLogger())

	deps := &pipelineDeps{
		cfg:           &config.Config{Node: config.Node{ShutdownTimeout: 5 * time.Second}},
		log:           discardLogger(),
		sources:       sources,
		joiner:        joiner,
		shipper:       shipper,
		spool:         spool,
		checkpoint:    checkpoint,
		adminSrv:      adminSrv,
		registerer:    nil,
		lines:         make(chan agent.Line, cfg.QueueCapacity),
		linesToJoiner: make(chan agent.Line, cfg.QueueCapacity),
		joined:        make(chan agent.Line, cfg.QueueCapacity),
	}

	ctx, cancel := context.WithCancel(context.Background())
	pipelineErrCh := make(chan error, 1)
	go func() { pipelineErrCh <- runPipeline(ctx, deps) }()

	// Append after the pipeline is running, matching how a log file behaves
	// in practice: the writer postdates the reader that will tail it.
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("open log file for append: %v", err)
	}
	if _, err := f.WriteString("hello from the e2e test\n"); err != nil {
		t.Fatalf("write line: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close log file: %v", err)
	}

	var batch *logaggv1.LogBatch
	select {
	case batch = <-stub.batches:
	case <-time.After(5 * time.Second):
		t.Fatal("stub collector never received a batch")
	}

	if got, want := batch.GetLabels().GetService(), "app"; got != want {
		t.Errorf("batch labels.service = %q, want %q", got, want)
	}
	if got, want := batch.GetLabels().GetHost(), "e2e-host"; got != want {
		t.Errorf("batch labels.host = %q, want %q", got, want)
	}
	if got, want := batch.GetLabels().GetEnv(), "e2e-env"; got != want {
		t.Errorf("batch labels.env = %q, want %q", got, want)
	}
	if len(batch.GetRecords()) != 1 {
		t.Fatalf("len(records) = %d, want 1", len(batch.GetRecords()))
	}
	if got, want := batch.GetRecords()[0].GetMessage(), "hello from the e2e test"; got != want {
		t.Errorf("record message = %q, want %q", got, want)
	}

	cancel()
	select {
	case err := <-pipelineErrCh:
		if err != nil {
			t.Errorf("runPipeline() error = %v, want nil after a clean shutdown", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runPipeline did not return within 5s of cancellation")
	}
}

// TestRunRejectsInvalidConfig checks that run() surfaces a configuration
// problem instead of trying to build a pipeline out of it. LOGAGG_AGENT_*
// is explicitly cleared, which config.Agent.Validate rejects as "no sources
// configured" -- the one error run() is guaranteed to hit before it ever
// touches a filesystem path or a network address.
func TestRunRejectsInvalidConfig(t *testing.T) {
	for _, key := range []string{"AGENT_FILES", "AGENT_CONTAINERS", "AGENT_STDIN"} {
		t.Setenv(config.EnvPrefix+key, "")
	}

	err := run()
	if err == nil {
		t.Fatal("run() with no agent sources configured = nil error, want one naming the problem")
	}
	if !strings.Contains(err.Error(), "no sources configured") {
		t.Errorf("run() error = %q, want it to mention \"no sources configured\"", err)
	}
}
