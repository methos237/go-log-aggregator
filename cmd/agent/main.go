// Command agent tails local log sources — files, Docker containers, stdin —
// and ships them to a collector's ingest listener.
//
// Phase 3 wires this binary: configuration, the pipeline (sources → multiline
// joiner → shipper), self-metrics and graceful shutdown. Every component it
// assembles — TailSource, DockerSource, StdinSource, Joiner, Extractor,
// Spool, CheckpointStore, Shipper — lives in internal/agent and was built and
// tested in earlier phase 3 subtasks; this binary's job is only to configure
// and connect them correctly, and to shut them down in an order that never
// loses an acknowledged-but-uncommitted line.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"

	"github.com/jamespolk/go-log-aggregator/internal/agent"
	"github.com/jamespolk/go-log-aggregator/internal/config"
	"github.com/jamespolk/go-log-aggregator/internal/ingest"
	"github.com/jamespolk/go-log-aggregator/internal/model"
	"github.com/jamespolk/go-log-aggregator/internal/observability"
	"github.com/jamespolk/go-log-aggregator/internal/version"
)

func main() {
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(version.String("agent"))
		return
	}

	if err := run(); err != nil {
		// The logger may not exist yet when configuration fails, so this path
		// writes to stderr directly, matching cmd/collector.
		fmt.Fprintf(os.Stderr, "agent: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	// Agent.Validate is not part of Config.Validate (see its doc comment):
	// this is the one process that actually needs sources configured, so it
	// checks for itself.
	if err = cfg.Agent.Validate(); err != nil {
		return fmt.Errorf("invalid agent configuration: %w", err)
	}

	log := observability.NewLogger(cfg.Log, os.Stdout, cfg.Node.Name)
	metrics := observability.NewMetrics(cfg.Node.Name)
	agentMetrics := agent.NewMetrics(metrics.Registerer)

	// Signals are trapped before anything starts, so a Ctrl-C during startup
	// still shuts down cleanly instead of killing the process mid-init —
	// mirroring cmd/collector.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Info("agent starting",
		slog.String("env", cfg.Agent.Env),
		slog.String("host", cfg.Agent.Host),
		slog.String("commit", version.Commit),
		slog.String("ingest_addr", cfg.Agent.IngestAddr),
		slog.String("admin_addr", cfg.Admin.Addr),
		slog.Int("files", len(cfg.Agent.Files)),
		slog.Int("containers", len(cfg.Agent.Containers)),
		slog.Bool("stdin", cfg.Agent.Stdin),
	)

	checkpoint := agent.NewCheckpointStore(cfg.Agent.CheckpointPath)
	if err = checkpoint.Load(); err != nil {
		return fmt.Errorf("load checkpoint: %w", err)
	}

	spool, err := agent.NewSpool(agent.SpoolConfig{
		Dir:          cfg.Agent.SpoolDir,
		MaxBytes:     cfg.Agent.SpoolMaxBytes,
		SegmentBytes: cfg.Agent.SpoolSegmentBytes,
		Metrics:      agentMetrics,
	})
	if err != nil {
		return fmt.Errorf("open spool: %w", err)
	}
	// Belt-and-suspenders: the normal shutdown sequence below also closes the
	// spool as its own explicit, ordered step (step 6). This defer only
	// matters if some later setup step fails and returns before that
	// sequence ever runs — Close is idempotent, so paying for it twice on the
	// happy path costs nothing but a redundant cursor persist.
	defer func() { _ = spool.Close() }()

	sources, sourceLabels, err := buildSources(&cfg.Agent, checkpoint, agentMetrics, log)
	if err != nil {
		return fmt.Errorf("build sources: %w", err)
	}
	defer func() {
		for _, src := range sources {
			_ = src.Close()
		}
	}()

	joiner, err := buildJoiner(&cfg.Agent, agentMetrics)
	if err != nil {
		return fmt.Errorf("build multiline joiner: %w", err)
	}

	extractor, err := buildExtractor(&cfg.Agent, agentMetrics)
	if err != nil {
		return fmt.Errorf("build field extractor: %w", err)
	}

	shipper, err := agent.NewShipper(&agent.ShipperConfig{
		Ingest: ingest.ClientConfig{
			Addr:     cfg.Agent.IngestAddr,
			CertFile: cfg.Agent.CertFile,
			KeyFile:  cfg.Agent.KeyFile,
			CAFile:   cfg.Agent.CAFile,
		},
		Spool:      spool,
		Checkpoint: checkpoint,
		Extractor:  extractor,
		Labels: func(source string) (model.LabelSet, bool) {
			ls, ok := sourceLabels[source]
			return ls, ok
		},
		// Level inference is out of scope for this phase (see
		// ShipperConfig.DefaultLevel's doc comment); LevelUnspecified is the
		// model package's own answer for "no level known," not a guess.
		DefaultLevel:    model.LevelUnspecified,
		MaxBatchRecords: cfg.Agent.BatchRecords,
		MaxBatchBytes:   cfg.Agent.BatchBytes,
		MaxBatchDelay:   cfg.Agent.BatchDelay,
		AckWindow:       cfg.Agent.AckWindow,
		MinBackoff:      cfg.Agent.MinBackoff,
		MaxBackoff:      cfg.Agent.MaxBackoff,
		Metrics:         agentMetrics,
	})
	if err != nil {
		return fmt.Errorf("build shipper: %w", err)
	}

	adminSrv := observability.NewAdminServer(cfg.Admin, metrics, log)

	// The pipeline: sources (N goroutines) -> lines (bounded, QueueCapacity)
	// -> an instrumented relay -> linesToJoiner -> Joiner -> joined (bounded)
	// -> Shipper. See runPipeline's doc comment for why there are two
	// channels between the sources and the Joiner rather than one.
	lines := make(chan agent.Line, cfg.Agent.QueueCapacity)
	linesToJoiner := make(chan agent.Line, cfg.Agent.QueueCapacity)
	joined := make(chan agent.Line, cfg.Agent.QueueCapacity)

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		if serveErr := adminSrv.ListenAndServe(); serveErr != nil {
			return fmt.Errorf("admin server: %w", serveErr)
		}
		return nil
	})

	g.Go(func() error {
		return runPipeline(gctx, &pipelineDeps{
			cfg:           cfg,
			log:           log,
			sources:       sources,
			joiner:        joiner,
			shipper:       shipper,
			spool:         spool,
			checkpoint:    checkpoint,
			adminSrv:      adminSrv,
			lines:         lines,
			linesToJoiner: linesToJoiner,
			joined:        joined,
			registerer:    metrics.Registerer,
		})
	})

	err = g.Wait()
	// A signal-driven exit surfaces as context.Canceled from gctx; that is the
	// success path, not a failure — mirroring cmd/collector.
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Error("agent stopped with error", slog.Any("error", err))
		return err
	}

	log.Info("agent stopped")
	return nil
}

// pipelineDeps bundles runPipeline's dependencies. A struct rather than a
// long parameter list because gocritic's hugeParam check would flag the
// latter, and every field here is genuinely required.
type pipelineDeps struct {
	cfg        *config.Config
	log        *slog.Logger
	sources    []agent.Source
	joiner     *agent.Joiner
	shipper    *agent.Shipper
	spool      *agent.Spool
	checkpoint *agent.CheckpointStore
	adminSrv   *observability.AdminServer
	registerer prometheus.Registerer

	lines         chan agent.Line
	linesToJoiner chan agent.Line
	joined        chan agent.Line
}

// runPipeline starts every source, the instrumented relay, the Joiner and
// the Shipper, then blocks until ctx is done and runs the shutdown sequence
// documented step by step below. It returns nil for the ordinary
// signal-driven shutdown path (ctx.Err() is context.Canceled) and a non-nil
// error for anything that went wrong along the way, including a stage that
// did not finish inside cfg.Node.ShutdownTimeout.
//
// Sources write to lines, not directly to what the Joiner reads. That
// indirection exists solely so this function can instrument the "lines chan"
// the phase 3 design calls for: neither Source nor Joiner exposes a hook for
// recording queue depth or wait time on the channel between them (a Source
// writes straight to whatever channel it is given, and the Joiner reads
// straight from whatever channel it is given), so relayLines is spliced in
// as the one place that can observe both ends. It changes nothing about the
// shutdown contract — closing lines still propagates to the Joiner's input
// exactly one hop later, once the relay itself sees lines close.
//
//nolint:contextcheck // drainCtx deliberately does not inherit ctx's cancellation; see its own doc comment below
func runPipeline(ctx context.Context, d *pipelineDeps) error {
	var sourceWG sync.WaitGroup
	sourceErrs := make([]error, len(d.sources))
	for i, src := range d.sources {
		sourceWG.Add(1)
		go func(i int, src agent.Source) {
			defer sourceWG.Done()
			// Sources run against ctx directly: a signal (or any other
			// failure that cancels the errgroup's context) must stop them
			// immediately, which is step 1 and step 2 of the sequence below.
			// ctx.Err() is the expected outcome of that and is not reported;
			// anything else is a genuine source failure.
			if runErr := src.Run(ctx, d.lines); runErr != nil && !errors.Is(runErr, context.Canceled) {
				sourceErrs[i] = runErr
			}
		}(i, src)
	}

	// drainCtx is deliberately NOT ctx: the Joiner and the Shipper must keep
	// running their ordinary select loops — which race a channel receive
	// against ctx.Done() in the very same select statement — until their
	// input channel closes, not until a signal fires. Passing them the
	// already-canceled ctx during shutdown would let the ctx.Done() case win
	// that race and return early without flushing, which is exactly the
	// silent-data-loss failure mode this design avoids. So drainCtx stays
	// alive for the whole of normal operation and is bounded only once the
	// shutdown sequence below actually starts.
	drainCtx, cancelDrain := context.WithCancel(context.Background())
	defer cancelDrain()

	queueDepth := observability.QueueDepth(d.registerer).WithLabelValues(observability.QueueAgentLines)
	queueWait := observability.QueueWait(d.registerer).WithLabelValues(observability.QueueAgentLines)
	go relayLines(drainCtx, d.lines, d.linesToJoiner, queueDepth, queueWait)

	joinerErrCh := make(chan error, 1)
	go func() { joinerErrCh <- d.joiner.Run(drainCtx, d.linesToJoiner, d.joined) }()

	shipperErrCh := make(chan error, 1)
	go func() { shipperErrCh <- d.shipper.Run(drainCtx, d.joined) }()

	<-ctx.Done()
	d.log.Info("agent shutting down", slog.Duration("timeout", d.cfg.Node.ShutdownTimeout))

	// One deadline bounds this whole sequence, mirroring cmd/collector: a
	// wedged stage still lets the process exit within ShutdownTimeout of the
	// signal, rather than the process hanging forever on any single step.
	shutdownCtx, cancelShutdown := context.WithTimeout(context.WithoutCancel(ctx), d.cfg.Node.ShutdownTimeout)
	defer cancelShutdown()
	go func() {
		<-shutdownCtx.Done()
		cancelDrain()
	}()

	var errs []error

	// Step 2: wait for every source's Run to return, bounded by the same
	// deadline. This has to happen, and happen first, because closing lines
	// (step 3) while a source might still send on it would panic.
	sourcesDone := make(chan struct{})
	go func() {
		sourceWG.Wait()
		close(sourcesDone)
	}()
	select {
	case <-sourcesDone:
		errs = append(errs, sourceErrs...)
	case <-shutdownCtx.Done():
		// Not closing lines here is the whole point: a source that is still
		// out there past the deadline might still be trying to send on it,
		// and a send on a closed channel panics. An unclean exit beats that.
		d.log.Error("shutdown timed out waiting for sources to stop; leaving the lines channel open to avoid a send-on-closed-channel panic")
		return errors.Join(append(errs, fmt.Errorf("agent: %d source(s) did not stop before the %s shutdown timeout", len(d.sources), d.cfg.Node.ShutdownTimeout))...)
	}

	// Step 3.
	close(d.lines)

	// Step 4: the relay sees lines close and closes linesToJoiner in turn;
	// the Joiner sees that close, flushes every held record, and returns.
	joinErr := <-joinerErrCh
	if joinErr != nil {
		d.log.Error("shutdown: multiline joiner did not finish flushing before the timeout", slog.Any("error", joinErr))
	}
	errs = append(errs, joinErr)
	close(d.joined)

	// Step 5: the shipper sees joined close and runs its own shutdown --
	// flush accumulators, a bounded wait for outstanding acks, spool the
	// rest, commit the checkpoint (see Shipper.shutdown).
	shipErr := <-shipperErrCh
	if shipErr != nil {
		d.log.Error("shutdown: shipper did not finish draining before the timeout", slog.Any("error", shipErr))
	}
	errs = append(errs, shipErr)

	// Step 6.
	if closeErr := d.spool.Close(); closeErr != nil {
		errs = append(errs, fmt.Errorf("close spool: %w", closeErr))
	}
	if commitErr := d.checkpoint.Commit(); commitErr != nil {
		errs = append(errs, fmt.Errorf("final checkpoint commit: %w", commitErr))
	}

	// Admin is torn down last, so metrics stay scrapeable for the whole
	// drain — the same reasoning cmd/collector's shutdown sequence gives for
	// its own admin server.
	if adminErr := d.adminSrv.Shutdown(shutdownCtx); adminErr != nil {
		errs = append(errs, fmt.Errorf("admin shutdown: %w", adminErr))
	}

	return errors.Join(errs...)
}

// relayLines copies every Line from src to dst, recording queue depth and
// wait time under the shared observability.QueueAgentLines label, and closes
// dst once src closes (or ctx is done, as a last-resort escape so this
// goroutine cannot leak if something downstream is permanently stuck).
//
// Depth is exact: len(src), sampled the instant an item leaves it. Wait uses
// each Line's own Time as the enqueue reference, which is exact for the tail
// and stdin sources — they set Time to time.Now() immediately before
// sending — and is instead dominated by replay lag for the Docker source
// while it is catching up on backlog after a resume. That is an honest
// reading in both cases, just of a different thing: for the file and stdin
// sources it is genuine time spent waiting in this bounded queue, and for a
// catching-up Docker source it is mostly "how far behind is this container's
// backlog," with real queue contention as a small additional component.
func relayLines(ctx context.Context, src <-chan agent.Line, dst chan<- agent.Line, depth prometheus.Gauge, wait prometheus.Observer) {
	defer close(dst)
	for {
		select {
		case line, ok := <-src:
			if !ok {
				return
			}
			depth.Set(float64(len(src)))
			wait.Observe(time.Since(line.Time).Seconds())
			select {
			case dst <- line:
			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

// buildSources resolves cfg into one agent.Source per file, two per
// container (stdout and stderr), and one for stdin if enabled, along with
// the label set for each. The returned map is built once, here, from
// exactly the sources this call constructs — see ShipperConfig.Labels' doc
// comment on why an unknown source name reaching it is meant to be a bug,
// not a possibility this function leaves open.
func buildSources(cfg *config.Agent, checkpoint *agent.CheckpointStore, metrics *agent.Metrics, log *slog.Logger) ([]agent.Source, map[string]model.LabelSet, error) {
	var sources []agent.Source
	labels := make(map[string]model.LabelSet)

	addLabel := func(name, service string) {
		labels[name] = model.LabelSet{Service: service, Host: cfg.Host, Env: cfg.Env}
	}
	// service resolves the per-source service label: cfg.Service, if set,
	// overrides every source and deliberately collapses them into one
	// stream (see config.Agent.Service's doc comment); otherwise fallback
	// computes it from the source's own identity.
	service := func(fallback func() string) string {
		if cfg.Service != "" {
			return cfg.Service
		}
		return fallback()
	}

	for _, path := range cfg.Files {
		resume, _ := checkpoint.Get(path)
		src, err := agent.NewTailSource(&agent.TailConfig{
			Path:         path,
			PollInterval: cfg.PollInterval,
			Resume:       resume,
			Metrics:      metrics,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("tail source %s: %w", path, err)
		}
		addLabel(src.Name(), service(func() string { return fileService(path) }))
		sources = append(sources, src)
	}

	for _, container := range cfg.Containers {
		for _, stream := range [...]agent.DockerStream{agent.DockerStdout, agent.DockerStderr} {
			name := dockerSourceName(container, stream)
			resume, _ := checkpoint.Get(name)
			src, err := agent.NewDockerSource(&agent.DockerConfig{
				Container: container,
				Stream:    stream,
				Resume:    resume,
				Metrics:   metrics,
				Logger:    log,
			})
			if err != nil {
				return nil, nil, fmt.Errorf("docker source %s: %w", name, err)
			}
			addLabel(src.Name(), service(func() string { return containerService(container, stream) }))
			sources = append(sources, src)
		}
	}

	if cfg.Stdin {
		src := agent.NewStdinSource(os.Stdin)
		// No per-source rule is documented for stdin the way there is for a
		// file or a container stream — there is exactly one of it, and its
		// own Name() ("stdin") is already the obvious, unambiguous label.
		addLabel(src.Name(), service(func() string { return src.Name() }))
		sources = append(sources, src)
	}

	return sources, labels, nil
}

// fileService derives a file source's service label: the path's base name
// with its extension stripped, e.g. "/var/log/app.log" -> "app".
func fileService(path string) string {
	base := filepath.Base(path)
	return strings.TrimSuffix(base, filepath.Ext(base))
}

// containerService derives a container source's service label:
// "<container>-<stream>", so stdout and stderr become distinct streams
// instead of collapsing into one.
func containerService(container string, stream agent.DockerStream) string {
	return container + "-" + string(stream)
}

// dockerSourceName mirrors agent.DockerSource.Name()'s documented format
// ("docker/<container>/<stream>") exactly, so a checkpoint lookup here uses
// the same key the constructed source will report once built. Duplicating
// the format instead of building the source first and asking it is what
// lets Resume be set at construction time, which is the only time
// DockerConfig accepts it.
func dockerSourceName(container string, stream agent.DockerStream) string {
	return fmt.Sprintf("docker/%s/%s", container, stream)
}

// buildJoiner compiles cfg's multiline pattern, if any, and constructs a
// Joiner. The pattern was already validated by config.Agent.Validate, so a
// compile failure here means Validate was skipped somewhere upstream, which
// is itself a bug worth surfacing rather than silently disabling joining.
func buildJoiner(cfg *config.Agent, metrics *agent.Metrics) (*agent.Joiner, error) {
	var continuation *regexp.Regexp
	if cfg.MultilinePattern != "" {
		re, err := regexp.Compile(cfg.MultilinePattern)
		if err != nil {
			return nil, fmt.Errorf("multiline pattern %q: %w", cfg.MultilinePattern, err)
		}
		continuation = re
	}
	return agent.NewJoiner(agent.MultilineConfig{
		Continuation: continuation,
		FlushTimeout: cfg.MultilineTimeout,
		Metrics:      metrics,
	})
}

// buildExtractor compiles cfg's extraction pattern, if any, and constructs
// an Extractor. See buildJoiner's doc comment on why a compile failure here
// is unexpected rather than routine.
func buildExtractor(cfg *config.Agent, metrics *agent.Metrics) (*agent.Extractor, error) {
	var pattern *regexp.Regexp
	if cfg.ExtractPattern != "" {
		re, err := regexp.Compile(cfg.ExtractPattern)
		if err != nil {
			return nil, fmt.Errorf("extract pattern %q: %w", cfg.ExtractPattern, err)
		}
		pattern = re
	}
	return agent.NewExtractor(agent.ExtractConfig{
		Pattern: pattern,
		JSON:    cfg.ExtractJSON,
		Metrics: metrics,
	})
}
