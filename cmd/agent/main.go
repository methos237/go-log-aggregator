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

	stopTracing, err := observability.NewTracer(ctx, cfg.Tracing, "logagg-agent", cfg.Node.Name)
	if err != nil {
		return fmt.Errorf("set up tracing: %w", err)
	}
	// Deferred rather than sequenced: the pipeline's own shutdown lives in
	// runPipeline, and the spans it buffers last are the shipper's.
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.Node.ShutdownTimeout)
		defer cancel()
		if flushErr := stopTracing(flushCtx); flushErr != nil {
			log.Error("tracing shutdown failed", slog.Any("error", flushErr))
		}
	}()

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

	checkpoint, err := prepareCheckpoint(cfg.Agent.CheckpointPath)
	if err != nil {
		return fmt.Errorf("prepare checkpoint: %w", err)
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

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		if serveErr := adminSrv.ListenAndServe(); serveErr != nil {
			return fmt.Errorf("admin server: %w", serveErr)
		}
		return nil
	})

	g.Go(func() error {
		return runPipeline(gctx, &pipelineDeps{
			cfg:        cfg,
			log:        log,
			sources:    sources,
			joiner:     joiner,
			shipper:    shipper,
			spool:      spool,
			checkpoint: checkpoint,
			adminSrv:   adminSrv,
			registerer: metrics.Registerer,
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

// prepareCheckpoint creates path's parent directory if necessary, loads
// whatever checkpoint already lives there, and proves the location is
// writable before returning.
//
// CheckpointStore.Commit's atomic rename needs a temp file next to path,
// and os.CreateTemp refuses with ENOENT if that directory does not exist.
// With every default setting this is invisible, because NewSpool's own
// MkdirAll for the spool directory happens to create the checkpoint's
// parent too — they share one default parent directory. Point
// LOGAGG_AGENT_CHECKPOINT_PATH somewhere else and every commit would
// otherwise fail forever, silently: the periodic commit ticker in
// Shipper.Run discards its error (a gap worth a log line or a metric there,
// but out of this package's scope to fix). Creating the directory here, and
// immediately self-testing it with a real Commit rather than just checking
// permissions, catches both a missing directory and an unwritable one at
// startup instead of on the first real commit, minutes into a run that
// looks otherwise healthy.
func prepareCheckpoint(path string) (*agent.CheckpointStore, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create checkpoint directory %s: %w", dir, err)
	}

	checkpoint := agent.NewCheckpointStore(path)
	if err := checkpoint.Load(); err != nil {
		return nil, fmt.Errorf("load checkpoint: %w", err)
	}
	if err := checkpoint.Commit(); err != nil {
		return nil, fmt.Errorf("checkpoint path %s is not writable: %w", path, err)
	}
	return checkpoint, nil
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
}

// runPipeline starts every source, the instrumented relay, the Joiner and
// the Shipper, then waits for whichever comes first: ctx being done (the
// ordinary signal-driven path), or the Joiner or the Shipper returning on
// its own. The latter is always fatal — see the comment on the select
// below for why — and is treated exactly like a signal for the purpose of
// starting the shutdown sequence documented step by step further down,
// except that it also makes this function return a non-nil error instead
// of the nil it returns for a clean, signal-driven stop. That error
// includes a stage that did not finish inside cfg.Node.ShutdownTimeout.
//
// The pipeline: sources (N goroutines) -> lines (bounded, QueueCapacity) ->
// an instrumented relay -> linesToJoiner -> Joiner -> joined (bounded) ->
// Shipper. Sources write to lines, not directly to what the Joiner reads.
// That indirection exists solely so this function can instrument the "lines
// chan" the phase 3 design calls for: neither Source nor Joiner exposes a
// hook for recording queue depth or wait time on the channel between them (a
// Source writes straight to whatever channel it is given, and the Joiner
// reads straight from whatever channel it is given), so relayLines is spliced
// in as the one place that can observe both ends. It changes nothing about
// the shutdown contract — closing lines still propagates to the Joiner's
// input exactly one hop later, once the relay itself sees lines close.
//
//nolint:contextcheck // drainCtx deliberately does not inherit ctx's cancellation; see its own doc comment below
func runPipeline(ctx context.Context, d *pipelineDeps) error {
	lines := make(chan agent.Line, d.cfg.Agent.QueueCapacity)
	linesToJoiner := make(chan agent.Line, d.cfg.Agent.QueueCapacity)
	joined := make(chan agent.Line, d.cfg.Agent.QueueCapacity)

	// srcCtx is a child of ctx, not ctx itself: every source stops the
	// instant ctx is canceled either way, because context.WithCancel
	// propagates a parent's cancellation, but this function also needs a
	// way to stop sources on its own, before ctx is ever canceled — see
	// cancelSrcCtx's call below.
	srcCtx, cancelSrcCtx := context.WithCancel(ctx)
	defer cancelSrcCtx()

	var sourceWG sync.WaitGroup
	sourceErrs := make([]error, len(d.sources))
	for i, src := range d.sources {
		sourceWG.Add(1)
		go func(i int, src agent.Source) {
			defer sourceWG.Done()
			// ctx.Err() (from either srcCtx or its parent being canceled) is
			// the expected outcome here and is not reported; anything else
			// is a genuine source failure.
			if runErr := src.Run(srcCtx, lines); runErr != nil && !errors.Is(runErr, context.Canceled) {
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
	go relayLines(drainCtx, lines, linesToJoiner, queueDepth, queueWait)

	joinerErrCh := make(chan error, 1)
	go func() { joinerErrCh <- d.joiner.Run(drainCtx, linesToJoiner, joined) }()

	shipperErrCh := make(chan error, 1)
	go func() { shipperErrCh <- d.shipper.Run(drainCtx, joined) }()

	// Nothing in this function ever closes linesToJoiner or joined before
	// ctx is done, so the Joiner and the Shipper are only ever supposed to
	// return here as a *consequence* of ctx firing. Either one returning
	// first is therefore a dead pipeline stage, not a stage finishing its
	// job: whichever one it is stops draining its input, that input's
	// channel fills, and everything upstream of it (the other stage, the
	// relay, every source) backs up and blocks in turn — silently, since
	// nothing here was watching for it. Treating this the same as a signal,
	// immediately, is what turns that hang into a bounded, reported
	// shutdown instead. joinerDone/shipperDone record which channel (if
	// either) already delivered here, so the sequence below never blocks
	// trying to read the same one twice.
	var (
		joinerDone, shipperDone bool
		joinErr, shipErr        error
		fatal                   error
	)
	select {
	case <-ctx.Done():
		d.log.Info("agent shutting down", slog.Duration("timeout", d.cfg.Node.ShutdownTimeout))
	case joinErr = <-joinerErrCh:
		joinerDone = true
		fatal = earlyStageErr("multiline joiner", joinErr)
		d.log.Error("agent shutting down: pipeline stage stopped unexpectedly",
			slog.String("stage", "multiline joiner"), slog.Any("error", joinErr))
		cancelSrcCtx()
	case shipErr = <-shipperErrCh:
		shipperDone = true
		fatal = earlyStageErr("shipper", shipErr)
		d.log.Error("agent shutting down: pipeline stage stopped unexpectedly",
			slog.String("stage", "shipper"), slog.Any("error", shipErr))
		cancelSrcCtx()
	}

	// One deadline bounds this whole sequence, mirroring cmd/collector: a
	// wedged stage still lets the process exit within ShutdownTimeout of the
	// signal (or of the fatal stage return above), rather than the process
	// hanging forever on any single step.
	shutdownCtx, cancelShutdown := context.WithTimeout(context.WithoutCancel(ctx), d.cfg.Node.ShutdownTimeout)
	defer cancelShutdown()
	context.AfterFunc(shutdownCtx, cancelDrain)

	var errs []error
	if fatal != nil {
		errs = append(errs, fatal)
	}

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
	close(lines)

	// Step 4: the relay sees lines close and closes linesToJoiner in turn;
	// the Joiner sees that close, flushes every held record, and returns.
	// Skipped when the Joiner is the stage that already triggered the fatal
	// path above — its result is already in errs via fatal, and it has
	// already stopped touching joined, so closing it below is still safe.
	if !joinerDone {
		joinErr = <-joinerErrCh
		errs = appendStageErr(errs, d.log, "multiline joiner", joinErr)
	}
	close(joined)

	// Step 5: the shipper sees joined close and runs its own shutdown --
	// flush accumulators, a bounded wait for outstanding acks, spool the
	// rest, commit the checkpoint (see Shipper.shutdown).
	if !shipperDone {
		shipErr = <-shipperErrCh
		errs = appendStageErr(errs, d.log, "shipper", shipErr)
	}

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
	//
	// This does not also flip an admin-served health endpoint to unhealthy:
	// unlike cmd/collector (whose httpapi serves observability.Health's
	// ReadyHandler), cmd/agent never constructs a Health registry at all,
	// and AdminServer (internal/observability/admin.go) serves only
	// /metrics and pprof — it has no readiness route to attach one to.
	// Wiring that in would mean adding a route to AdminServer itself, which
	// is outside internal/observability and therefore out of scope for this
	// fix; /metrics already carries agentMetrics for anything scraping it to
	// alert on instead.
	if adminErr := d.adminSrv.Shutdown(shutdownCtx); adminErr != nil {
		errs = append(errs, fmt.Errorf("admin shutdown: %w", adminErr))
	}

	return errors.Join(errs...)
}

// earlyStageErr builds the fatal error for a pipeline stage (the multiline
// joiner or the shipper) that returned before shutdown was ever requested.
// err may itself be nil: even a stage exiting cleanly this early is a bug
// worth reporting, not a success, because nothing upstream of it will ever
// stop sending on its own — see runPipeline's doc comment.
func earlyStageErr(stage string, err error) error {
	if err == nil {
		return fmt.Errorf("agent: %s stopped before shutdown was requested", stage)
	}
	// A stage that reports context.Canceled this early is still a fatal bug, but
	// the sentinel must not travel with the error: run() treats a result matching
	// context.Canceled as the ordinary signal-driven success path, and errors.Is
	// needs only one matching leaf anywhere in the tree to find it. Wrapping with
	// %w here would make this fatal condition exit zero. The cause is kept in the
	// message with %v, where it is still readable but no longer matchable.
	if errors.Is(err, context.Canceled) {
		return fmt.Errorf("agent: %s stopped before shutdown was requested: %v", stage, err) //nolint:errorlint // deliberate: see above
	}
	return fmt.Errorf("agent: %s stopped before shutdown was requested: %w", stage, err)
}

// appendStageErr adds a pipeline stage's shutdown-time error to errs,
// filtering out a bare context.Canceled first.
//
// context.Canceled is exactly what the Joiner and the Shipper return as the
// unremarkable consequence of drainCtx being canceled once the shutdown
// deadline elapses — expected, not a failure. Letting it into errs anyway
// would let a caller's later errors.Is(result, context.Canceled) find it and
// treat the whole joined result as a clean stop, even when another error
// collected alongside it (a failed final checkpoint commit, say) is
// genuinely not one: errors.Is only needs one matching leaf in the tree, and
// does not care how many other, real errors are joined next to it. Filtering
// here, at the point each stage's result is actually collected, is where the
// code knows the difference; reconstructing that distinction later from the
// aggregate is what the bug this function fixes did instead.
func appendStageErr(errs []error, log *slog.Logger, stage string, err error) []error {
	if err == nil || errors.Is(err, context.Canceled) {
		return errs
	}
	log.Error("shutdown: pipeline stage returned an error", slog.String("stage", stage), slog.Any("error", err))
	return append(errs, fmt.Errorf("%s: %w", stage, err))
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
	// owners records which source first claimed a given stream_id (a
	// LabelSet's ID(), the same identity Shipper keys its accumulators and
	// the collector keys its dimension table by — see shipper.go's accums
	// field), so a second source landing on the same one is caught here —
	// where the operator's config and both sources' names are still in
	// scope to name in the error — rather than downstream, where it would
	// silently collide sequence numbers with the first (see the finding
	// this check exists for: fileService alone cannot tell
	// /var/log/a/app.log from /var/log/b/app.log apart). LabelSet itself is
	// not a valid map key — Extra is a map — so ID() is used instead of the
	// LabelSet directly.
	owners := make(map[model.StreamID]string)

	// cfg.Service, if set, overrides every source's own service and
	// deliberately collapses them into one stream (see config.Agent.Service's
	// doc comment); otherwise service is the one the caller derived from the
	// source's own identity.
	addLabel := func(name, service string) error {
		if cfg.Service != "" {
			service = cfg.Service
		}
		ls := model.LabelSet{Service: service, Host: cfg.Host, Env: cfg.Env}
		// Skipped entirely when cfg.Service is set: that override
		// deliberately collapses every source into one LabelSet (see
		// config.Agent.Service's doc comment), so every "collision" it
		// produces is the intended outcome, not the bug this check exists
		// to catch.
		if cfg.Service == "" {
			if other, ok := owners[ls.ID()]; ok {
				return fmt.Errorf(
					"sources %q and %q both resolve to service %q and would collide on one stream_id: set %sAGENT_SERVICE to collapse them deliberately, or give one of them a distinguishing name",
					other, name, service, config.EnvPrefix)
			}
			owners[ls.ID()] = name
		}
		labels[name] = ls
		return nil
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
		if err := addLabel(src.Name(), fileService(path)); err != nil {
			return nil, nil, err
		}
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
			if err := addLabel(src.Name(), containerService(container, stream)); err != nil {
				return nil, nil, err
			}
			sources = append(sources, src)
		}
	}

	if cfg.Stdin {
		src := agent.NewStdinSource(os.Stdin)
		// No per-source rule is documented for stdin the way there is for a
		// file or a container stream — there is exactly one of it, and its
		// own Name() ("stdin") is already the obvious, unambiguous label.
		if err := addLabel(src.Name(), src.Name()); err != nil {
			return nil, nil, err
		}
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
