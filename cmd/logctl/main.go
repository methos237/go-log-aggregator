// Command logctl is the operator CLI.
//
// `send` ships lines from stdin or from a flag into a collector, so "does this
// cluster accept a log line" is one command without a load generator or a real
// agent. `query` runs a DSL query against the HTTP API and prints the rows.
// `tail` streams matching records over the WebSocket endpoint as they arrive.
//
// Exit codes: 0 on success, 2 for a usage error (bad flag, missing argument,
// unknown command), 1 for anything that failed at runtime. Records and rows go
// to stdout; progress, summaries and warnings go to stderr.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	logaggv1 "github.com/jamespolk/go-log-aggregator/api/proto/logagg/v1"
	"github.com/jamespolk/go-log-aggregator/internal/ingest"
	"github.com/jamespolk/go-log-aggregator/internal/model"
	"github.com/jamespolk/go-log-aggregator/internal/version"
)

// usageError marks an error the operator caused by how the command was invoked,
// as opposed to one the cluster caused. main maps it to exit code 2, the code
// the flag package conventionally uses for a bad flag, so scripts can tell the
// two apart. It is a type rather than a sentinel so the classification never
// leaks into the message.
type usageError struct{ err error }

func (e usageError) Error() string { return e.err.Error() }
func (e usageError) Unwrap() error { return e.err }

// usagef builds a usageError.
func usagef(format string, a ...any) error { return usageError{fmt.Errorf(format, a...)} }

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "logctl: %v\n", err)
		var ue usageError
		if errors.As(err, &ue) {
			os.Exit(2)
		}
		os.Exit(1)
	}
}

// parseFlags parses args and decides where the output goes: requested help
// prints the usage to stdout and reports helped, a bad flag prints the usage to
// stderr and returns a usageError carrying the flag package's message.
func parseFlags(fs *flag.FlagSet, args []string) (helped bool, err error) {
	fs.SetOutput(io.Discard)
	err = fs.Parse(args)
	switch {
	case errors.Is(err, flag.ErrHelp):
		fs.SetOutput(os.Stdout)
		fs.Usage()
		return true, nil
	case err != nil:
		fs.SetOutput(os.Stderr)
		fs.Usage()
		return false, usageError{err}
	}
	fs.SetOutput(os.Stderr)
	return false, nil
}

// usage writes the top-level help. It goes to stdout when the operator asked
// for it and to stderr when it accompanies an error.
func usage(w io.Writer) {
	_, _ = fmt.Fprint(w, `logctl controls a go-log-aggregator cluster.

usage: logctl <command> [flags]

commands:
  send       ship log lines to a collector
  query      run a query against a collector and print the rows
  tail       stream matching records from a collector as they arrive
  version    print version and exit

run "logctl <command> -h" or "logctl help <command>" for a command's flags.
`)
}

func run(args []string) error {
	if len(args) == 0 {
		usage(os.Stderr)
		return usagef("no command given")
	}

	switch args[0] {
	case "send":
		return sendCmd(args[1:])
	case "query":
		return queryCmd(args[1:])
	case "tail":
		return tailCmd(args[1:])
	case "version", "-version", "--version":
		fmt.Println(version.String("logctl"))
		return nil
	case "help", "-h", "--help":
		if len(args) > 1 {
			switch args[1] {
			case "send", "query", "tail":
				return run([]string{args[1], "-h"})
			}
		}
		usage(os.Stdout)
		return nil
	default:
		usage(os.Stderr)
		return usagef("unknown command %q", args[0])
	}
}

// sendOptions are the flags of the send subcommand.
type sendOptions struct {
	addr      string
	service   string
	host      string
	env       string
	level     string
	message   string
	file      string
	batchSize int
	certFile  string
	keyFile   string
	caFile    string
}

func sendCmd(args []string) error {
	fs := flag.NewFlagSet("send", flag.ContinueOnError)
	var opt sendOptions
	fs.StringVar(&opt.addr, "addr", "127.0.0.1:9095", "collector gRPC ingest address, host:port")
	fs.StringVar(&opt.service, "service", "", "service label (required)")
	fs.StringVar(&opt.host, "host", hostname(), "host label")
	fs.StringVar(&opt.env, "env", "dev", "env label")
	fs.StringVar(&opt.level, "level", "info", "level for every record: trace|debug|info|warn|error|fatal")
	fs.StringVar(&opt.message, "message", "", "send this single line instead of reading input")
	fs.StringVar(&opt.file, "file", "-", `read lines from this file ("-" is stdin)`)
	fs.IntVar(&opt.batchSize, "batch-size", 500, "records per batch")
	fs.StringVar(&opt.certFile, "tls-cert", "", "client certificate (mTLS)")
	fs.StringVar(&opt.keyFile, "tls-key", "", "client key (mTLS)")
	fs.StringVar(&opt.caFile, "tls-ca", "", "CA that signed the collector certificate")
	fs.Usage = func() {
		_, _ = fmt.Fprint(fs.Output(), "usage: logctl send [flags] < lines\n",
			"example: echo \"hello\" | logctl send -service demo\n",
			"one record per input line, all with the same labels and level\n")
		fs.PrintDefaults()
	}
	if helped, err := parseFlags(fs, args); helped || err != nil {
		return err
	}
	if fs.NArg() != 0 {
		fs.Usage()
		return usagef("send takes no arguments; pipe lines in or use -message")
	}

	if opt.service == "" {
		return usagef("-service is required: it is part of the stream identity")
	}
	level, err := parseLevel(opt.level)
	if err != nil {
		return usageError{err}
	}
	if opt.batchSize < 1 {
		return usagef("batch size must be at least 1, got %d", opt.batchSize)
	}

	// Validated locally before a connection is made, so a bad label set is a message
	// about the label set rather than a rejected batch to interpret.
	labels := model.LabelSet{Service: opt.service, Host: opt.host, Env: opt.env}
	if err = labels.Validate(); err != nil {
		return usagef("labels: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	lines, closeInput, err := input(&opt)
	if err != nil {
		return err
	}
	defer closeInput()

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

	return ship(ctx, client, &opt, labels, level, lines)
}

// input returns a line reader for the configured source plus its closer.
func input(opt *sendOptions) (*bufio.Scanner, func(), error) {
	if opt.message != "" {
		return bufio.NewScanner(strings.NewReader(opt.message)), func() {}, nil
	}
	if opt.file == "-" {
		return bufio.NewScanner(os.Stdin), func() {}, nil
	}
	f, err := os.Open(opt.file)
	if err != nil {
		return nil, nil, fmt.Errorf("open %s: %w", opt.file, err)
	}
	return bufio.NewScanner(f), func() { _ = f.Close() }, nil
}

// ship reads lines, batches them and reports each ack.
//
// One batch is sent and acked before the next is built. That makes this slower than
// loadgen by design: an operator sending lines by hand wants to see which batch was
// accepted, in order, and a pipelined version would report them interleaved.
func ship(
	ctx context.Context,
	client *ingest.Client,
	opt *sendOptions,
	labels model.LabelSet,
	level logaggv1.Level,
	lines *bufio.Scanner,
) error {
	stream, err := client.Stream(ctx)
	if err != nil {
		return err
	}

	var (
		batch    []*logaggv1.LogRecord
		seq      int64
		batchNum int
		accepted uint32
		rejected uint32
	)

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		id := fmt.Sprintf("logctl-%d", batchNum)
		if sendErr := stream.Send(&logaggv1.LogBatch{
			BatchId: id,
			Labels:  labels.Proto(),
			Records: batch,
		}); sendErr != nil {
			return fmt.Errorf("send batch %s: %w", id, sendErr)
		}
		ack, recvErr := stream.Recv()
		if recvErr != nil {
			return fmt.Errorf("await ack for %s: %w", id, recvErr)
		}
		accepted += ack.GetAccepted()
		rejected += ack.GetRejected()
		report(ack)

		batchNum++
		batch = batch[:0]
		return nil
	}

	// Buffered scanning has a line-length ceiling; raising it here rather than
	// letting a long line silently end the send.
	lines.Buffer(make([]byte, 0, 64*1024), model.MaxMessageLen)

	for lines.Scan() {
		if ctx.Err() != nil {
			break
		}
		batch = append(batch, &logaggv1.LogRecord{
			TimeUnixNano: time.Now().UnixNano(),
			Seq:          seq,
			Level:        level,
			Message:      lines.Text(),
		})
		seq++
		if len(batch) >= opt.batchSize {
			if flushErr := flush(); flushErr != nil {
				return flushErr
			}
		}
	}
	if err = lines.Err(); err != nil {
		return fmt.Errorf("read input: %w", err)
	}
	if flushErr := flush(); flushErr != nil {
		return flushErr
	}
	if err = stream.CloseSend(); err != nil {
		return fmt.Errorf("close send: %w", err)
	}

	fmt.Fprintf(os.Stderr, "%d accepted, %d rejected\n", accepted, rejected)
	if rejected > 0 {
		return fmt.Errorf("%d records were rejected", rejected)
	}
	return nil
}

// report prints one ack to stderr, including the detail, which is the only place
// the reason for a rejection is visible to a human. Everything send prints is
// progress or a summary, so like the other commands it keeps stdout empty.
func report(ack *logaggv1.Ack) {
	line := fmt.Sprintf("%-24s %-12s accepted=%d rejected=%d",
		ack.GetBatchId(), code(ack.GetCode()), ack.GetAccepted(), ack.GetRejected())
	if detail := ack.GetDetail(); detail != "" {
		line += ": " + detail
	}
	fmt.Fprintln(os.Stderr, line)
}

func code(c logaggv1.AckCode) string {
	return strings.ToLower(strings.TrimPrefix(c.String(), "ACK_CODE_"))
}

// parseLevel maps a flag value to a wire level.
//
// Rejecting an unknown name here rather than sending LEVEL_UNSPECIFIED matters: the
// collector would reject every record for an invalid level, and the operator would
// be looking at a rejection count instead of a typo.
func parseLevel(name string) (logaggv1.Level, error) {
	level, ok := logaggv1.Level_value["LEVEL_"+strings.ToUpper(name)]
	if !ok || level == 0 {
		return logaggv1.Level_LEVEL_UNSPECIFIED,
			fmt.Errorf("unknown level %q: use trace, debug, info, warn, error or fatal", name)
	}
	return logaggv1.Level(level), nil
}

func hostname() string {
	name, err := os.Hostname()
	if err != nil || name == "" {
		return "localhost"
	}
	return name
}
