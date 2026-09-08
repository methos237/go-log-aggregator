// Command logctl is the operator CLI.
//
// This phase adds one subcommand, `send`, which ships lines from stdin or from a
// flag into a collector. It exists so the ingest path can be exercised by hand
// without a load generator or a real agent — "does this cluster accept a log line"
// should be one command. Query, tail and cluster subcommands arrive in phases 4 to 6.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
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

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "logctl: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `logctl controls a go-log-aggregator cluster.

usage: logctl <command> [flags]

commands:
  send       ship log lines to a collector
  version    print version and exit

run "logctl <command> -h" for a command's flags.
`)
}

func run(args []string) error {
	if len(args) == 0 {
		usage()
		return errors.New("no command given")
	}

	switch args[0] {
	case "send":
		return sendCmd(args[1:])
	case "version", "-version", "--version":
		fmt.Println(version.String("logctl"))
		return nil
	case "help", "-h", "--help":
		usage()
		return nil
	default:
		usage()
		return fmt.Errorf("unknown command %q", args[0])
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
	fs := flag.NewFlagSet("send", flag.ExitOnError)
	var opt sendOptions
	fs.StringVar(&opt.addr, "addr", "127.0.0.1:9095", "collector ingest address")
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
	if err := fs.Parse(args); err != nil {
		return err
	}

	if opt.service == "" {
		return errors.New("-service is required: it is part of the stream identity")
	}
	level, err := parseLevel(opt.level)
	if err != nil {
		return err
	}
	if opt.batchSize < 1 {
		return fmt.Errorf("batch size must be at least 1, got %d", opt.batchSize)
	}

	// Validated locally before a connection is made, so a bad label set is a message
	// about the label set rather than a rejected batch to interpret.
	labels := model.LabelSet{Service: opt.service, Host: opt.host, Env: opt.env}
	if err = labels.Validate(); err != nil {
		return fmt.Errorf("labels: %w", err)
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

	fmt.Printf("%d accepted, %d rejected\n", accepted, rejected)
	if rejected > 0 {
		return fmt.Errorf("%d records were rejected", rejected)
	}
	return nil
}

// report prints one ack, including the detail, which is the only place the reason
// for a rejection is visible to a human.
func report(ack *logaggv1.Ack) {
	line := fmt.Sprintf("%-24s %-12s accepted=%d rejected=%d",
		ack.GetBatchId(), code(ack.GetCode()), ack.GetAccepted(), ack.GetRejected())
	if detail := ack.GetDetail(); detail != "" {
		line += ": " + detail
	}
	fmt.Println(line)
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
	levels := map[string]logaggv1.Level{
		"trace": logaggv1.Level_LEVEL_TRACE,
		"debug": logaggv1.Level_LEVEL_DEBUG,
		"info":  logaggv1.Level_LEVEL_INFO,
		"warn":  logaggv1.Level_LEVEL_WARN,
		"error": logaggv1.Level_LEVEL_ERROR,
		"fatal": logaggv1.Level_LEVEL_FATAL,
	}
	level, ok := levels[strings.ToLower(name)]
	if !ok {
		return logaggv1.Level_LEVEL_UNSPECIFIED,
			fmt.Errorf("unknown level %q: use trace, debug, info, warn, error or fatal", name)
	}
	return level, nil
}

func hostname() string {
	name, err := os.Hostname()
	if err != nil || name == "" {
		return "localhost"
	}
	return name
}
