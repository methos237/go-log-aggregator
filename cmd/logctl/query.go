package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"
)

// queryOptions are the flags of the query subcommand.
type queryOptions struct {
	addr    string
	token   string
	since   time.Duration
	start   string
	end     string
	limit   int
	forward bool
	raw     bool
}

// queryResponse mirrors the /v1/query body. Declared here rather than shared
// with internal/httpapi so the CLI depends on the wire format, not the server.
type queryResponse struct {
	Records []struct {
		Time    time.Time         `json:"time"`
		Level   string            `json:"level"`
		Message string            `json:"message"`
		Fields  map[string]string `json:"fields"`
	} `json:"records"`
	Points []struct {
		Bucket time.Time         `json:"bucket"`
		Value  float64           `json:"value"`
		Labels map[string]string `json:"labels"`
	} `json:"points"`
	Source    string  `json:"source"`
	Streams   int     `json:"streams"`
	ElapsedMS float64 `json:"elapsed_ms"`
	Error     string  `json:"error"`
}

func queryCmd(args []string) error {
	fs := flag.NewFlagSet("query", flag.ExitOnError)
	var opt queryOptions
	fs.StringVar(&opt.addr, "addr", "http://127.0.0.1:8080", "collector HTTP address")
	fs.StringVar(&opt.token, "token", os.Getenv("LOGAGG_HTTP_AUTH_TOKEN"), "bearer token (default $LOGAGG_HTTP_AUTH_TOKEN)")
	fs.DurationVar(&opt.since, "since", time.Hour, "look back this far from now (ignored when -start is set)")
	fs.StringVar(&opt.start, "start", "", "range start, RFC 3339")
	fs.StringVar(&opt.end, "end", "", "range end, RFC 3339 (default now)")
	fs.IntVar(&opt.limit, "limit", 100, "maximum rows")
	fs.BoolVar(&opt.forward, "forward", false, "oldest first instead of newest first")
	fs.BoolVar(&opt.raw, "json", false, "print the response body as-is")
	fs.Usage = func() {
		_, _ = fmt.Fprint(fs.Output(), "usage: logctl query [flags] '<query>'\n",
			`example: logctl query -since 15m '{service="api", level>="warn"} |= "timeout"'`, "\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		fs.Usage()
		return errors.New("exactly one query is required; quote it so the shell leaves it alone")
	}

	body, err := opt.body(fs.Arg(0), time.Now())
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSuffix(opt.addr, "/")+"/v1/query", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+opt.token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return err
	}

	var out queryResponse
	if err = json.Unmarshal(raw, &out); err != nil {
		return fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(raw)))
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: %s", resp.Status, out.Error)
	}
	if opt.raw {
		_, err = os.Stdout.Write(raw)
		return err
	}
	return out.print(os.Stdout, os.Stderr)
}

// body builds the request body. The server applies its own defaults, but the
// CLI fills the range in explicitly so -since means "from my clock", which is
// what an operator on a laptop with a skewed VM expects.
func (o *queryOptions) body(query string, now time.Time) ([]byte, error) {
	req := map[string]any{"query": query, "limit": o.limit}
	end := now
	if o.end != "" {
		var err error
		if end, err = time.Parse(time.RFC3339, o.end); err != nil {
			return nil, fmt.Errorf("-end: %w", err)
		}
	}
	start := end.Add(-o.since)
	if o.start != "" {
		var err error
		if start, err = time.Parse(time.RFC3339, o.start); err != nil {
			return nil, fmt.Errorf("-start: %w", err)
		}
	}
	req["start"], req["end"] = start.UTC(), end.UTC()
	if o.forward {
		req["direction"] = "forward"
	}
	return json.Marshal(req)
}

// print writes records or points one per line, and a summary to stderr so a
// piped stdout stays clean.
func (r *queryResponse) print(out, summary io.Writer) error {
	rows := len(r.Points)
	for _, p := range r.Points {
		if _, err := fmt.Fprintf(out, "%s  %g%s\n", p.Bucket.Format(time.RFC3339), p.Value, kv(p.Labels)); err != nil {
			return err
		}
	}
	if r.Points == nil {
		rows = len(r.Records)
		for _, rec := range r.Records {
			if _, err := fmt.Fprintf(out, "%s  %-5s  %s%s\n", rec.Time.Format(time.RFC3339Nano), rec.Level, rec.Message, kv(rec.Fields)); err != nil {
				return err
			}
		}
	}
	_, err := fmt.Fprintf(summary, "%d rows from %s (%d streams) in %.1fms\n", rows, r.Source, r.Streams, r.ElapsedMS)
	return err
}

// kv renders labels or fields as "  k=v k=v" in key order, empty when there are none.
func kv(m map[string]string) string {
	if len(m) == 0 {
		return ""
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		b.WriteString(" " + k + "=" + m[k])
	}
	return " " + b.String()
}
