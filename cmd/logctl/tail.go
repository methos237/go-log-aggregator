package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/coder/websocket"
)

// tailOptions are the flags of the tail subcommand.
type tailOptions struct {
	addr  string
	token string
	raw   bool
}

// tailRecord mirrors one /v1/tail message. Declared here rather than shared
// with internal/httpapi for the same reason queryResponse is: the CLI depends
// on the wire format, not the server.
type tailRecord struct {
	Time    time.Time         `json:"time"`
	Level   string            `json:"level"`
	Message string            `json:"message"`
	Fields  map[string]string `json:"fields"`
	Labels  map[string]string `json:"labels"`
	// Dropped is how many matches the server discarded for this client since
	// the previous message, because this client was not reading fast enough.
	Dropped int64 `json:"dropped"`
}

func tailCmd(args []string) error {
	fs := flag.NewFlagSet("tail", flag.ExitOnError)
	var opt tailOptions
	fs.StringVar(&opt.addr, "addr", "http://127.0.0.1:8080", "collector HTTP address")
	fs.StringVar(&opt.token, "token", os.Getenv("LOGAGG_HTTP_AUTH_TOKEN"), "bearer token (default $LOGAGG_HTTP_AUTH_TOKEN)")
	fs.BoolVar(&opt.raw, "json", false, "print each message as-is, one per line")
	fs.Usage = func() {
		_, _ = fmt.Fprint(fs.Output(), "usage: logctl tail [flags] '<query>'\n",
			`example: logctl tail '{service="api", level>="warn"} |= "timeout"'`, "\n",
			"selectors and line filters only; parser stages and aggregations need query\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		fs.Usage()
		return errors.New("exactly one query is required; quote it so the shell leaves it alone")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return opt.run(ctx, fs.Arg(0), os.Stdout, os.Stderr)
}

// run streams until ctx ends or the server closes. Ctrl-C is a clean exit;
// the server going away is reported, since the operator will want to
// reconnect.
func (o *tailOptions) run(ctx context.Context, query string, out, errOut io.Writer) error {
	addr := "ws" + strings.TrimPrefix(strings.TrimSuffix(o.addr, "/"), "http") + "/v1/tail?query=" + url.QueryEscape(query)
	hdr := http.Header{}
	hdr.Set("Authorization", "Bearer "+o.token)
	c, resp, err := websocket.Dial(ctx, addr, &websocket.DialOptions{HTTPHeader: hdr})
	if resp != nil && resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	if err != nil {
		if resp != nil {
			// The library keeps the body of a refused handshake, which is where
			// the server put the reason: a parse error with its position, or 401.
			return fmt.Errorf("%s: %s", resp.Status, handshakeError(resp))
		}
		return err
	}
	defer c.CloseNow() //nolint:errcheck // idempotent teardown
	// A record may carry a 64KB message and up to 64 structured fields, well
	// past the library's 32KB default; the server is the trusted party here.
	c.SetReadLimit(-1)

	for {
		_, data, err := c.Read(ctx)
		if err != nil {
			switch {
			case ctx.Err() != nil:
				return nil
			case websocket.CloseStatus(err) == websocket.StatusGoingAway:
				return errors.New("server is shutting down")
			default:
				return err
			}
		}
		if o.raw {
			if _, err = fmt.Fprintf(out, "%s\n", data); err != nil {
				return err
			}
			continue
		}
		var rec tailRecord
		if err = json.Unmarshal(data, &rec); err != nil {
			return fmt.Errorf("undecodable message %q: %w", data, err)
		}
		if rec.Dropped > 0 {
			_, _ = fmt.Fprintf(errOut, "warning: %d matching records dropped: this client is not reading fast enough\n", rec.Dropped)
		}
		if _, err = fmt.Fprintf(out, "%s  %-5s  %s%s\n", rec.Time.Format(time.RFC3339Nano), rec.Level, rec.Message, kv(rec.Fields)); err != nil {
			return err
		}
	}
}

// handshakeError extracts the server's message from a refused upgrade: the
// JSON error body when there is one, the raw text otherwise.
func handshakeError(resp *http.Response) string {
	if resp.Body == nil {
		return ""
	}
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	var e struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(raw, &e) == nil && e.Error != "" {
		return e.Error
	}
	return strings.TrimSpace(string(raw))
}
