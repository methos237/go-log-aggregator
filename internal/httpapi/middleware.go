package httpapi

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strings"
	"time"

	"github.com/jamespolk/go-log-aggregator/internal/config"
)

type middleware func(http.Handler) http.Handler

// statusRecorder captures the status code and byte count for access logging.
type statusRecorder struct {
	http.ResponseWriter
	status int
	bytes  int
}

// Unwrap exposes the underlying writer, the http.ResponseController convention,
// so the WebSocket upgrade can reach the Hijacker this wrapper would otherwise
// hide. The access log still sees the 101.
func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

func (r *statusRecorder) WriteHeader(code int) {
	if r.status == 0 {
		r.status = code
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	n, err := r.ResponseWriter.Write(b)
	r.bytes += n
	return n, err
}

// requestLog emits one line per request. Health probes log at debug level so a
// 1s Kubernetes or compose probe interval does not bury everything else.
func requestLog(log *slog.Logger) middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w}

			next.ServeHTTP(rec, r)

			level := slog.LevelInfo
			switch {
			case isProbe(r.URL.Path) && rec.status < http.StatusBadRequest:
				level = slog.LevelDebug
			case rec.status >= http.StatusInternalServerError:
				level = slog.LevelError
			}

			log.Log(r.Context(), level, "http request",
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.Int("status", rec.status),
				slog.Int("bytes", rec.bytes),
				slog.Duration("duration", time.Since(start)),
				slog.String("remote", r.RemoteAddr),
			)
		})
	}
}

func isProbe(path string) bool {
	return path == "/healthz" || path == "/readyz"
}

// recoverPanic keeps one bad request from taking down the process. Without it, a
// nil map access in a query handler kills every in-flight ingest stream too,
// because they share the process.
func recoverPanic(log *slog.Logger) middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				v := recover()
				if v == nil {
					return
				}
				// ErrAbortHandler is the documented way for a handler to drop a
				// connection deliberately; it is not a bug and must propagate.
				if err, ok := v.(error); ok && errors.Is(err, http.ErrAbortHandler) {
					panic(v)
				}
				log.Error("panic in http handler",
					slog.String("method", r.Method),
					slog.String("path", r.URL.Path),
					slog.Any("panic", v),
					slog.String("stack", string(debug.Stack())),
				)
				writeError(w, http.StatusInternalServerError, "internal error")
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// bearerAuth requires "Authorization: Bearer <token>" on every request. The
// scheme is case-insensitive as RFC 9110 says; the token compare is
// constant-time so response timing does not leak how much of a guess was
// right. An empty configured token refuses everything: the API is off until
// an operator sets one, never open by accident.
func bearerAuth(token string) middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reject := func(msg string) {
				w.Header().Set("WWW-Authenticate", `Bearer realm="logagg"`)
				writeError(w, http.StatusUnauthorized, msg)
			}
			if token == "" {
				reject("query API disabled: set " + config.EnvPrefix + "HTTP_AUTH_TOKEN")
				return
			}
			scheme, got, _ := strings.Cut(r.Header.Get("Authorization"), " ")
			if !strings.EqualFold(scheme, "Bearer") || subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
				reject("invalid or missing bearer token")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

type errorBody struct {
	Error string `json:"error"`
}

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorBody{Error: msg})
}

func notFound(w http.ResponseWriter, _ *http.Request) {
	writeError(w, http.StatusNotFound, "not found")
}
