package observability

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"sync"
	"time"
)

// CheckFunc reports whether a dependency is usable. It must respect ctx and must
// not block indefinitely: a health check that hangs takes the whole probe with it.
type CheckFunc func(ctx context.Context) error

// Health tracks readiness dependencies.
//
// Liveness and readiness are deliberately different: liveness answers "is this
// process wedged, should the orchestrator restart it", readiness answers "can
// this process serve traffic right now". A dropped database connection makes a
// node unready, not dead — restarting it would not help.
type Health struct {
	mu      sync.RWMutex
	checks  map[string]CheckFunc
	timeout time.Duration
}

// NewHealth creates a registry. Each check gets at most timeout to answer.
func NewHealth(timeout time.Duration) *Health {
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	return &Health{
		checks:  make(map[string]CheckFunc),
		timeout: timeout,
	}
}

// Register adds or replaces a readiness check. Safe to call after serving starts,
// which is how components register themselves as they finish initializing.
func (h *Health) Register(name string, fn CheckFunc) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.checks[name] = fn
}

// Deregister removes a check, for components shutting down ahead of the server.
func (h *Health) Deregister(name string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.checks, name)
}

// result is the JSON shape returned by the readiness handler.
type result struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks,omitempty"`
}

// Check runs every registered check concurrently and returns the failures keyed
// by check name.
func (h *Health) Check(ctx context.Context) map[string]error {
	h.mu.RLock()
	checks := make(map[string]CheckFunc, len(h.checks))
	for name, fn := range h.checks {
		checks[name] = fn
	}
	timeout := h.timeout
	h.mu.RUnlock()

	if len(checks) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var (
		mu     sync.Mutex
		failed = make(map[string]error)
		wg     sync.WaitGroup
	)
	for name, fn := range checks {
		wg.Add(1)
		go func(name string, fn CheckFunc) {
			defer wg.Done()
			err := fn(ctx)
			if err == nil {
				return
			}
			mu.Lock()
			failed[name] = err
			mu.Unlock()
		}(name, fn)
	}
	wg.Wait()

	if len(failed) == 0 {
		return nil
	}
	return failed
}

// LiveHandler always reports healthy. Reaching it proves the process is
// scheduling goroutines and serving HTTP, which is the whole question liveness
// asks. Anything heavier here risks restart loops during a dependency outage.
func (h *Health) LiveHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, result{Status: "ok"})
	})
}

// ReadyHandler runs every check and returns 503 with per-check detail on failure.
func (h *Health) ReadyHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		failed := h.Check(r.Context())
		if len(failed) == 0 {
			writeJSON(w, http.StatusOK, result{Status: "ready"})
			return
		}

		names := make([]string, 0, len(failed))
		for name := range failed {
			names = append(names, name)
		}
		sort.Strings(names)

		detail := make(map[string]string, len(failed))
		for _, name := range names {
			detail[name] = failed[name].Error()
		}
		writeJSON(w, http.StatusServiceUnavailable, result{Status: "unready", Checks: detail})
	})
}

// ErrNotReady is the conventional error for a component that has not finished
// initializing yet, as opposed to one that has failed.
var ErrNotReady = errors.New("not ready")

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	// A failed write here means the client vanished; there is nothing useful to do
	// and the status line is already sent.
	_ = json.NewEncoder(w).Encode(body)
}
