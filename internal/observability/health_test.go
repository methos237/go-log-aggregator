package observability

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestLiveHandlerIgnoresFailingChecks(t *testing.T) {
	h := NewHealth(time.Second)
	h.Register("db", func(context.Context) error { return errors.New("connection refused") })

	rec := httptest.NewRecorder()
	h.LiveHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	// A dead dependency must not make the process look dead, or the orchestrator
	// restarts it in a loop while the real problem is elsewhere.
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestReadyHandlerNoChecks(t *testing.T) {
	rec := httptest.NewRecorder()
	NewHealth(time.Second).ReadyHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestReadyHandlerReportsEveryFailure(t *testing.T) {
	h := NewHealth(time.Second)
	h.Register("db", func(context.Context) error { return errors.New("connection refused") })
	h.Register("queue", func(context.Context) error { return errors.New("not ready") })
	h.Register("ring", func(context.Context) error { return nil })

	rec := httptest.NewRecorder()
	h.ReadyHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}

	var body struct {
		Status string            `json:"status"`
		Checks map[string]string `json:"checks"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body %q: %v", rec.Body.String(), err)
	}
	if body.Status != "unready" {
		t.Errorf("status field = %q, want %q", body.Status, "unready")
	}
	if len(body.Checks) != 2 {
		t.Errorf("checks = %v, want the 2 failing ones only", body.Checks)
	}
	if got := body.Checks["db"]; got != "connection refused" {
		t.Errorf("checks[db] = %q, want the underlying error", got)
	}
	if _, ok := body.Checks["ring"]; ok {
		t.Error("passing check should not appear in the failure detail")
	}
}

func TestCheckTimesOutSlowCheck(t *testing.T) {
	h := NewHealth(50 * time.Millisecond)
	h.Register("slow", func(ctx context.Context) error {
		// A check that outlives the probe budget must fail the probe rather than
		// hold the connection open until the client gives up.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
			return nil
		}
	})

	start := time.Now()
	failed := h.Check(context.Background())
	elapsed := time.Since(start)

	if len(failed) != 1 {
		t.Fatalf("failed = %v, want the slow check to fail", failed)
	}
	if elapsed > time.Second {
		t.Errorf("Check took %s, want it bounded by the 50ms timeout", elapsed)
	}
}

func TestDeregister(t *testing.T) {
	h := NewHealth(time.Second)
	h.Register("db", func(context.Context) error { return errors.New("boom") })
	h.Deregister("db")

	if failed := h.Check(context.Background()); len(failed) != 0 {
		t.Errorf("failed = %v, want none after deregistering", failed)
	}
}

func TestRegisterConcurrentWithCheck(t *testing.T) {
	// Registration happens as subsystems finish initializing, which is
	// concurrent with probes already arriving. Run under -race.
	h := NewHealth(time.Second)
	done := make(chan struct{})

	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			h.Register("db", func(context.Context) error { return nil })
			h.Deregister("db")
		}
	}()
	for i := 0; i < 200; i++ {
		h.Check(context.Background())
	}
	<-done
}
