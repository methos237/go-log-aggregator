package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/jamespolk/go-log-aggregator/internal/observability"
	"github.com/jamespolk/go-log-aggregator/internal/queue"
	"github.com/jamespolk/go-log-aggregator/internal/queue/queuetest"
)

// The drop reason is how an operator tells "the broker is slow" from "the agent went
// away", so each failure has to land under its own reason rather than whichever case
// the switch happens to reach first.
func TestPublishFailureReasons(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		err        error
		wantReason string
		wantDetail string
	}{
		{
			name:       "a shed batch",
			err:        errShed,
			wantReason: reasonBufferFull,
			wantDetail: "saturated",
		},
		{
			name:       "a shutting-down collector",
			err:        errPipelineClosed,
			wantReason: reasonShutdown,
			wantDetail: "shutting down",
		},
		{
			// Conn.Publish wraps both the kind and DeadlineExceeded on a timeout.
			name:       "a broker that did not ack in time",
			err:        fmt.Errorf("publish: %w: %w", queue.ErrUnavailable, context.DeadlineExceeded),
			wantReason: reasonQueueRefused,
			wantDetail: "queue rejected",
		},
		{
			name:       "a full stream",
			err:        fmt.Errorf("publish: %w: %w", queue.ErrOverloaded, errors.New("max bytes")),
			wantReason: reasonQueueRefused,
			wantDetail: "queue rejected",
		},
		{
			name:       "an unsendable payload",
			err:        fmt.Errorf("publish: %w: %w", queue.ErrTooLarge, errors.New("max payload")),
			wantReason: reasonQueueTooLarge,
			wantDetail: "queue rejected",
		},
		{
			// A genuine client cancel reaches here from the pipeline's own select,
			// with no queue kind attached.
			name:       "a client that hung up",
			err:        context.Canceled,
			wantReason: reasonClientGone,
			wantDetail: "client went away",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			reg := prometheus.NewRegistry()
			metrics := NewMetrics(reg)
			svc := &service{
				pipeline: newPipeline(context.Background(), testIngestConfig(),
					&queuetest.Publisher{}, metrics, slog.New(slog.DiscardHandler)),
				log:     slog.New(slog.DiscardHandler),
				metrics: metrics,
			}

			ack := svc.publishFailed("b1", "logs.dev.api", 42, 10, 10, tt.err)

			if got := counter(t, reg, "logagg_records_dropped_total", map[string]string{
				"component": observability.ComponentIngest,
				"reason":    tt.wantReason,
			}); got != 10 {
				t.Errorf("records_dropped_total{ingest,%s} = %v, want 10", tt.wantReason, got)
			}
			if !strings.Contains(ack.GetDetail(), tt.wantDetail) {
				t.Errorf("detail = %q, want it to mention %q", ack.GetDetail(), tt.wantDetail)
			}
			if ack.GetAccepted() != 0 || ack.GetRejected() != 10 {
				t.Errorf("accepted/rejected = %d/%d, want 0/10", ack.GetAccepted(), ack.GetRejected())
			}
		})
	}
}
