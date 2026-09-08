package ingest

import (
	"strings"
	"testing"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"

	logaggv1 "github.com/jamespolk/go-log-aggregator/api/proto/logagg/v1"
)

// A detail cut mid-rune is invalid UTF-8, which a proto3 string field rejects at
// marshal time — so a rejected batch would break its own stream instead of being
// answered.
func TestTruncateKeepsValidUTF8(t *testing.T) {
	t.Parallel()

	// Multi-byte runes positioned so a byte-boundary cut lands inside one.
	for offset := 0; offset < 4; offset++ {
		s := strings.Repeat("a", offset) + strings.Repeat("日", MaxAckDetailLen)
		got := truncate(s, MaxAckDetailLen)

		if len(got) > MaxAckDetailLen {
			t.Errorf("offset %d: len = %d, want at most %d", offset, len(got), MaxAckDetailLen)
		}
		if !utf8.ValidString(got) {
			t.Errorf("offset %d: truncation produced invalid UTF-8", offset)
		}
		if !strings.HasSuffix(got, "...") {
			t.Errorf("offset %d: truncation is not marked", offset)
		}

		// The real consequence: the ack has to survive marshaling.
		if _, err := proto.Marshal(&logaggv1.Ack{Detail: got}); err != nil {
			t.Errorf("offset %d: marshaling the ack failed: %v", offset, err)
		}
	}
}

// A defensive helper must not have a lower bound its callers are expected to know.
func TestTruncateHandlesTinyLimits(t *testing.T) {
	t.Parallel()

	for maxLen := 0; maxLen <= 4; maxLen++ {
		got := truncate("abcdefgh", maxLen)
		if len(got) > maxLen {
			t.Errorf("truncate(_, %d) returned %d bytes", maxLen, len(got))
		}
	}
}
