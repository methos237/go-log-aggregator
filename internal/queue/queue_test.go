package queue

import (
	"strings"
	"testing"
)

func TestSubject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		env, service string
		want         string
	}{
		{
			name:    "plain labels pass through",
			env:     "prod",
			service: "checkout-api",
			want:    "logs.prod.checkout-api",
		},
		{
			// A dot in a label would otherwise add a token and push the message
			// outside the stream's logs.> filter.
			name:    "dots become underscores",
			env:     "prod",
			service: "checkout.api",
			want:    "logs.prod.checkout_api",
		},
		{
			// The dangerous case: a service named for a wildcard.
			name:    "wildcards are neutralized",
			env:     ">",
			service: "*",
			want:    "logs._._",
		},
		{
			name:    "spaces and control bytes are replaced",
			env:     "pre prod",
			service: "sync\tsvc\n",
			want:    "logs.pre_prod.sync_svc_",
		},
		{
			name:    "multi-byte utf8 is replaced per byte",
			env:     "dev",
			service: "café",
			want:    "logs.dev.caf__",
		},
		{
			// Cannot happen through Validate, which requires a non-empty service, but
			// an empty token would produce an unaddressable subject if it did.
			name:    "empty labels get a placeholder",
			env:     "",
			service: "",
			want:    "logs._._",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := Subject("logs", tt.env, tt.service); got != tt.want {
				t.Errorf("Subject(logs, %q, %q) = %q, want %q", tt.env, tt.service, got, tt.want)
			}
		})
	}
}

func TestFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		env, service string
		want         string
	}{
		{"prod", "api", "tail.prod.api"},
		{"", "api", "tail.*.api"},
		{"prod", "", "tail.prod.*"},
		{"", "", "tail.*.*"},
		// Sanitized like Subject, so the filter lands on the same token the
		// publisher chose; a literal wildcard in a label must not widen it.
		{"prod", "a.b", "tail.prod.a_b"},
		{">", "*", "tail._._"},
	}
	for _, tt := range tests {
		if got := Filter("tail", tt.env, tt.service); got != tt.want {
			t.Errorf("Filter(tail, %q, %q) = %q, want %q", tt.env, tt.service, got, tt.want)
		}
	}
}

func TestSubjectTruncatesLongTokens(t *testing.T) {
	t.Parallel()

	// Label values are bounded at 1KB, so without truncation the routing key would
	// carry two of them in every message.
	long := strings.Repeat("a", 4096)
	got := Subject("logs", "prod", long)

	want := "logs.prod." + strings.Repeat("a", MaxSubjectTokenLen)
	if got != want {
		t.Errorf("token was not truncated to %d bytes: got %d bytes", MaxSubjectTokenLen, len(got))
	}
}

func TestSubjectTokenCountIsFixed(t *testing.T) {
	t.Parallel()

	// The stream filter is prefix.> and a consumer may filter on prefix.env.*, both
	// of which break if a label can add or remove a token.
	for _, service := range []string{"a", "a.b.c.d", ".", "..", strings.Repeat(".", 100)} {
		if n := strings.Count(Subject("logs", "prod", service), "."); n != 2 {
			t.Errorf("Subject with service %q produced %d dots, want 2", service, n)
		}
	}
}
