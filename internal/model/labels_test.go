package model

import (
	"errors"
	"strings"
	"testing"
)

func TestLabelSetIDIsStableAndOrderIndependent(t *testing.T) {
	t.Parallel()

	base := LabelSet{
		Service: "api",
		Host:    "node-1",
		Env:     "prod",
		Extra:   map[string]string{"region": "eu-west-1", "version": "1.4.2", "tier": "edge"},
	}
	// Same content, built in a different order. Go randomizes map iteration, so if
	// the canonical encoding depended on it this would flake rather than fail
	// cleanly -- which is exactly why the encoding sorts.
	reordered := LabelSet{
		Service: "api",
		Host:    "node-1",
		Env:     "prod",
		Extra:   map[string]string{"tier": "edge", "version": "1.4.2", "region": "eu-west-1"},
	}

	if base.ID() != reordered.ID() {
		t.Fatalf("map order changed the ID: %d != %d", base.ID(), reordered.ID())
	}

	// Repeated calls must agree: the ID is persisted, so instability would split one
	// logical stream across rows.
	for i := 0; i < 8; i++ {
		if got := base.ID(); got != base.ID() {
			t.Fatalf("ID is not deterministic: %d", got)
		}
	}
}

// TestLabelSetIDIsInjective is the regression test for the length-prefixed
// encoding. With a delimiter-based encoding every pair below would collide, and two
// unrelated services would silently merge into one stream.
func TestLabelSetIDIsInjective(t *testing.T) {
	t.Parallel()

	pairs := []struct {
		name string
		a, b LabelSet
	}{
		{
			name: "value boundary shifted between fields",
			a:    LabelSet{Service: "a", Host: "bc", Env: "d"},
			b:    LabelSet{Service: "ab", Host: "c", Env: "d"},
		},
		{
			name: "delimiter-looking bytes inside a value",
			a:    LabelSet{Service: "a", Host: "b|c", Env: "d"},
			b:    LabelSet{Service: "a|b", Host: "c", Env: "d"},
		},
		{
			name: "null bytes inside a value",
			a:    LabelSet{Service: "a\x00b", Host: "c", Env: "d"},
			b:    LabelSet{Service: "a", Host: "b\x00c", Env: "d"},
		},
		{
			name: "extra label key/value boundary shifted",
			a:    LabelSet{Service: "s", Host: "h", Env: "e", Extra: map[string]string{"ab": "c"}},
			b:    LabelSet{Service: "s", Host: "h", Env: "e", Extra: map[string]string{"a": "bc"}},
		},
		{
			name: "absent extra label versus empty value",
			a:    LabelSet{Service: "s", Host: "h", Env: "e"},
			b:    LabelSet{Service: "s", Host: "h", Env: "e", Extra: map[string]string{"x": ""}},
		},
	}

	for _, tc := range pairs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.a.ID() == tc.b.ID() {
				t.Fatalf("distinct label sets collided on ID %d:\n  a = %s\n  b = %s",
					tc.a.ID(), tc.a, tc.b)
			}
		})
	}
}

func TestLabelSetCloneDoesNotAlias(t *testing.T) {
	t.Parallel()

	original := LabelSet{Service: "api", Host: "h", Env: "e", Extra: map[string]string{"k": "v"}}
	clone := original.Clone()
	clone.Extra["k"] = "mutated"

	if original.Extra["k"] != "v" {
		t.Fatalf("clone aliases the original map: got %q", original.Extra["k"])
	}
	if clone.ID() == original.ID() {
		t.Fatal("mutating the clone should have changed its ID")
	}
}

func TestLabelSetCloneOfNilExtraStaysNil(t *testing.T) {
	t.Parallel()

	clone := LabelSet{Service: "s", Host: "h", Env: "e"}.Clone()
	if clone.Extra != nil {
		t.Fatalf("expected nil Extra, got %v", clone.Extra)
	}
}

func TestLabelSetValidate(t *testing.T) {
	t.Parallel()

	valid := func() LabelSet {
		return LabelSet{Service: "api", Host: "node-1", Env: "prod", Extra: map[string]string{"region": "eu"}}
	}

	tests := []struct {
		name    string
		mutate  func(*LabelSet)
		wantErr error
	}{
		{name: "valid", mutate: func(*LabelSet) {}},
		{name: "no extra labels", mutate: func(ls *LabelSet) { ls.Extra = nil }},
		{name: "empty service", mutate: func(ls *LabelSet) { ls.Service = "" }, wantErr: ErrMissingLabel},
		{name: "empty host", mutate: func(ls *LabelSet) { ls.Host = "" }, wantErr: ErrMissingLabel},
		{name: "empty env", mutate: func(ls *LabelSet) { ls.Env = "" }, wantErr: ErrMissingLabel},
		{
			name:    "oversized service",
			mutate:  func(ls *LabelSet) { ls.Service = strings.Repeat("s", MaxLabelValueLen+1) },
			wantErr: ErrLabelTooLong,
		},
		{
			name:    "oversized extra value",
			mutate:  func(ls *LabelSet) { ls.Extra["region"] = strings.Repeat("v", MaxLabelValueLen+1) },
			wantErr: ErrLabelTooLong,
		},
		{
			name:    "oversized extra name",
			mutate:  func(ls *LabelSet) { ls.Extra[strings.Repeat("n", MaxLabelNameLen+1)] = "v" },
			wantErr: ErrLabelTooLong,
		},
		{
			name: "too many extra labels",
			mutate: func(ls *LabelSet) {
				for i := 0; i <= MaxExtraLabels; i++ {
					ls.Extra[string(rune('a'+i%26))+string(rune('a'+i/26))] = "v"
				}
			},
			wantErr: ErrTooManyLabels,
		},
		{
			name:    "extra label shadowing a column",
			mutate:  func(ls *LabelSet) { ls.Extra["service"] = "other" },
			wantErr: ErrReservedLabel,
		},
		{
			name:    "extra label name starting with a digit",
			mutate:  func(ls *LabelSet) { ls.Extra["1region"] = "v" },
			wantErr: ErrInvalidLabelName,
		},
		{
			name:    "extra label name with a dash",
			mutate:  func(ls *LabelSet) { ls.Extra["region-id"] = "v" },
			wantErr: ErrInvalidLabelName,
		},
		{
			name:    "empty extra label name",
			mutate:  func(ls *LabelSet) { ls.Extra[""] = "v" },
			wantErr: ErrMissingLabel,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ls := valid()
			tc.mutate(&ls)

			err := ls.Validate()
			switch {
			case tc.wantErr == nil && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tc.wantErr != nil && err == nil:
				t.Fatalf("expected error %v, got nil", tc.wantErr)
			case tc.wantErr != nil && !errors.Is(err, tc.wantErr):
				t.Fatalf("expected error %v, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestLabelSetStringIsSelectorShaped(t *testing.T) {
	t.Parallel()

	ls := LabelSet{Service: "api", Host: "h", Env: "prod", Extra: map[string]string{"b": "2", "a": "1"}}
	// Extra labels sorted, so the output is stable enough to assert on and to paste
	// into a query.
	want := `{service="api", host="h", env="prod", a="1", b="2"}`
	if got := ls.String(); got != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestCanonicalReusesBuffer(t *testing.T) {
	t.Parallel()

	ls := LabelSet{Service: "api", Host: "h", Env: "e"}
	buf := make([]byte, 0, 64)
	first := ls.canonical(buf)
	second := ls.canonical(first)

	if len(second) != 2*len(first) {
		t.Fatalf("expected appending twice to double the length: %d then %d", len(first), len(second))
	}
	if string(second[:len(first)]) != string(first) {
		t.Fatal("appending overwrote the existing prefix")
	}
}

func BenchmarkLabelSetID(b *testing.B) {
	ls := LabelSet{
		Service: "checkout-api",
		Host:    "ip-10-0-14-233.eu-west-1.compute.internal",
		Env:     "prod",
		Extra:   map[string]string{"region": "eu-west-1", "version": "2.11.4", "tier": "edge", "az": "eu-west-1b"},
	}
	for b.Loop() {
		_ = ls.ID()
	}
}
