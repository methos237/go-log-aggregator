package model

import (
	"errors"
	"testing"
)

func TestLevelOrderingMatchesSeverity(t *testing.T) {
	t.Parallel()

	// The DSL compiles level>="warn" to a comparison on the stored integer, so this
	// ordering is a storage-format guarantee, not a convenience.
	ordered := []Level{LevelUnspecified, LevelTrace, LevelDebug, LevelInfo, LevelWarn, LevelError, LevelFatal}
	for i := 1; i < len(ordered); i++ {
		if ordered[i-1] >= ordered[i] {
			t.Fatalf("%s (%d) should sort before %s (%d)",
				ordered[i-1], ordered[i-1], ordered[i], ordered[i])
		}
	}
}

// TestLevelNumbersAreFrozen guards the on-disk format. These integers are stored in
// the logs table, so changing one silently reinterprets every existing row.
func TestLevelNumbersAreFrozen(t *testing.T) {
	t.Parallel()

	want := map[Level]int16{
		LevelUnspecified: 0,
		LevelTrace:       1,
		LevelDebug:       2,
		LevelInfo:        3,
		LevelWarn:        4,
		LevelError:       5,
		LevelFatal:       6,
	}
	for lvl, n := range want {
		if int16(lvl) != n {
			t.Errorf("%s = %d, want %d; renumbering breaks stored data", lvl, int16(lvl), n)
		}
	}
}

func TestParseLevel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in   string
		want Level
	}{
		{"trace", LevelTrace},
		{"TRACE", LevelTrace},
		{"  Debug  ", LevelDebug},
		{"info", LevelInfo},
		{"notice", LevelInfo},
		{"warn", LevelWarn},
		{"warning", LevelWarn},
		{"WRN", LevelWarn},
		{"error", LevelError},
		{"err", LevelError},
		{"fatal", LevelFatal},
		{"critical", LevelFatal},
		{"panic", LevelFatal},
		{"unspecified", LevelUnspecified},
	}

	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			got, err := ParseLevel(tc.in)
			if err != nil {
				t.Fatalf("ParseLevel(%q): %v", tc.in, err)
			}
			if got != tc.want {
				t.Fatalf("ParseLevel(%q) = %s, want %s", tc.in, got, tc.want)
			}
		})
	}
}

func TestParseLevelRejectsUnknown(t *testing.T) {
	t.Parallel()

	for _, in := range []string{"", "  ", "nope", "warn2", "5"} {
		if _, err := ParseLevel(in); !errors.Is(err, ErrInvalidLevel) {
			t.Errorf("ParseLevel(%q) error = %v, want ErrInvalidLevel", in, err)
		}
	}
}

func TestLevelTextRoundTrip(t *testing.T) {
	t.Parallel()

	for lvl := LevelUnspecified; lvl <= LevelFatal; lvl++ {
		text, err := lvl.MarshalText()
		if err != nil {
			t.Fatalf("MarshalText(%d): %v", int16(lvl), err)
		}
		var back Level
		if err := back.UnmarshalText(text); err != nil {
			t.Fatalf("UnmarshalText(%q): %v", text, err)
		}
		if back != lvl {
			t.Fatalf("round trip of %s produced %s", lvl, back)
		}
	}
}

func TestLevelValidAndStringForOutOfRange(t *testing.T) {
	t.Parallel()

	for _, lvl := range []Level{-1, 7, 1000} {
		if lvl.Valid() {
			t.Errorf("Level(%d).Valid() = true", int16(lvl))
		}
		// String must not panic on a value read back from a database row written by a
		// newer version of the schema.
		if got := lvl.String(); got == "" {
			t.Errorf("Level(%d).String() is empty", int16(lvl))
		}
		if _, err := lvl.MarshalText(); !errors.Is(err, ErrInvalidLevel) {
			t.Errorf("Level(%d).MarshalText() error = %v, want ErrInvalidLevel", int16(lvl), err)
		}
	}
}
