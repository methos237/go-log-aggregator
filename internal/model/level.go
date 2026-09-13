package model

import (
	"fmt"
	"slices"
	"strings"
)

// Level is a log severity.
//
// The underlying type is int16 to match the SMALLINT column it is stored in. The
// numeric values are part of the on-disk format and are shared with the protobuf
// enum in api/proto/logagg/v1: they may be appended to, never renumbered.
//
// Severities are ordered, which is what lets the query DSL write level>="warn".
type Level int16

// Severity values, ordered from least to most severe.
const (
	LevelUnspecified Level = 0
	LevelTrace       Level = 1
	LevelDebug       Level = 2
	LevelInfo        Level = 3
	LevelWarn        Level = 4
	LevelError       Level = 5
	LevelFatal       Level = 6
)

// levelNames is indexed by Level. Keep in sync with the constants above.
var levelNames = [...]string{
	LevelUnspecified: "unspecified",
	LevelTrace:       "trace",
	LevelDebug:       "debug",
	LevelInfo:        "info",
	LevelWarn:        "warn",
	LevelError:       "error",
	LevelFatal:       "fatal",
}

// LevelNames returns the canonical name of every level, in severity order.
func LevelNames() []string { return slices.Clone(levelNames[:]) }

// levelAliases maps every accepted spelling to a level. Agents emit whatever
// their source used, so "warning", "err" and "critical" all have to land
// somewhere; refusing them would drop real logs over a synonym.
var levelAliases = map[string]Level{
	// Round-trips MarshalText output. Without it, marshaling a record with no
	// level and reading it back would fail.
	"unspecified": LevelUnspecified,

	"trace":     LevelTrace,
	"trc":       LevelTrace,
	"verbose":   LevelTrace,
	"debug":     LevelDebug,
	"dbg":       LevelDebug,
	"info":      LevelInfo,
	"inf":       LevelInfo,
	"notice":    LevelInfo,
	"warn":      LevelWarn,
	"wrn":       LevelWarn,
	"warning":   LevelWarn,
	"error":     LevelError,
	"err":       LevelError,
	"eror":      LevelError,
	"fatal":     LevelFatal,
	"critical":  LevelFatal,
	"crit":      LevelFatal,
	"panic":     LevelFatal,
	"emergency": LevelFatal,
	"alert":     LevelFatal,
}

// Valid reports whether l is a known severity. LevelUnspecified is valid: it is
// how a record whose source had no level is stored, and dropping those would
// lose plain unstructured logs.
func (l Level) Valid() bool {
	return l >= LevelUnspecified && int(l) < len(levelNames)
}

// String returns the canonical lowercase name.
func (l Level) String() string {
	if !l.Valid() {
		return fmt.Sprintf("level(%d)", int16(l))
	}
	return levelNames[l]
}

// MarshalText implements encoding.TextMarshaler so levels render as names in
// JSON API responses rather than as opaque integers.
func (l Level) MarshalText() ([]byte, error) {
	if !l.Valid() {
		return nil, fmt.Errorf("marshal level: %w: %d", ErrInvalidLevel, int16(l))
	}
	return []byte(l.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (l *Level) UnmarshalText(text []byte) error {
	parsed, err := ParseLevel(string(text))
	if err != nil {
		return err
	}
	*l = parsed
	return nil
}

// ParseLevel resolves a level name or alias, case-insensitively.
func ParseLevel(s string) (Level, error) {
	lvl, ok := levelAliases[strings.ToLower(strings.TrimSpace(s))]
	if !ok {
		return LevelUnspecified, fmt.Errorf("parse level %q: %w", s, ErrInvalidLevel)
	}
	return lvl, nil
}
