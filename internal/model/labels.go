package model

import (
	"encoding/binary"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/cespare/xxhash/v2"
)

// StreamID identifies a label set. It is the xxhash64 of the canonical encoding
// of that label set, reinterpreted as a signed integer so it fits Postgres
// BIGINT (which has no unsigned counterpart).
//
// Reinterpreting rather than truncating keeps all 64 bits, so the collision
// probability stays at the birthday bound for 64-bit hashes: negligible for the
// label cardinality a log aggregator sees. Collisions are not catastrophic
// either — two label sets would merge into one stream — but they would be
// confusing, which is why this is 64 bits and not 32.
type StreamID int64

// LabelSet is the set of labels identifying a stream.
//
// Service, Host and Env are promoted out of the map because every query filters
// on them and they are stored as real columns. Extra holds everything else and
// lands in a JSONB column.
type LabelSet struct {
	Service string
	Host    string
	Env     string
	Extra   map[string]string
}

// Label-shaped limits. These exist to bound memory and index size per §8 of the
// roadmap: a hostile or misconfigured agent must not be able to make the streams
// table or its GIN index grow without limit.
const (
	// MaxExtraLabels caps labels beyond service/host/env. Label cardinality is
	// what kills log stores, so this is deliberately small.
	MaxExtraLabels = 32
	// MaxLabelNameLen bounds a label name.
	MaxLabelNameLen = 128
	// MaxLabelValueLen bounds a label value. Values are part of the stream
	// identity, so long ones multiply cardinality rather than just cost bytes.
	MaxLabelValueLen = 1024
)

// ID returns the stream ID for the label set.
func (ls LabelSet) ID() StreamID {
	// The cast is the whole point: keep the bits, change the interpretation.
	return StreamID(xxhash.Sum64(ls.canonical(nil))) //nolint:gosec // intentional reinterpretation, see StreamID
}

// AppendCanonical appends the canonical byte encoding of the label set to dst
// and returns the extended slice. Exposed so hot paths can reuse a buffer.
//
// The encoding is length-prefixed rather than delimited. With a delimiter,
// {service: "a", host: "b|c"} and {service: "a|b", host: "c"} would encode
// identically and collide into one stream; there is no byte a label value cannot
// contain, so no delimiter is safe. Length prefixes make the encoding injective.
//
// Extra labels are sorted by name so map iteration order cannot change the ID.
func (ls LabelSet) AppendCanonical(dst []byte) []byte {
	return ls.canonical(dst)
}

func (ls LabelSet) canonical(dst []byte) []byte {
	dst = appendField(dst, ls.Service)
	dst = appendField(dst, ls.Host)
	dst = appendField(dst, ls.Env)

	dst = binary.AppendUvarint(dst, uint64(len(ls.Extra)))
	for _, name := range slices.Sorted(maps.Keys(ls.Extra)) {
		dst = appendField(dst, name)
		dst = appendField(dst, ls.Extra[name])
	}
	return dst
}

func appendField(dst []byte, s string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(s)))
	return append(dst, s...)
}

// String renders the label set in the query DSL's selector syntax, which makes
// log lines and error messages copy-pasteable into a query.
func (ls LabelSet) String() string {
	var b strings.Builder
	b.WriteByte('{')
	fmt.Fprintf(&b, "service=%q, host=%q, env=%q", ls.Service, ls.Host, ls.Env)
	for _, name := range slices.Sorted(maps.Keys(ls.Extra)) {
		fmt.Fprintf(&b, ", %s=%q", name, ls.Extra[name])
	}
	b.WriteByte('}')
	return b.String()
}

// Clone returns a deep copy. Callers that keep a LabelSet past the lifetime of
// the request that produced it need this: Extra is a map, so the shallow copy a
// plain assignment gives would alias it.
func (ls LabelSet) Clone() LabelSet {
	out := ls
	if ls.Extra != nil {
		out.Extra = maps.Clone(ls.Extra)
	}
	return out
}

// Validate reports the first problem with the label set.
//
// It returns on the first failure rather than accumulating: this runs per batch
// on the ingest path, and a caller that is going to reject the batch anyway
// gains nothing from a full list.
func (ls LabelSet) Validate() error {
	for _, f := range [...]struct {
		name, value string
	}{
		{"service", ls.Service},
		{"host", ls.Host},
		{"env", ls.Env},
	} {
		if f.value == "" {
			return fmt.Errorf("label %s: %w", f.name, ErrMissingLabel)
		}
		if len(f.value) > MaxLabelValueLen {
			return fmt.Errorf("label %s: %w: %d bytes exceeds %d", f.name, ErrLabelTooLong, len(f.value), MaxLabelValueLen)
		}
	}

	if len(ls.Extra) > MaxExtraLabels {
		return fmt.Errorf("%w: %d labels exceeds %d", ErrTooManyLabels, len(ls.Extra), MaxExtraLabels)
	}
	for name, value := range ls.Extra {
		if err := validateLabelName(name); err != nil {
			return err
		}
		switch name {
		case "service", "host", "env":
			// Otherwise a selector on {service="x"} would be ambiguous: it could
			// mean the column or the JSONB key, and the two could disagree.
			return fmt.Errorf("extra label %q: %w", name, ErrReservedLabel)
		}
		if len(value) > MaxLabelValueLen {
			return fmt.Errorf("extra label %q: %w: %d bytes exceeds %d", name, ErrLabelTooLong, len(value), MaxLabelValueLen)
		}
	}
	return nil
}

// validateLabelName enforces the identifier shape the query DSL's grammar uses
// for labels. Keeping storage and grammar in agreement means every stored label
// is addressable by a query; allowing arbitrary bytes would create labels that
// exist but cannot be selected.
func validateLabelName(name string) error {
	if name == "" {
		return fmt.Errorf("label name: %w", ErrMissingLabel)
	}
	if len(name) > MaxLabelNameLen {
		return fmt.Errorf("label name %q: %w: %d bytes exceeds %d", name, ErrLabelTooLong, len(name), MaxLabelNameLen)
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c == '_':
		case i > 0 && c >= '0' && c <= '9':
		default:
			return fmt.Errorf("label name %q: %w", name, ErrInvalidLabelName)
		}
	}
	return nil
}
