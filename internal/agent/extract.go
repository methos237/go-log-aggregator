package agent

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"unicode/utf8"

	"github.com/jamespolk/go-log-aggregator/internal/model"
	"github.com/jamespolk/go-log-aggregator/internal/observability"
)

// ExtractConfig configures an Extractor.
type ExtractConfig struct {
	// Pattern extracts fields from named capture groups. Nil disables regex
	// extraction.
	Pattern *regexp.Regexp
	// JSON parses the line as a JSON object into fields.
	JSON bool
	// Metrics records mismatches, truncations and dropped fields. Nil builds
	// an unregistered set via NewMetrics(nil), which is what tests want;
	// production wiring supplies one built against the real registry.
	Metrics *Metrics
}

// Extractor pulls structured fields out of a line's bytes, honoring the
// model package's per-record field limits so a caller that ships whatever
// it returns never builds a record model.LogRecord.Validate would reject.
//
// An Extractor is safe for concurrent use: *regexp.Regexp is documented safe
// for concurrent matching, its own config is immutable after construction,
// and its counters are atomic.
type Extractor struct {
	pattern *regexp.Regexp
	json    bool
	metrics *Metrics
}

// NewExtractor validates cfg and returns an Extractor.
//
// A Pattern with no named capture groups is rejected rather than accepted as
// a silent no-op: such a pattern can never produce a field, and letting it
// through would mean an agent runs indefinitely with regex extraction
// "configured" and never extracting anything, which is a config mistake
// worth catching at startup rather than discovering as a permanent gap in
// the data.
func NewExtractor(cfg ExtractConfig) (*Extractor, error) {
	if cfg.Pattern != nil && !hasNamedGroup(cfg.Pattern) {
		return nil, fmt.Errorf("extract: pattern %q has no named capture groups, so it would never extract a field", cfg.Pattern.String())
	}

	metrics := cfg.Metrics
	if metrics == nil {
		metrics = NewMetrics(nil)
	}

	return &Extractor{pattern: cfg.Pattern, json: cfg.JSON, metrics: metrics}, nil
}

func hasNamedGroup(re *regexp.Regexp) bool {
	for _, name := range re.SubexpNames() {
		if name != "" {
			return true
		}
	}
	return false
}

// Fields extracts structured fields from a line's bytes. It returns nil when
// there is nothing to extract or extraction did not apply.
//
// Extraction never fails the line it is called on: a pattern that does not
// match, or bytes that are not valid JSON, are still a log line and Fields
// reports no fields for them rather than an error a caller might treat as a
// reason to drop the message. b is only read, never retained past the call
// and never mutated, so the line that arrived is exactly the line a caller
// still holds after this returns.
func (e *Extractor) Fields(b []byte) map[string]string {
	// Checked before doing anything else so that an agent running with
	// neither extractor configured — the common case for a line source that
	// only ships raw messages — never allocates a map it would immediately
	// discard.
	if e.pattern == nil && !e.json {
		return nil
	}

	var fields map[string]string
	if e.pattern != nil {
		fields = e.extractRegex(b, fields)
	}
	if e.json {
		// JSON runs second and wins any name collision with the regex pass.
		// A line that parses as a whole JSON object is telling you exactly
		// what its producer named and typed each value; a regex match is a
		// heuristic guess at a substring of the same text. When both apply
		// to the same field name, the parsed value is the more trustworthy
		// one.
		fields = e.extractJSON(b, fields)
	}
	if len(fields) == 0 {
		return nil
	}
	return fields
}

// extractRegex applies e.pattern to b, adding one field per named capture
// group that participated in the match.
func (e *Extractor) extractRegex(b []byte, fields map[string]string) map[string]string {
	loc := e.pattern.FindSubmatchIndex(b)
	if loc == nil {
		e.metrics.ExtractRegexMismatches.Inc()
		return fields
	}

	for i, name := range e.pattern.SubexpNames() {
		if name == "" {
			// Unnamed group: a field called "1" is useless, so these are
			// ignored rather than given a synthetic name.
			continue
		}
		start, end := loc[2*i], loc[2*i+1]
		if start < 0 {
			// This group did not participate in the match (it sits inside
			// an alternation or an optional quantifier that did not take
			// this branch). Contributing no field here, instead of an empty
			// one, is what lets a caller later tell "this group did not
			// fire" apart from "it fired and matched an empty string" —
			// those are different facts about the line.
			continue
		}
		fields = e.addField(fields, name, string(b[start:end]))
	}
	return fields
}

// extractJSON parses b as a JSON object and adds one field per top-level
// key.
//
// Parsing is all-or-nothing: keys are collected into a local slice and only
// merged into fields once the whole object has been consumed without error.
// A document that fails to parse partway through contributes nothing at
// all, matching a document that never parsed in the first place, rather than
// leaving behind whatever prefix happened to decode before the error.
func (e *Extractor) extractJSON(b []byte, fields map[string]string) map[string]string {
	dec := json.NewDecoder(bytes.NewReader(b))

	tok, err := dec.Token()
	if err != nil {
		e.metrics.ExtractJSONUnparsed.Inc()
		return fields
	}
	delim, ok := tok.(json.Delim)
	if !ok || delim != '{' {
		// Valid JSON that is not an object — an array, a bare string, a
		// number, a bool, null — has no keys to name, so it is counted the
		// same as unparseable input: in both cases JSON extraction found
		// nothing to contribute.
		e.metrics.ExtractJSONUnparsed.Inc()
		return fields
	}

	type pair struct{ name, value string }
	var pairs []pair
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			e.metrics.ExtractJSONUnparsed.Inc()
			return fields
		}
		key, ok := keyTok.(string)
		if !ok {
			// Unreachable for a genuine JSON object — encoding/json's
			// grammar guarantees object keys decode as strings — but guarded
			// rather than asserted, since a bare type assertion here would
			// panic on any future decoder behavior this file does not
			// control.
			e.metrics.ExtractJSONUnparsed.Inc()
			return fields
		}
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			e.metrics.ExtractJSONUnparsed.Inc()
			return fields
		}
		pairs = append(pairs, pair{key, jsonValueString(raw)})
	}
	if _, err := dec.Token(); err != nil { // the closing '}'
		e.metrics.ExtractJSONUnparsed.Inc()
		return fields
	}

	for _, p := range pairs {
		fields = e.addField(fields, p.name, p.value)
	}
	return fields
}

// jsonValueString renders one top-level JSON value as a field value.
func jsonValueString(raw json.RawMessage) string {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return ""
	}

	switch trimmed[0] {
	case '{', '[':
		// A nested object or array becomes its compact JSON encoding rather
		// than a flattened set of dotted keys. Flattening invents field
		// names the log never had and lets one nested value blow past
		// MaxFields on its own; compacting only strips insignificant
		// whitespace, so it is lossless and bounded by the value's own size.
		var buf bytes.Buffer
		if err := json.Compact(&buf, trimmed); err != nil {
			// The decoder already validated trimmed as well-formed JSON to
			// get this far, so Compact cannot fail on it in practice. Fall
			// back to the raw bytes rather than losing the value if it
			// somehow does.
			return string(trimmed)
		}
		return buf.String()
	case '"':
		var s string
		if err := json.Unmarshal(trimmed, &s); err != nil {
			return string(trimmed)
		}
		return s
	default:
		// true, false, null, and numbers: the JSON text is already the
		// natural string form, used verbatim rather than re-formatted
		// through Go's number types, which can change "1.50" to "1.5" and
		// would make the stored value not the one the producer wrote.
		//
		// null becomes the literal string "null", not "" and not a dropped
		// field. Either of those would make a JSON null indistinguishable
		// from a field that was empty or absent — exactly the distinction
		// this package already preserves for a non-participating regex
		// group in extractRegex.
		return string(trimmed)
	}
}

// truncateAtRuneBoundary cuts s to at most limit bytes without splitting a
// multi-byte rune.
//
// A plain s[:limit] can land mid-rune, and these values end up in a JSONB
// column: encoding/json coerces the invalid bytes to U+FFFD rather than
// failing, so nothing breaks, but the stored value then carries a replacement
// character where the producer wrote a real one. Backing up to the boundary
// costs a few bytes of a value that was already over the limit and keeps what
// is stored readable. The limit stays a byte limit, because that is what
// model.MaxFieldValueLen and LogRecord.Validate measure.
func truncateAtRuneBoundary(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	cut := limit
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}

// addField inserts name/value into fields, applying the limits
// model.LogRecord.Validate enforces so that a record built from this map is
// never rejected for a reason extraction could have avoided. Checked against
// the model package's constants directly, not restated numbers, so this
// cannot drift from the validator that actually rejects records.
//
// Preferring to keep data over discarding it shapes every branch here: a
// too-long name is skipped rather than truncated, because truncating it
// could collide with another field's name and silently overwrite real data;
// a too-long value is truncated rather than dropped, because a truncated
// value still carries information and a dropped one carries none.
//
// A skipped name and a MaxFields overflow both discard a candidate field
// outright — nothing about it survives — so both are recorded on the shared
// observability.RecordsDropped family (reasonFieldNameSkipped,
// reasonFieldOverflow) rather than on a package-local counter. A truncated
// value is different: the field still exists, just shortened, so it gets its
// own Metrics.ExtractValuesTruncated instead of counting as a drop.
func (e *Extractor) addField(fields map[string]string, name, value string) map[string]string {
	if name == "" || len(name) > model.MaxFieldNameLen {
		e.metrics.RecordsDropped.WithLabelValues(observability.ComponentAgent, reasonFieldNameSkipped).Inc()
		return fields
	}

	// A collision (name already present) always overwrites: it never
	// consumes a new slot, so it is exempt from the MaxFields cap below.
	if _, exists := fields[name]; !exists && len(fields) >= model.MaxFields {
		e.metrics.RecordsDropped.WithLabelValues(observability.ComponentAgent, reasonFieldOverflow).Inc()
		return fields
	}

	if len(value) > model.MaxFieldValueLen {
		value = truncateAtRuneBoundary(value, model.MaxFieldValueLen)
		e.metrics.ExtractValuesTruncated.Inc()
	}

	if fields == nil {
		fields = make(map[string]string)
	}
	fields[name] = value
	return fields
}
