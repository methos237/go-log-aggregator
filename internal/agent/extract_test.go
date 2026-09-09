package agent

import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/jamespolk/go-log-aggregator/internal/model"
	"github.com/jamespolk/go-log-aggregator/internal/observability"
)

// mustExtractor builds an Extractor or fails the test immediately.
func mustExtractor(t *testing.T, cfg ExtractConfig) *Extractor {
	t.Helper()
	e, err := NewExtractor(cfg)
	if err != nil {
		t.Fatalf("NewExtractor(%+v): %v", cfg, err)
	}
	return e
}

func TestExtractorRegexNamedGroups(t *testing.T) {
	// The first group is unnamed and must be ignored. The third group is
	// named but optional, and the input never gives it anything to match.
	re := regexp.MustCompile(`^(\d+) (?P<level>\w+)(?: (?P<extra>\w+))?$`)
	e := mustExtractor(t, ExtractConfig{Pattern: re})

	fields := e.Fields([]byte("42 info"))
	if fields == nil {
		t.Fatal("Fields = nil, want a map with \"level\"")
	}
	if got, want := fields["level"], "info"; got != want {
		t.Errorf(`fields["level"] = %q, want %q`, got, want)
	}
	if _, ok := fields["1"]; ok {
		t.Error(`fields["1"] present: unnamed group must be ignored`)
	}
	if _, ok := fields["extra"]; ok {
		t.Error(`fields["extra"] present: a non-participating optional group must contribute no field, not an empty one`)
	}
	if len(fields) != 1 {
		t.Errorf("len(fields) = %d, want 1 (only \"level\")", len(fields))
	}
}

func TestExtractorRegexParticipatingEmptyMatch(t *testing.T) {
	// A group that DID participate but matched zero characters is different
	// from one that never fired at all, and must still produce a field.
	re := regexp.MustCompile(`^(?P<a>x*)(?P<b>y)$`)
	e := mustExtractor(t, ExtractConfig{Pattern: re})

	fields := e.Fields([]byte("y"))
	got, ok := fields["a"]
	if !ok {
		t.Fatal(`fields["a"] absent, want present with an empty value (the group matched, just matched nothing)`)
	}
	if got != "" {
		t.Errorf(`fields["a"] = %q, want ""`, got)
	}
}

func TestExtractorRegexNoMatch(t *testing.T) {
	re := regexp.MustCompile(`^(?P<level>ERROR)`)
	e := mustExtractor(t, ExtractConfig{Pattern: re})

	line := []byte("this line does not match the pattern at all")
	original := append([]byte(nil), line...)

	fields := e.Fields(line)
	if fields != nil {
		t.Errorf("Fields = %v, want nil for a non-matching line", fields)
	}
	if got := counterValue(t, e.metrics.ExtractRegexMismatches); got != 1 {
		t.Errorf("ExtractRegexMismatches = %v, want 1", got)
	}
	if string(line) != string(original) {
		t.Errorf("line bytes changed: got %q, want %q (extraction must never touch the message)", line, original)
	}
}

func TestExtractorJSONObject(t *testing.T) {
	e := mustExtractor(t, ExtractConfig{JSON: true})

	line := []byte(`{"level":"info","count":42,"pi":3.5,"ok":true,"missing":null,"tags":["a","b"],"meta":{"x":1,"y":"z"}}`)
	fields := e.Fields(line)
	if fields == nil {
		t.Fatal("Fields = nil, want extracted fields")
	}

	want := map[string]string{
		"level":   "info",
		"count":   "42",
		"pi":      "3.5",
		"ok":      "true",
		"missing": "null",
		"tags":    `["a","b"]`,
		"meta":    `{"x":1,"y":"z"}`,
	}
	for name, wantVal := range want {
		if got := fields[name]; got != wantVal {
			t.Errorf("fields[%q] = %q, want %q", name, got, wantVal)
		}
	}
	if len(fields) != len(want) {
		t.Errorf("len(fields) = %d, want %d (got %v)", len(fields), len(want), fields)
	}
}

func TestExtractorJSONInvalid(t *testing.T) {
	e := mustExtractor(t, ExtractConfig{JSON: true})

	fields := e.Fields([]byte(`{"a": 1, "b": }`))
	if fields != nil {
		t.Errorf("Fields = %v, want nil for invalid JSON", fields)
	}
	if got := counterValue(t, e.metrics.ExtractJSONUnparsed); got != 1 {
		t.Errorf("ExtractJSONUnparsed = %v, want 1", got)
	}
}

func TestExtractorJSONNonObject(t *testing.T) {
	tests := []struct {
		name string
		line string
	}{
		{"array", `["a","b","c"]`},
		{"bare string", `"just a string"`},
		{"bare number", `42`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := mustExtractor(t, ExtractConfig{JSON: true})
			fields := e.Fields([]byte(tt.line))
			if fields != nil {
				t.Errorf("Fields(%q) = %v, want nil: no keys to name", tt.line, fields)
			}
		})
	}
}

func TestExtractorBothConfiguredJSONWinsCollision(t *testing.T) {
	// The line is valid JSON starting with '{', so a pattern that captures
	// just its first character gives the regex pass a "level" field of "{".
	// JSON extraction of the same line gives "level" the value "info". Per
	// the documented collision rule, JSON runs second and wins, so the final
	// value must be "info", deterministically, every time.
	re := regexp.MustCompile(`^(?P<level>.)`)
	e := mustExtractor(t, ExtractConfig{Pattern: re, JSON: true})

	line := []byte(`{"level":"info"}`)
	for i := 0; i < 10; i++ {
		fields := e.Fields(line)
		if got, want := fields["level"], "info"; got != want {
			t.Fatalf("run %d: fields[\"level\"] = %q, want %q (JSON must win the collision)", i, got, want)
		}
	}
}

func TestExtractorNeitherConfiguredNoAlloc(t *testing.T) {
	e := mustExtractor(t, ExtractConfig{})
	line := []byte("plain log line, nothing configured")

	if fields := e.Fields(line); fields != nil {
		t.Errorf("Fields = %v, want nil", fields)
	}

	allocs := testing.AllocsPerRun(1000, func() {
		_ = e.Fields(line)
	})
	if allocs != 0 {
		t.Errorf("AllocsPerRun = %v, want 0 when neither Pattern nor JSON is configured", allocs)
	}
}

func TestExtractorFieldNameLimit(t *testing.T) {
	e := mustExtractor(t, ExtractConfig{JSON: true})

	okName := strings.Repeat("n", model.MaxFieldNameLen)
	tooLongName := strings.Repeat("n", model.MaxFieldNameLen+1)
	line := []byte(`{"` + okName + `":"a","` + tooLongName + `":"b"}`)

	fields := e.Fields(line)
	if _, ok := fields[okName]; !ok {
		t.Errorf("field of exactly MaxFieldNameLen (%d) was dropped, want kept", model.MaxFieldNameLen)
	}
	if _, ok := fields[tooLongName]; ok {
		t.Errorf("field of MaxFieldNameLen+1 was kept, want skipped")
	}
	if got := counterValue(t, e.metrics.RecordsDropped.WithLabelValues(observability.ComponentAgent, reasonFieldNameSkipped)); got != 1 {
		t.Errorf("RecordsDropped{reason=field_name_skipped} = %v, want 1", got)
	}
}

func TestExtractorFieldValueLimit(t *testing.T) {
	e := mustExtractor(t, ExtractConfig{JSON: true})

	okValue := strings.Repeat("v", model.MaxFieldValueLen)
	longValue := strings.Repeat("v", model.MaxFieldValueLen+1)
	line := []byte(`{"a":"` + okValue + `","b":"` + longValue + `"}`)

	fields := e.Fields(line)
	if got := fields["a"]; got != okValue {
		t.Errorf("value of exactly MaxFieldValueLen was altered: len(got)=%d, want %d unchanged", len(got), model.MaxFieldValueLen)
	}
	got, ok := fields["b"]
	if !ok {
		t.Fatal(`fields["b"] absent, want truncated and kept`)
	}
	if len(got) != model.MaxFieldValueLen {
		t.Errorf("len(fields[\"b\"]) = %d, want exactly %d", len(got), model.MaxFieldValueLen)
	}
	if got != longValue[:model.MaxFieldValueLen] {
		t.Error(`fields["b"] is not a prefix-truncation of the original value`)
	}
	if got := counterValue(t, e.metrics.ExtractValuesTruncated); got != 1 {
		t.Errorf("ExtractValuesTruncated = %v, want 1", got)
	}
}

// buildJSONObject returns a JSON object literal with n top-level keys,
// "k00".."k{n-1}", in ascending source order, so which keys survive a
// MaxFields cap is deterministic rather than dependent on map iteration
// order or a marshaler's key-sorting behavior.
func buildJSONObject(n int) []byte {
	var b strings.Builder
	b.WriteByte('{')
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(`"k`)
		if i < 10 {
			b.WriteByte('0')
		}
		b.WriteString(strconv.Itoa(i))
		b.WriteString(`":"v`)
		b.WriteString(strconv.Itoa(i))
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return []byte(b.String())
}

func TestExtractorFieldOverflow(t *testing.T) {
	const extra = 5
	e := mustExtractor(t, ExtractConfig{JSON: true})

	line := buildJSONObject(model.MaxFields + extra)
	fields := e.Fields(line)

	if len(fields) != model.MaxFields {
		t.Errorf("len(fields) = %d, want exactly model.MaxFields (%d)", len(fields), model.MaxFields)
	}
	if got := counterValue(t, e.metrics.RecordsDropped.WithLabelValues(observability.ComponentAgent, reasonFieldOverflow)); got != float64(extra) {
		t.Errorf("RecordsDropped{reason=field_overflow} = %v, want %d", got, extra)
	}
}

func TestExtractorEmptyFieldNameSkipped(t *testing.T) {
	e := mustExtractor(t, ExtractConfig{JSON: true})

	fields := e.Fields([]byte(`{"":"x","b":"y"}`))
	if _, ok := fields[""]; ok {
		t.Error(`fields[""] present, want skipped`)
	}
	if got, want := fields["b"], "y"; got != want {
		t.Errorf(`fields["b"] = %q, want %q`, got, want)
	}
	if got := counterValue(t, e.metrics.RecordsDropped.WithLabelValues(observability.ComponentAgent, reasonFieldNameSkipped)); got != 1 {
		t.Errorf("RecordsDropped{reason=field_name_skipped} = %v, want 1", got)
	}
}

// TestExtractorRoundTripValidate proves the limits were actually honored, not
// just matched by eye against model's constants: it builds a real
// model.LogRecord from extracted fields for each over-limit case and runs it
// through the same Validate the collector uses to reject records.
func TestExtractorRoundTripValidate(t *testing.T) {
	now := time.Now()

	cases := []struct {
		name string
		line []byte
	}{
		{"name at and over the limit", []byte(`{"` + strings.Repeat("n", model.MaxFieldNameLen) + `":"a","` + strings.Repeat("n", model.MaxFieldNameLen+1) + `":"b"}`)},
		{"value at and over the limit", []byte(`{"a":"` + strings.Repeat("v", model.MaxFieldValueLen) + `","b":"` + strings.Repeat("v", model.MaxFieldValueLen+1) + `"}`)},
		{"more than MaxFields keys", buildJSONObject(model.MaxFields + 5)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := mustExtractor(t, ExtractConfig{JSON: true})
			fields := e.Fields(tc.line)

			record := model.LogRecord{
				Time:    now,
				Level:   model.LevelInfo,
				Message: "test message",
				Fields:  fields,
			}
			if err := record.Validate(now); err != nil {
				t.Errorf("record.Validate() = %v, want nil: extracted fields %v should already respect model's limits", err, fields)
			}
		})
	}
}

func TestNewExtractorRejectsPatternWithoutNamedGroups(t *testing.T) {
	re := regexp.MustCompile(`^(\d+) (\w+)$`)
	if _, err := NewExtractor(ExtractConfig{Pattern: re}); err == nil {
		t.Error("NewExtractor(pattern with no named groups) = nil error, want an error: this config can never extract a field")
	}
}

// TestExtractorValueTruncationKeepsUTF8Valid pins that truncating an over-long
// value does not split a multi-byte rune.
//
// A plain byte slice at the limit can land mid-rune. Nothing crashes if it does
// — these values reach a JSONB column through encoding/json, which coerces
// invalid bytes to U+FFFD rather than failing — but the stored value then
// carries a replacement character in place of what the producer actually wrote,
// and it is queryable data, so it is worth keeping intact.
func TestExtractorValueTruncationKeepsUTF8Valid(t *testing.T) {
	// Build a value that overshoots the limit and guarantees a multi-byte rune
	// straddles it: "é" is two bytes, so an odd-length ASCII prefix puts the
	// boundary inside one.
	prefix := strings.Repeat("a", model.MaxFieldValueLen-1)
	value := prefix + strings.Repeat("é", 16)

	e, err := NewExtractor(ExtractConfig{JSON: true})
	if err != nil {
		t.Fatalf("NewExtractor() error = %v", err)
	}

	encoded, err := json.Marshal(map[string]string{"k": value})
	if err != nil {
		t.Fatalf("marshal input: %v", err)
	}

	fields := e.Fields(encoded)
	got, ok := fields["k"]
	if !ok {
		t.Fatal(`no field "k" extracted`)
	}

	if len(got) > model.MaxFieldValueLen {
		t.Errorf("truncated value is %d bytes, over the %d limit", len(got), model.MaxFieldValueLen)
	}
	if !utf8.ValidString(got) {
		t.Error("truncated value is not valid UTF-8; a rune was split at the boundary")
	}
	if got := counterValue(t, e.metrics.ExtractValuesTruncated); got != 1 {
		t.Errorf("ExtractValuesTruncated = %v, want 1", got)
	}
	// The whole ASCII prefix must survive: backing up to a rune boundary should
	// cost at most the width of one rune, not silently drop more.
	if !strings.HasPrefix(got, prefix) {
		t.Error("truncation cut into the ASCII prefix rather than backing up to the rune boundary")
	}
}
