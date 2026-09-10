package query

import (
	"errors"
	"testing"
)

func FuzzParse(f *testing.F) {
	for _, seed := range roadmapExamples {
		f.Add(seed)
	}
	for _, cases := range [][]errCase{selectorErrorCases, stageErrorCases} {
		for _, tc := range cases {
			f.Add(tc.src)
		}
	}
	f.Fuzz(func(t *testing.T, src string) {
		q, err := Parse(src)
		if (q == nil) == (err == nil) {
			t.Fatalf("Parse(%q) = %v, %v: want exactly one of result and error", src, q, err)
		}
		if err != nil {
			var pe *Error
			if !errors.As(err, &pe) {
				t.Fatalf("Parse(%q) error is %T, want *Error", src, err)
			}
			if pe.Pos.Line < 1 || pe.Pos.Col < 1 || pe.Msg == "" {
				t.Fatalf("Parse(%q) error is malformed: %+v", src, pe)
			}
			return
		}
		if len(q.Selector.Matchers) == 0 {
			t.Fatalf("Parse(%q) accepted a selector with no matchers", src)
		}
		if q.Agg != nil && q.Agg.Range <= 0 {
			t.Fatalf("Parse(%q) accepted a non-positive range %v", src, q.Agg.Range)
		}
	})
}
