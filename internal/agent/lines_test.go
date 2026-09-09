package agent

import (
	"bufio"
	"io"
	"strings"
	"testing"

	"github.com/jamespolk/go-log-aggregator/internal/model"
)

// TestLineAssembler_Next covers the byte-level assembly rules shared by every
// caller: terminator handling, CRLF, empty lines, and the terminated flag that
// lets stdin and the tail source disagree about a final unterminated line
// without either copying the other's truncation logic.
func TestLineAssembler_Next(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		wantLine       string
		wantConsumed   int64
		wantTerminated bool
		wantErr        error
	}{
		{
			name:           "terminated line",
			input:          "hello\n",
			wantLine:       "hello",
			wantConsumed:   6,
			wantTerminated: true,
		},
		{
			name:           "unterminated line at EOF",
			input:          "hello",
			wantLine:       "hello",
			wantConsumed:   5,
			wantTerminated: false,
			wantErr:        io.EOF,
		},
		{
			name:           "empty line",
			input:          "\n",
			wantLine:       "",
			wantConsumed:   1,
			wantTerminated: true,
		},
		{
			name:           "empty input",
			input:          "",
			wantLine:       "",
			wantConsumed:   0,
			wantTerminated: false,
			wantErr:        io.EOF,
		},
		{
			name:           "CRLF terminated",
			input:          "windows\r\n",
			wantLine:       "windows",
			wantConsumed:   9,
			wantTerminated: true,
		},
		{
			name:           "bare CR is not a terminator",
			input:          "mac\rend\n",
			wantLine:       "mac\rend",
			wantConsumed:   8,
			wantTerminated: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := newLineAssembler()
			br := bufio.NewReader(strings.NewReader(tt.input))

			line, consumed, terminated, err := a.next(br)

			if line == nil {
				t.Error("next() returned a nil line, want non-nil even when empty")
			}
			if string(line) != tt.wantLine {
				t.Errorf("next() line = %q, want %q", line, tt.wantLine)
			}
			if consumed != tt.wantConsumed {
				t.Errorf("next() consumed = %d, want %d", consumed, tt.wantConsumed)
			}
			if terminated != tt.wantTerminated {
				t.Errorf("next() terminated = %v, want %v", terminated, tt.wantTerminated)
			}
			if tt.wantErr == nil {
				if err != nil {
					t.Errorf("next() err = %v, want nil", err)
				}
			} else if err == nil {
				t.Errorf("next() err = nil, want an error wrapping %v", tt.wantErr)
			}
		})
	}
}

// TestLineAssembler_Truncation pins the truncation contract byte for byte:
// exactly MaxMessageLen is emitted, consumed accounts for the discarded
// overflow, and the reader is left positioned correctly for the next line.
func TestLineAssembler_Truncation(t *testing.T) {
	overflow := 1000
	long := strings.Repeat("x", model.MaxMessageLen+overflow)
	input := long + "\nnext line\n"

	a := newLineAssembler()
	br := bufio.NewReader(strings.NewReader(input))

	line, consumed, terminated, err := a.next(br)
	if err != nil {
		t.Fatalf("first next() err = %v, want nil", err)
	}
	if !terminated {
		t.Error("first next() terminated = false, want true (the \\n was found, just past the truncation point)")
	}
	if len(line) != model.MaxMessageLen {
		t.Fatalf("first next() line length = %d, want %d", len(line), model.MaxMessageLen)
	}
	if string(line) != strings.Repeat("x", model.MaxMessageLen) {
		t.Error("first next() line content is not all 'x'")
	}
	wantConsumed := int64(model.MaxMessageLen + overflow + 1) // the x's, the overflow, and \n.
	if consumed != wantConsumed {
		t.Errorf("first next() consumed = %d, want %d", consumed, wantConsumed)
	}

	line2, _, terminated2, err2 := a.next(br)
	if err2 != nil {
		t.Fatalf("second next() err = %v, want nil", err2)
	}
	if !terminated2 {
		t.Error("second next() terminated = false, want true")
	}
	if string(line2) != "next line" {
		t.Errorf("second next() line = %q, want %q", line2, "next line")
	}
}

// TestLineAssembler_DistinctAllocations guards the promise that a returned
// line never aliases the reused internal buffer: a caller mutating one line
// (or the buffer being reused on the next call) must never affect a line
// already handed out.
func TestLineAssembler_DistinctAllocations(t *testing.T) {
	a := newLineAssembler()
	br := bufio.NewReader(strings.NewReader("first\nsecond\n"))

	line1, _, _, err := a.next(br)
	if err != nil {
		t.Fatalf("first next() err = %v, want nil", err)
	}
	for i := range line1 {
		line1[i] = 'X'
	}

	line2, _, _, err := a.next(br)
	if err != nil {
		t.Fatalf("second next() err = %v, want nil", err)
	}
	if string(line2) != "second" {
		t.Errorf("line2 = %q after mutating line1, want %q (buffer reuse leaked across calls)", line2, "second")
	}
}
