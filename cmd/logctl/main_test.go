package main

import (
	"errors"
	"strings"
	"testing"

	logaggv1 "github.com/jamespolk/go-log-aggregator/api/proto/logagg/v1"
)

// A typo in -level must be a message about the flag. Sending LEVEL_UNSPECIFIED
// instead would have the collector reject every record, leaving the operator staring
// at a rejection count.
func TestParseLevel(t *testing.T) {
	t.Parallel()

	for name, want := range map[string]logaggv1.Level{
		"trace": logaggv1.Level_LEVEL_TRACE,
		"debug": logaggv1.Level_LEVEL_DEBUG,
		"info":  logaggv1.Level_LEVEL_INFO,
		"INFO":  logaggv1.Level_LEVEL_INFO,
		"Warn":  logaggv1.Level_LEVEL_WARN,
		"error": logaggv1.Level_LEVEL_ERROR,
		"fatal": logaggv1.Level_LEVEL_FATAL,
	} {
		got, err := parseLevel(name)
		if err != nil {
			t.Errorf("parseLevel(%q): %v", name, err)
			continue
		}
		if got != want {
			t.Errorf("parseLevel(%q) = %s, want %s", name, got, want)
		}
	}

	if _, err := parseLevel("verbose"); err == nil {
		t.Error("an unknown level was accepted")
	}
	if _, err := parseLevel(""); err == nil {
		t.Error("an empty level was accepted")
	}
}

func TestCodeIsReadable(t *testing.T) {
	t.Parallel()

	for code, want := range map[logaggv1.AckCode]string{
		logaggv1.AckCode_ACK_CODE_ACCEPTED:   "accepted",
		logaggv1.AckCode_ACK_CODE_OVERLOADED: "overloaded",
		logaggv1.AckCode_ACK_CODE_INVALID:    "invalid",
		logaggv1.AckCode_ACK_CODE_INTERNAL:   "internal",
	} {
		if got := code2(code); got != want {
			t.Errorf("code(%s) = %q, want %q", code, got, want)
		}
	}
}

// code2 exists only to keep the test readable next to the map key named code.
func code2(c logaggv1.AckCode) string { return code(c) }

func TestRunRejectsUnknownCommands(t *testing.T) {
	t.Parallel()

	if err := run([]string{"frobnicate"}); err == nil {
		t.Error("an unknown command was accepted")
	}
	if err := run(nil); err == nil {
		t.Error("no command was accepted")
	}
	if err := run([]string{"help"}); err != nil {
		t.Errorf("help returned an error: %v", err)
	}
}

func TestSendRequiresAService(t *testing.T) {
	t.Parallel()

	// Service is part of the stream identity, so there is no sensible default and
	// guessing one would silently attribute an operator's lines to the wrong stream.
	err := sendCmd([]string{"-addr", "127.0.0.1:1"})
	if err == nil || !strings.Contains(err.Error(), "service") {
		t.Fatalf("err = %v, want a complaint about -service", err)
	}
}

func TestSendRejectsABadLevelBeforeConnecting(t *testing.T) {
	t.Parallel()

	// Port 1 is not listening: reaching a dial attempt at all would mean the flag was
	// not validated first.
	err := sendCmd([]string{"-addr", "127.0.0.1:1", "-service", "api", "-level", "loud"})
	if err == nil || !strings.Contains(err.Error(), "unknown level") {
		t.Fatalf("err = %v, want a complaint about the level", err)
	}
}

func TestSendRejectsInvalidLabelsBeforeConnecting(t *testing.T) {
	t.Parallel()

	err := sendCmd([]string{"-addr", "127.0.0.1:1", "-service", "api", "-env", ""})
	if err == nil || !strings.Contains(err.Error(), "labels") {
		t.Fatalf("err = %v, want a complaint about the labels", err)
	}
}

func TestUsageErrorsAreDistinguishable(t *testing.T) {
	t.Parallel()

	for _, args := range [][]string{nil, {"frobnicate"}, {"send"}, {"send", "-service", "x", "stray"}, {"query"}, {"tail"}} {
		if err := run(args); !errors.Is(err, errUsage) {
			t.Errorf("run(%q) = %v, want a usage error", args, err)
		}
	}
}
