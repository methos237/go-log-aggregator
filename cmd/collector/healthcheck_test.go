package main

import "testing"

func TestLocalAddr(t *testing.T) {
	tests := map[string]string{
		":8080":          "127.0.0.1:8080",
		"0.0.0.0:8080":   "127.0.0.1:8080",
		"[::]:8080":      "127.0.0.1:8080",
		"127.0.0.1:9090": "127.0.0.1:9090",
		"collector:8080": "collector:8080",
		// Not a host:port pair at all; returned unchanged so the caller's dial
		// error names the original value instead of a mangled one.
		"garbage": "garbage",
	}
	for listen, want := range tests {
		t.Run(listen, func(t *testing.T) {
			if got := localAddr(listen); got != want {
				t.Errorf("localAddr(%q) = %q, want %q", listen, got, want)
			}
		})
	}
}
