package config

import (
	"strings"
	"testing"
)

// Ingest is a write path with no application-level auth, so the default must not be
// reachable from off-box.
func TestIngestDefaultsToLoopback(t *testing.T) {
	t.Parallel()

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !loopbackAddr(cfg.Ingest.Addr) {
		t.Errorf("default ingest addr %q is not loopback", cfg.Ingest.Addr)
	}
}

func TestLoopbackAddr(t *testing.T) {
	t.Parallel()

	loopback := []string{"127.0.0.1:9095", "127.0.0.5:1", "[::1]:9095", "localhost:9095"}
	routable := []string{":9095", "0.0.0.0:9095", "192.168.1.10:9095", "[::]:9095", "collector:9095", "garbage"}

	for _, addr := range loopback {
		if !loopbackAddr(addr) {
			t.Errorf("loopbackAddr(%q) = false, want true", addr)
		}
	}
	for _, addr := range routable {
		if loopbackAddr(addr) {
			t.Errorf("loopbackAddr(%q) = true, want false", addr)
		}
	}
}

// The four TLS combinations that must be refused, and the two that must not.
func TestIngestTLSValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mutate  func(*Ingest)
		wantErr string
	}{
		{
			name:   "loopback plaintext is the zero-setup default",
			mutate: func(*Ingest) {},
		},
		{
			name: "full mTLS on a routable address",
			mutate: func(i *Ingest) {
				i.Addr = ":9095"
				i.TLSCertFile, i.TLSKeyFile, i.TLSClientCAFile = "c.pem", "k.pem", "ca.pem"
			},
		},
		{
			name:    "cert without key",
			mutate:  func(i *Ingest) { i.TLSCertFile = "c.pem" },
			wantErr: "both cert and key",
		},
		{
			name:    "key without cert",
			mutate:  func(i *Ingest) { i.TLSKeyFile = "k.pem" },
			wantErr: "both cert and key",
		},
		{
			name:    "client CA without server TLS",
			mutate:  func(i *Ingest) { i.TLSClientCAFile = "ca.pem" },
			wantErr: "client CA is set but server TLS is not enabled",
		},
		{
			name: "server TLS without a client CA is one-way TLS",
			mutate: func(i *Ingest) {
				i.TLSCertFile, i.TLSKeyFile = "c.pem", "k.pem"
			},
			wantErr: "mutual authentication is required",
		},
		{
			name:    "plaintext on a routable address needs an explicit opt-in",
			mutate:  func(i *Ingest) { i.Addr = ":9095" },
			wantErr: "unauthenticated write path",
		},
		{
			name: "plaintext on a routable address with the opt-in",
			mutate: func(i *Ingest) {
				i.Addr = ":9095"
				i.AllowPlaintext = true
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg, err := Load()
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			tt.mutate(&cfg.Ingest)

			err = cfg.Validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Validate accepted %s", tt.name)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("Validate error = %q, want it to mention %q", err, tt.wantErr)
			}
		})
	}
}
