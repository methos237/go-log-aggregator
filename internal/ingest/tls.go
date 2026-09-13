package ingest

import (
	"fmt"

	"google.golang.org/grpc"

	"github.com/jamespolk/go-log-aggregator/internal/config"
	"github.com/jamespolk/go-log-aggregator/internal/tlsx"
)

// ErrNoClientCA is returned when server TLS is configured without a client CA.
// mTLS is the security baseline for this hop; see tlsx.ErrNoClientCA.
var ErrNoClientCA = tlsx.ErrNoClientCA

// transportCredentials builds the server's transport security, or nil for plaintext.
//
// Plaintext is allowed only because `make dev` has to work with no setup, and it is
// what the empty configuration means. Anything reachable beyond localhost is
// expected to set all three files; see `make certs`.
//
//nolint:gocritic // hugeParam: called once per process
func transportCredentials(cfg config.Ingest) (grpc.ServerOption, error) {
	if cfg.TLSCertFile == "" && cfg.TLSKeyFile == "" {
		return grpc.EmptyServerOption{}, nil
	}
	creds, err := tlsx.Server(cfg.TLSCertFile, cfg.TLSKeyFile, cfg.TLSClientCAFile)
	if err != nil {
		return nil, fmt.Errorf("ingest: %w", err)
	}
	return grpc.Creds(creds), nil
}
