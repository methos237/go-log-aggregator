package ingest

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/jamespolk/go-log-aggregator/internal/config"
)

// ErrNoClientCA is returned when server TLS is configured without a client CA.
//
// mTLS is the security baseline for this hop, and one-way TLS here would be worse
// than useless: it looks secure while leaving ingest open to anyone who can reach
// the port. Refusing to start is the only honest response.
var ErrNoClientCA = errors.New("ingest TLS requires a client CA for mutual authentication")

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
	if cfg.TLSClientCAFile == "" {
		return nil, ErrNoClientCA
	}

	cert, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
	if err != nil {
		return nil, fmt.Errorf("load ingest key pair: %w", err)
	}

	pool, err := clientCAs(cfg.TLSClientCAFile)
	if err != nil {
		return nil, err
	}

	return grpc.Creds(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		// RequireAndVerify, not VerifyIfGiven: the weaker mode accepts a connection
		// that presents no certificate at all, which is exactly the case being
		// defended against.
		ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs:  pool,
		// TLS 1.3 only. Every client here is a Go binary from this repo, so there is
		// no legacy peer to accommodate, and 1.3 removes the cipher-suite and
		// renegotiation choices that make older configurations quietly wrong.
		MinVersion: tls.VersionTLS13,
	})), nil
}

// clientCAs builds the trust root for client certificates.
//
// A dedicated pool rather than the system roots: the system pool would trust every
// public CA, so any certificate signed by any of them would authenticate as an
// agent. The set of things allowed to write logs into this cluster is exactly the
// set signed by this one CA.
func clientCAs(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read ingest client CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		// AppendCertsFromPEM reports only "nothing parsed", so an empty or malformed
		// file would otherwise produce a server that rejects every client with a
		// handshake error and no explanation.
		return nil, fmt.Errorf("%s: no certificates found", path)
	}
	return pool, nil
}
