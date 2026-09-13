// Package tlsx builds the mutual-TLS transport credentials every gRPC hop in
// the project uses: agent to collector on ingest, collector to collector on
// the peer service. One place, so the rules are the same on both.
package tlsx

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"

	"google.golang.org/grpc/credentials"
)

// ErrNoClientCA is returned when a server is given a certificate but no client
// CA. One-way TLS would look secure while leaving the port open to anyone who
// can reach it, so refusing to start is the only honest response.
var ErrNoClientCA = errors.New("TLS requires a client CA for mutual authentication")

// ErrPartial is returned when only some of a client's three files are set.
var ErrPartial = errors.New("client TLS needs a certificate, a key and a CA")

// Server builds mTLS server credentials from a key pair and the CA that signs
// clients. Every client must present a certificate from that CA: RequireAndVerify,
// not VerifyIfGiven, since the weaker mode accepts a connection with no
// certificate at all, which is exactly the case being defended against.
func Server(certFile, keyFile, clientCAFile string) (credentials.TransportCredentials, error) {
	if clientCAFile == "" {
		return nil, ErrNoClientCA
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load key pair: %w", err)
	}
	pool, err := Pool(clientCAFile)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
		// TLS 1.3 only. Every peer is a Go binary from this repo, so there is no
		// legacy client to accommodate, and 1.3 removes the cipher-suite and
		// renegotiation choices that make older configurations quietly wrong.
		MinVersion: tls.VersionTLS13,
	}), nil
}

// Client builds mTLS client credentials: the certificate to present and the CA
// that signs servers. All three files or ErrPartial.
func Client(certFile, keyFile, caFile string) (credentials.TransportCredentials, error) {
	if certFile == "" || keyFile == "" || caFile == "" {
		return nil, fmt.Errorf("%w: got cert=%q key=%q ca=%q", ErrPartial, certFile, keyFile, caFile)
	}
	pair, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load client key pair: %w", err)
	}
	pool, err := Pool(caFile)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{pair},
		RootCAs:      pool,
		MinVersion:   tls.VersionTLS13,
	}), nil
}

// Pool reads one PEM file into a dedicated trust root. Not the system roots:
// those would trust every public CA, so any certificate signed by any of them
// would authenticate. The set of things allowed in is exactly the set this one
// CA signed.
func Pool(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		// AppendCertsFromPEM reports only "nothing parsed", so an empty or
		// malformed file would otherwise produce a server that rejects every
		// client with a handshake error and no explanation.
		return nil, fmt.Errorf("%s: no certificates found", path)
	}
	return pool, nil
}
