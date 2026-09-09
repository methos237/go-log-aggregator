package ingest

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	logaggv1 "github.com/jamespolk/go-log-aggregator/api/proto/logagg/v1"
	"github.com/jamespolk/go-log-aggregator/internal/config"
	"github.com/jamespolk/go-log-aggregator/internal/queue/queuetest"
)

// certAuthority is a throwaway CA for one test.
//
// Generated in process rather than shelling out to `make certs`: the tests must run
// on a machine with only a Go toolchain, and a test that depends on openssl fails
// for reasons that have nothing to do with this code.
type certAuthority struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	dir  string
	// caFile is the PEM trust root both sides use.
	caFile string
}

func newCA(t *testing.T) *certAuthority {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("self-sign CA: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA: %v", err)
	}

	ca := &certAuthority{cert: cert, key: key, dir: t.TempDir()}
	ca.caFile = filepath.Join(ca.dir, "ca.pem")
	writePEM(t, ca.caFile, "CERTIFICATE", der)
	return ca
}

// issue writes a leaf certificate and key signed by this CA, and returns their paths.
func (ca *certAuthority) issue(t *testing.T, name string, server bool) (certPath, keyPath string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate %s key: %v", name, err)
	}

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	if server {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		tmpl.DNSNames = []string{"localhost"}
		tmpl.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
	} else {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("sign %s: %v", name, err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal %s key: %v", name, err)
	}

	certPath = filepath.Join(ca.dir, name+".pem")
	keyPath = filepath.Join(ca.dir, name+"-key.pem")
	writePEM(t, certPath, "CERTIFICATE", der)
	writePEM(t, keyPath, "EC PRIVATE KEY", keyDER)
	return certPath, keyPath
}

// clientCreds builds transport credentials presenting a certificate from this CA.
func (ca *certAuthority) clientCreds(t *testing.T, name string) credentials.TransportCredentials {
	t.Helper()

	certPath, keyPath := ca.issue(t, name, false)
	pair, err := tlsKeyPair(certPath, keyPath)
	if err != nil {
		t.Fatalf("load client pair: %v", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca.cert)

	return credentials.NewTLS(tlsClientConfig(pair, pool))
}

func writePEM(t *testing.T, path, blockType string, der []byte) {
	t.Helper()

	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der}), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// tlsConfig returns an ingest config with mTLS enabled against this CA.
func (ca *certAuthority) serverConfig(t *testing.T) config.Ingest {
	t.Helper()

	certPath, keyPath := ca.issue(t, "server", true)
	cfg := testIngestConfig()
	cfg.TLSCertFile = certPath
	cfg.TLSKeyFile = keyPath
	cfg.TLSClientCAFile = ca.caFile
	return cfg
}

// A client with a certificate from the configured CA gets through, which proves the
// happy path of the whole handshake rather than just that the config parsed.
func TestMTLSAcceptsATrustedClient(t *testing.T) {
	t.Parallel()

	ca := newCA(t)
	srv := serveTLS(t, ca.serverConfig(t))

	conn, err := grpc.NewClient(srv.Addr(), grpc.WithTransportCredentials(ca.clientCreds(t, "agent")))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	ack := exchange(t, logaggv1.NewLogServiceClient(conn), validBatch("b1", 2))
	if ack.GetCode() != logaggv1.AckCode_ACK_CODE_ACCEPTED {
		t.Fatalf("code = %s (%s), want ACCEPTED", ack.GetCode(), ack.GetDetail())
	}
}

// The case the whole subtask exists for: no client certificate must not be enough.
func TestMTLSRejectsAClientWithNoCertificate(t *testing.T) {
	t.Parallel()

	ca := newCA(t)
	srv := serveTLS(t, ca.serverConfig(t))

	pool := x509.NewCertPool()
	pool.AddCert(ca.cert)
	// Verifies the server but presents nothing itself, which is what plain TLS does.
	conn, err := grpc.NewClient(srv.Addr(), grpc.WithTransportCredentials(
		credentials.NewTLS(tlsClientConfig(nil, pool))))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if err = attemptStream(t, conn); err == nil {
		t.Fatal("a client with no certificate was allowed to stream")
	}
}

// A certificate from some other CA must not work either, or the trust root is
// decorative.
func TestMTLSRejectsAClientFromAnotherCA(t *testing.T) {
	t.Parallel()

	ca := newCA(t)
	srv := serveTLS(t, ca.serverConfig(t))

	other := newCA(t)
	conn, err := grpc.NewClient(srv.Addr(), grpc.WithTransportCredentials(other.clientCreds(t, "impostor")))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if err = attemptStream(t, conn); err == nil {
		t.Fatal("a client signed by an unknown CA was allowed to stream")
	}
}

// A plaintext client must fail rather than silently negotiate something weaker.
func TestMTLSRejectsAPlaintextClient(t *testing.T) {
	t.Parallel()

	ca := newCA(t)
	srv := serveTLS(t, ca.serverConfig(t))

	conn, err := grpc.NewClient(srv.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if err = attemptStream(t, conn); err == nil {
		t.Fatal("a plaintext client was allowed to stream against a TLS listener")
	}
}

// Server TLS without a client CA looks secure and is not, so it must refuse to
// start rather than serve one-way TLS.
func TestServerRefusesTLSWithoutAClientCA(t *testing.T) {
	t.Parallel()

	ca := newCA(t)
	cfg := ca.serverConfig(t)
	cfg.TLSClientCAFile = ""

	_, err := New(context.Background(), cfg, &queuetest.Publisher{}, NewMetrics(nil), nil)
	if !errors.Is(err, ErrNoClientCA) {
		t.Fatalf("err = %v, want ErrNoClientCA", err)
	}
}

func TestServerReportsBadTLSMaterial(t *testing.T) {
	t.Parallel()

	ca := newCA(t)

	t.Run("missing certificate file", func(t *testing.T) {
		t.Parallel()
		cfg := ca.serverConfig(t)
		cfg.TLSCertFile = filepath.Join(t.TempDir(), "absent.pem")
		if _, err := New(context.Background(), cfg, &queuetest.Publisher{}, NewMetrics(nil), nil); err == nil {
			t.Fatal("New succeeded with a missing certificate")
		}
	})

	t.Run("client CA that contains no certificates", func(t *testing.T) {
		t.Parallel()
		// AppendCertsFromPEM only reports "nothing parsed", so without an explicit
		// check this would start and then reject every agent with a bare handshake
		// error.
		empty := filepath.Join(t.TempDir(), "empty.pem")
		if err := os.WriteFile(empty, []byte("not a certificate\n"), 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
		cfg := ca.serverConfig(t)
		cfg.TLSClientCAFile = empty
		if _, err := New(context.Background(), cfg, &queuetest.Publisher{}, NewMetrics(nil), nil); err == nil {
			t.Fatal("New succeeded with a client CA file containing no certificates")
		}
	})
}

// Plaintext stays the default so `make dev` needs no setup.
func TestNoTLSConfiguredMeansPlaintext(t *testing.T) {
	t.Parallel()

	creds, err := transportCredentials(testIngestConfig())
	if err != nil {
		t.Fatalf("transportCredentials: %v", err)
	}
	if _, ok := creds.(grpc.EmptyServerOption); !ok {
		t.Fatalf("got %T, want grpc.EmptyServerOption", creds)
	}
}

// serveTLS starts a server from cfg and returns it, cleaning up afterwards.
//
//nolint:gocritic // hugeParam: test helper
func serveTLS(t *testing.T, cfg config.Ingest) *Server {
	t.Helper()

	srv, err := New(context.Background(), cfg, &queuetest.Publisher{}, NewMetrics(nil), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	served := make(chan error, 1)
	go func() { served <- srv.Serve() }()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if shutErr := srv.Shutdown(ctx); shutErr != nil {
			t.Errorf("Shutdown: %v", shutErr)
		}
		if serveErr := <-served; serveErr != nil {
			t.Errorf("Serve: %v", serveErr)
		}
	})
	return srv
}

// attemptStream opens a stream and sends one batch, returning the resulting error.
// A rejected handshake surfaces on the first Recv rather than on dial, because gRPC
// connects lazily.
func attemptStream(t *testing.T, conn *grpc.ClientConn) error {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := logaggv1.NewLogServiceClient(conn).Stream(ctx)
	if err != nil {
		return err
	}
	_ = stream.Send(validBatch("b1", 1))
	_, err = stream.Recv()
	return err
}

// tlsKeyPair loads a leaf certificate and key for a client.
func tlsKeyPair(certPath, keyPath string) (*tls.Certificate, error) {
	pair, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, err
	}
	return &pair, nil
}

// tlsClientConfig builds a client config trusting pool, presenting pair when it is
// not nil. A nil pair is how the "no client certificate" case is expressed.
func tlsClientConfig(pair *tls.Certificate, pool *x509.CertPool) *tls.Config {
	cfg := &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS13}
	if pair != nil {
		cfg.Certificates = []tls.Certificate{*pair}
	}
	return cfg
}
