package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/jamespolk/go-log-aggregator/internal/config"
)

// probeSelf requests /readyz on this process's own public listener.
//
// It loads the same configuration the server did, so an overridden port is
// picked up automatically instead of being hardcoded in two places.
func probeSelf() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	url := "http://" + localAddr(cfg.HTTP.Addr) + "/readyz"

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		return fmt.Errorf("probe %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("probe %s: status %d", url, resp.StatusCode)
	}
	return nil
}

// localAddr turns a listen address into a dialable loopback address. A listen
// address may omit the host (":8080") or bind all interfaces ("0.0.0.0:8080"),
// neither of which is a valid dial target on every platform.
func localAddr(listen string) string {
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return listen
	}
	switch host {
	case "", "0.0.0.0", "[::]", "::":
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, port)
}
