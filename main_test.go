package main

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"
)

// TestHealthzSmoke boots the app and hits /healthz to confirm the router
// is wired. This replaces the original standalone-healthz tests, which
// assumed a hand-rolled mux; the app now owns routing.
func TestHealthzSmoke(t *testing.T) {
	// Build the app with defaults (no CHAINS_SUPPORTED => stub adapter).
	srv, err := buildTestServer()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["status"] != "ok" {
		t.Fatalf("expected status ok, got %q", body["status"])
	}
}

// TestUnknownRouteReturns404 verifies the router 404s unknown paths.
func TestUnknownRouteReturns404(t *testing.T) {
	srv, err := buildTestServer()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/nope")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

// TestRunReturnsErrorOnBusyAddress boots a server on a busy port and
// expects Run to return an error quickly (the HTTP listener fails to bind).
func TestRunReturnsErrorOnBusyAddress(t *testing.T) {
	// Bind a listener on a random port to occupy it.
	ln, err := netListen("0.0.0.0:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()
	port := portFromAddr(ln.Addr().String())
	// Build a second http.Server on the same port; ListenAndServe must fail.
	srv2 := newHTTPServerOnPort(port)
	errCh := make(chan error, 1)
	go func() { errCh <- srv2.ListenAndServe() }()
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected error when address is in use, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ListenAndServe did not return within 2s on busy address")
	}
}