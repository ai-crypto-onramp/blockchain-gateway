package app

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestBuildDefault(t *testing.T) {
	cfg := LoadConfig()
	cfg.Port = "0"
	srv, err := Build(cfg)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	defer func() { _ = srv.Shutdown() }()
	if srv.HTTPHandler() == nil {
		t.Fatal("nil handler")
	}
	if len(srv.Registry().Chains()) == 0 {
		t.Fatal("no chains registered")
	}
}

func TestBuildWithChainsSupported(t *testing.T) {
	t.Setenv("CHAINS_SUPPORTED", "ethereum,polygon")
	t.Setenv("RPC_URLS_ETHEREUM", "http://localhost:8545")
	t.Setenv("RPC_URLS_POLYGON", "http://localhost:8546")
	t.Setenv("FINALITY_BLOCKS_ETHEREUM", "64")
	t.Setenv("FINALITY_BLOCKS_POLYGON", "256")
	cfg := LoadConfig()
	cfg.Port = "0"
	cfg.ConfirmationPoll = 50 * time.Millisecond
	srv, err := Build(cfg)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	defer func() { _ = srv.Shutdown() }()
	chains := srv.Registry().Chains()
	if len(chains) != 2 {
		t.Fatalf("chains: %v", chains)
	}
}

func TestStartFollowersAndShutdown(t *testing.T) {
	cfg := LoadConfig()
	cfg.Port = "0"
	srv, err := Build(cfg)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	srv.StartFollowers(ctx)
	ts := httptest.NewServer(srv.HTTPHandler())
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	cancel()
	if err := srv.Shutdown(); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

func TestLoadConfigDefaults(t *testing.T) {
	cfg := LoadConfig()
	if cfg.Port != "8080" {
		t.Errorf("port: %s", cfg.Port)
	}
	if cfg.BroadcastTimeout != 10*time.Second {
		t.Errorf("broadcast timeout: %v", cfg.BroadcastTimeout)
	}
	if cfg.BroadcastRetryMax != 3 {
		t.Errorf("retry max: %d", cfg.BroadcastRetryMax)
	}
}

func TestEnvHelpers(t *testing.T) {
	if envOr("NONEXISTENT_VAR_XYZ", "def") != "def" {
		t.Error("envOr default")
	}
	if envDur("NONEXISTENT_VAR_XYZ", 5*time.Second) != 5*time.Second {
		t.Error("envDur default")
	}
	if envInt("NONEXISTENT_VAR_XYZ", 7) != 7 {
		t.Error("envInt default")
	}
}