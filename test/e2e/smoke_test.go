// Package e2e contains the end-to-end smoke test exercised by `make
// e2e-smoke`. It drives the full broadcast -> confirm -> finalize lifecycle
// against a stub adapter + in-memory stores, without requiring Docker or a
// live chain. The test is in package e2e so it can be filtered separately
// from the unit suite (`go test ./test/e2e/`).
package e2e

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ai-crypto-onramp/blockchain-gateway/internal/app"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/chain"
)

// TestSmokeBroadcastConfirmFinalize boots the app with a stub adapter,
// broadcasts a signed tx, simulates the chain advancing past the finality
// depth, and asserts the tx reaches `finalized`.
func TestSmokeBroadcastConfirmFinalize(t *testing.T) {
	// Force stub-only registry regardless of CI env (CHAINS_SUPPORTED may
	// be set by `make test-integration`).
	t.Setenv("CHAINS_SUPPORTED", "")
	cfg := app.LoadConfig()
	cfg.Port = "0"
	cfg.BroadcastTimeout = 2 * time.Second
	cfg.ConfirmationPoll = 50 * time.Millisecond

	srv, err := app.Build(cfg)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	srv.StartFollowers(ctx)
	ts := httptest.NewServer(srv.HTTPHandler())
	defer func() {
		cancel()
		_ = srv.Shutdown()
		ts.Close()
	}()

	// The app registers a "stub" adapter when CHAINS_SUPPORTED is empty.
	// Reach into the registry to drive the stub's head channel.
	stub := srv.Registry().StubEmitter("stub")

	// 1. Broadcast.
	body := strings.NewReader(`{"signed_tx":"0xdeadbeef","from":"0xfrom","to":"0xto","value":"1000"}`)
	resp, err := http.Post(ts.URL+"/v1/chains/stub/broadcast", "application/json", body)
	if err != nil {
		t.Fatalf("broadcast: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("broadcast status: %d", resp.StatusCode)
	}
	var br struct {
		TxHash string `json:"tx_hash"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&br); err != nil {
		t.Fatalf("decode broadcast: %v", err)
	}
	if br.TxHash == "" {
		t.Fatal("missing tx hash")
	}

	// 2. Seed the tx as included at block 10 and emit heads to advance
	//    confirmations past finality (stub finality is 3).
	stub.SeedTx(
		&chain.Tx{ChainID: "stub", Hash: br.TxHash, BlockHeight: 10, Status: chain.StatusConfirmed},
		&chain.TxStatus{ChainID: "stub", TxHash: br.TxHash, Status: chain.StatusConfirmed, BlockHeight: 10, Confirmations: 1},
	)
	for h := uint64(11); h <= 13; h++ {
		stub.EmitHead(chain.Head{ChainID: "stub", Height: h, Hash: hashFor(h), ParentHash: hashFor(h - 1)})
	}

	// 3. Poll /status until finalized.
	deadline := time.Now().Add(3 * time.Second)
	var status string
	for time.Now().Before(deadline) {
		r, err := http.Get(ts.URL + "/v1/chains/stub/tx/" + br.TxHash + "/status")
		if err != nil {
			t.Fatalf("status: %v", err)
		}
		var sr struct {
			Status        string `json:"status"`
			Confirmations int    `json:"confirmations"`
		}
		_ = json.NewDecoder(r.Body).Decode(&sr)
		r.Body.Close()
		status = sr.Status
		if status == "finalized" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("tx did not finalize in time, last status=%s", status)
}

// TestSmokeHealth verifies /healthz returns 200 on a booted app.
func TestSmokeHealth(t *testing.T) {
	t.Setenv("CHAINS_SUPPORTED", "")
	cfg := app.LoadConfig()
	cfg.Port = "0"
	srv, err := app.Build(cfg)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	ts := httptest.NewServer(srv.HTTPHandler())
	defer ts.Close()
	defer srv.Shutdown()
	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health status: %d", resp.StatusCode)
	}
}

// TestSmokeHeight verifies /v1/chains/stub/height returns 200.
func TestSmokeHeight(t *testing.T) {
	t.Setenv("CHAINS_SUPPORTED", "")
	cfg := app.LoadConfig()
	cfg.Port = "0"
	srv, err := app.Build(cfg)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	ts := httptest.NewServer(srv.HTTPHandler())
	defer ts.Close()
	defer srv.Shutdown()
	resp, err := http.Get(ts.URL + "/v1/chains/stub/height")
	if err != nil {
		t.Fatalf("height: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("height status: %d", resp.StatusCode)
	}
}

// hashFor synthesizes a deterministic block hash for height h so the tip
// follower can persist it.
func hashFor(h uint64) string {
	const hexd = "0123456789abcdef"
	out := make([]byte, 66)
	out[0] = '0'
	out[1] = 'x'
	for i := 2; i < 66; i++ {
		out[i] = hexd[(h+uint64(i))%16]
	}
	return string(out)
}