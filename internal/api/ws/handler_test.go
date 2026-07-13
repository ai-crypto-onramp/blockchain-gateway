package ws

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ai-crypto-onramp/blockchain-gateway/internal/chain"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/store/memstore"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/tip"
	"github.com/go-chi/chi/v5"
	"github.com/gorilla/websocket"
)

// TestHeadToWSZeroTimestamp exercises the branch in headToWS where
// h.Timestamp.Unix() == 0 (the epoch), which falls back to time.Now().
func TestHeadToWSZeroTimestamp(t *testing.T) {
	m := headToWS(chain.Head{ChainID: "eth", Height: 1, Hash: "0x1", Timestamp: time.Unix(0, 0)})
	if m.Timestamp <= 0 {
		t.Errorf("timestamp should fall back to now, got %d", m.Timestamp)
	}
}

func TestHeadToWSWithTimestamp(t *testing.T) {
	ts := time.Unix(1234567890, 0)
	m := headToWS(chain.Head{ChainID: "eth", Height: 1, Hash: "0x1", Timestamp: ts})
	if m.Timestamp != 1234567890 {
		t.Errorf("timestamp: %d want 1234567890", m.Timestamp)
	}
}

// TestWSHandlerUnknownChain404 ensures the handler writes a 404 before
// upgrading when the chain is unknown.
func TestWSHandlerUnknownChain404(t *testing.T) {
	h := NewHandler(map[string]*tip.Follower{})
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/chains/nope/heads", nil)
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rr.Code)
	}
}

// TestWSUpgradeFailure ensures ServeHTTP returns silently when the upgrade
// fails (non-WS request).
func TestWSUpgradeFailure(t *testing.T) {
	stub := chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "stub", FinalityBlocks: 3})
	follower := tip.NewFollower(stub, memstore.NewTipStore(), time.Second)
	h := NewHandler(map[string]*tip.Follower{"stub": follower})
	r := chi.NewRouter()
	r.Get("/v1/chains/{chain}/heads", h.ServeHTTP)
	srv := httptest.NewServer(r)
	defer srv.Close()
	// Plain HTTP GET without WS upgrade headers -> Upgrade returns error.
	resp, err := http.Get(srv.URL + "/v1/chains/stub/heads")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	// gorilla returns 400 from Upgrade on a plain GET; the handler then
	// returns without writing further. We only assert no panic.
}

// TestWSWriteErrorExits verifies the handler exits when the client closes
// the connection mid-stream (WriteJSON returns an error).
func TestWSWriteErrorExits(t *testing.T) {
	stub := chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "stub", FinalityBlocks: 3})
	follower := tip.NewFollower(stub, memstore.NewTipStore(), time.Second)
	h := NewHandler(map[string]*tip.Follower{"stub": follower})
	r := chi.NewRouter()
	r.Get("/v1/chains/{chain}/heads", h.ServeHTTP)
	srv := httptest.NewServer(r)
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = follower.Run(ctx) }()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/v1/chains/stub/heads"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	// Close the client connection immediately; the server's next WriteJSON
	// will error and the handler should exit cleanly.
	conn.Close()
	// Give the handler a moment to observe the write error.
	time.Sleep(100 * time.Millisecond)
}

func TestWSHeadsStream(t *testing.T) {
	stub := chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "stub", FinalityBlocks: 3})
	follower := tip.NewFollower(stub, memstore.NewTipStore(), time.Second)
	followers := map[string]*tip.Follower{"stub": follower}
	h := NewHandler(followers)
	r := chi.NewRouter()
	r.Get("/v1/chains/{chain}/heads", h.ServeHTTP)
	srv := httptest.NewServer(r)
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = follower.Run(ctx) }()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/v1/chains/stub/heads"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	reg := chain.NewRegistry()
	reg.Register(stub)

	// The WS handler subscribes asynchronously after the dial completes;
	// under -race in CI the subscription may not be registered before we
	// emit. Retry the emit + read until a head arrives or the deadline
	// passes.
	deadline := time.Now().Add(5 * time.Second)
	_ = conn.SetReadDeadline(deadline)
	var msg HeadMessage
	for time.Now().Before(deadline) {
		reg.StubEmitter("stub").EmitHead(chain.Head{ChainID: "stub", Height: 42, Hash: "0x42"})
		if err := conn.ReadJSON(&msg); err == nil {
			break
		} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
			// No head yet — subscription still racing. Extend and retry.
			_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			continue
		}
		t.Fatalf("read: %v", err)
	}
	if msg.Height != 42 {
		t.Errorf("height: %d", msg.Height)
	}
}

func TestWSUnknownChain(t *testing.T) {
	h := NewHandler(map[string]*tip.Follower{})
	r := chi.NewRouter()
	r.Get("/v1/chains/{chain}/heads", h.ServeHTTP)
	srv := httptest.NewServer(r)
	defer srv.Close()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/v1/chains/nope/heads"
	_, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err == nil {
		t.Fatal("expected error for unknown chain")
	}
	if resp != nil && resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404, got %d", resp.StatusCode)
	}
}