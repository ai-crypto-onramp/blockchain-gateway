package ws

import (
	"context"
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
	go follower.Run(ctx)

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/v1/chains/stub/heads"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	reg := chain.NewRegistry()
	reg.Register(stub)
	reg.StubEmitter("stub").EmitHead(chain.Head{ChainID: "stub", Height: 42, Hash: "0x42"})
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var msg HeadMessage
	if err := conn.ReadJSON(&msg); err != nil {
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