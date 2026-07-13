package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ai-crypto-onramp/blockchain-gateway/internal/broadcast"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/chain"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/eventbus"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/mempool"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/store"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/store/memstore"
)

func newTestDeps(t *testing.T) (*Deps, *memstore.All) {
	t.Helper()
	reg := chain.NewRegistry()
	reg.Register(chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "ethereum", FinalityBlocks: 3, Height: 100, Balance: big.NewInt(1_000_000_000)}))
	stores := memstore.NewAll()
	bus := eventbus.NewBus(stores.Outbox, eventbus.NopPublisher{}, "")
	watcher := mempool.NewWatcher(nil, 0)
	svc := broadcast.NewService(reg, stores.Broadcast, stores.Confirmation, nil, watcher, bus, nil, broadcast.Options{RetryMax: 1})
	return &Deps{
		Registry:   reg,
		Broadcast:  svc,
		Broadcasts: stores.Broadcast,
		Confirms:   stores.Confirmation,
		Tips:       stores.Tip,
		Bus:        bus,
	}, stores
}

func TestHandleBroadcastHappyPath(t *testing.T) {
	deps, _ := newTestDeps(t)
	r := NewRouter(deps)
	body := bytes.NewBufferString(`{"signed_tx":"0xdeadbeef","from":"0xfrom","to":"0xto","value":"1000"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/chains/ethereum/broadcast", body)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("code: %d body: %s", rr.Code, rr.Body.String())
	}
	var resp map[string]any
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp["tx_hash"] == nil || resp["tx_hash"] == "" {
		t.Errorf("missing tx_hash: %+v", resp)
	}
}

func TestHandleBroadcastIdempotent(t *testing.T) {
	deps, _ := newTestDeps(t)
	r := NewRouter(deps)
	body := bytes.NewBufferString(`{"signed_tx":"0xdeadbeef"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/chains/ethereum/broadcast", body)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	first := rr.Body.String()
	// Re-broadcast.
	body2 := bytes.NewBufferString(`{"signed_tx":"0xdeadbeef"}`)
	req2 := httptest.NewRequest(http.MethodPost, "/v1/chains/ethereum/broadcast", body2)
	rr2 := httptest.NewRecorder()
	r.ServeHTTP(rr2, req2)
	if first != rr2.Body.String() {
		t.Errorf("idempotency broken:\nfirst: %s\nsecond: %s", first, rr2.Body.String())
	}
}

func TestHandleBroadcastUnknownChain(t *testing.T) {
	deps, _ := newTestDeps(t)
	r := NewRouter(deps)
	body := bytes.NewBufferString(`{"signed_tx":"0xdeadbeef"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/chains/nope/broadcast", body)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rr.Code)
	}
}

func TestHandleBroadcastMalformed(t *testing.T) {
	deps, _ := newTestDeps(t)
	r := NewRouter(deps)
	body := bytes.NewBufferString(`not json`)
	req := httptest.NewRequest(http.MethodPost, "/v1/chains/ethereum/broadcast", body)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rr.Code)
	}
}

func TestHandleBroadcastMissingSignedTx(t *testing.T) {
	deps, _ := newTestDeps(t)
	r := NewRouter(deps)
	body := bytes.NewBufferString(`{}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/chains/ethereum/broadcast", body)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rr.Code)
	}
}

func TestHandleEstimateFee(t *testing.T) {
	deps, _ := newTestDeps(t)
	r := NewRouter(deps)
	body := bytes.NewBufferString(`{"to":"0xto","priority":"high"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/chains/ethereum/estimate-fee", body)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("code: %d body: %s", rr.Code, rr.Body.String())
	}
	var resp map[string]any
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp["gas_limit"] == nil {
		t.Errorf("missing gas_limit: %+v", resp)
	}
}

func TestHandleHeight(t *testing.T) {
	deps, _ := newTestDeps(t)
	r := NewRouter(deps)
	req := httptest.NewRequest(http.MethodGet, "/v1/chains/ethereum/height", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("code: %d", rr.Code)
	}
	var resp map[string]any
	_ = json.NewDecoder(rr.Body).Decode(&resp)
	if resp["height"] == nil {
		t.Errorf("missing height: %+v", resp)
	}
}

func TestHandleBalance(t *testing.T) {
	deps, _ := newTestDeps(t)
	r := NewRouter(deps)
	req := httptest.NewRequest(http.MethodGet, "/v1/chains/ethereum/address/0xabc/balance", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("code: %d", rr.Code)
	}
	var resp map[string]any
	_ = json.NewDecoder(rr.Body).Decode(&resp)
	if resp["balance"] == nil || resp["symbol"] != "ETH" {
		t.Errorf("balance: %+v", resp)
	}
}

func TestHandleGetTxStatusFromStore(t *testing.T) {
	deps, stores := newTestDeps(t)
	_ = stores.Confirmation.Upsert(context.Background(), &store.Confirmation{ChainID: "ethereum", TxHash: "0x1", Status: chain.StatusConfirmed, Confirmations: 3})
	r := NewRouter(deps)
	req := httptest.NewRequest(http.MethodGet, "/v1/chains/ethereum/tx/0x1/status", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("code: %d body: %s", rr.Code, rr.Body.String())
	}
	var resp map[string]any
	_ = json.NewDecoder(rr.Body).Decode(&resp)
	if resp["status"] != "confirmed" {
		t.Errorf("status: %+v", resp)
	}
}

func TestHandleGetTxFromAdapter(t *testing.T) {
	deps, _ := newTestDeps(t)
	reg := deps.Registry
	reg.StubEmitter("ethereum").SeedTx(&chain.Tx{ChainID: "ethereum", Hash: "0x1", From: "0xa", To: "0xb", Status: chain.StatusConfirmed}, nil)
	r := NewRouter(deps)
	req := httptest.NewRequest(http.MethodGet, "/v1/chains/ethereum/tx/0x1", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("code: %d body: %s", rr.Code, rr.Body.String())
	}
}

func TestHandleGetTxNotFound(t *testing.T) {
	deps, _ := newTestDeps(t)
	r := NewRouter(deps)
	req := httptest.NewRequest(http.MethodGet, "/v1/chains/ethereum/tx/0xmissing", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rr.Code)
	}
}

func TestHealthz(t *testing.T) {
	deps, _ := newTestDeps(t)
	r := NewRouter(deps)
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("code: %d", rr.Code)
	}
}

func TestDecodePayload(t *testing.T) {
	hexBytes, err := decodePayload("0xdeadbeef")
	if err != nil || len(hexBytes) != 4 {
		t.Fatalf("hex: %v %v", hexBytes, err)
	}
	b64, err := decodePayload("aGVsbG8=")
	if err != nil || string(b64) != "hello" {
		t.Fatalf("base64: %v %v", b64, err)
	}
}

func TestSymbolFor(t *testing.T) {
	if symbolFor("ethereum") != "ETH" {
		t.Error("ethereum symbol")
	}
	if symbolFor("solana") != "SOL" {
		t.Error("solana symbol")
	}
	if symbolFor("bitcoin") != "BTC" {
		t.Error("bitcoin symbol")
	}
}