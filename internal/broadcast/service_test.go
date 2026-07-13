package broadcast

import (
	"context"
	"errors"
	"math/big"
	"testing"

	"github.com/ai-crypto-onramp/blockchain-gateway/internal/chain"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/eventbus"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/mempool"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/prepayment"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/store/memstore"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/walletclient"
)

func newTestService(t *testing.T) (*Service, *chain.Registry, *memstore.All) {
	t.Helper()
	reg := chain.NewRegistry()
	reg.Register(chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "ethereum", FinalityBlocks: 3}))
	stores := memstore.NewAll()
	bus := eventbus.NewBus(stores.Outbox, eventbus.NopPublisher{}, "")
	watcher := mempool.NewWatcher(nil, 0)
	svc := NewService(reg, stores.Broadcast, stores.Confirmation, nil, watcher, bus, nil, Options{Timeout: 0, RetryMax: 1})
	return svc, reg, stores
}

func TestBroadcastHappyPath(t *testing.T) {
	svc, _, stores := newTestService(t)
	resp, err := svc.Broadcast(context.Background(), &Request{
		ChainID:  "ethereum",
		SignedTx: []byte("signed-payload"),
		From:     "0xfrom",
		To:       "0xto",
		Value:    "1000",
	})
	if err != nil {
		t.Fatalf("broadcast: %v", err)
	}
	if resp.TxHash == "" {
		t.Fatal("missing tx hash")
	}
	exists, _ := stores.Broadcast.Exists(context.Background(), "ethereum", resp.TxHash)
	if !exists {
		t.Fatal("broadcast row not persisted")
	}
}

func TestBroadcastIdempotent(t *testing.T) {
	svc, reg, _ := newTestService(t)
	stub := reg.AsStub("ethereum")
	req := &Request{ChainID: "ethereum", SignedTx: []byte("signed-payload")}
	r1, err := svc.Broadcast(context.Background(), req)
	if err != nil {
		t.Fatalf("r1: %v", err)
	}
	r2, err := svc.Broadcast(context.Background(), req)
	if err != nil {
		t.Fatalf("r2: %v", err)
	}
	if r1.TxHash != r2.TxHash {
		t.Errorf("idempotency: %s != %s", r1.TxHash, r2.TxHash)
	}
	// The stub adapter should only have been called once (the second
	// broadcast short-circuits via the persisted row).
	if stub.BroadcastCount() != 1 {
		t.Errorf("adapter broadcast count: %d want 1", stub.BroadcastCount())
	}
}

func TestBroadcastUnknownChain(t *testing.T) {
	svc, _, _ := newTestService(t)
	_, err := svc.Broadcast(context.Background(), &Request{ChainID: "nope", SignedTx: []byte("x")})
	if err == nil {
		t.Fatal("expected unknown chain error")
	}
}

func TestBroadcastMalformed(t *testing.T) {
	svc, _, _ := newTestService(t)
	_, err := svc.Broadcast(context.Background(), &Request{ChainID: "ethereum"})
	if err == nil {
		t.Fatal("expected bad request")
	}
	if !errors.Is(err, ErrBadRequest) {
		t.Errorf("expected ErrBadRequest, got %v", err)
	}
}

func TestBroadcastAdapterError(t *testing.T) {
	reg := chain.NewRegistry()
	reg.Register(chain.NewStubAdapter(chain.StubAdapterOptions{
		ChainID:      "ethereum",
		FinalityBlocks: 3,
		BroadcastErr: errors.New("rpc timeout"),
	}))
	stores := memstore.NewAll()
	bus := eventbus.NewBus(stores.Outbox, eventbus.NopPublisher{}, "")
	svc := NewService(reg, stores.Broadcast, stores.Confirmation, nil, nil, bus, nil, Options{Timeout: 0, RetryMax: 2})
	_, err := svc.Broadcast(context.Background(), &Request{ChainID: "ethereum", SignedTx: []byte("x")})
	if err == nil {
		t.Fatal("expected adapter error")
	}
	if !errors.Is(err, ErrAdapter) {
		t.Errorf("expected ErrAdapter, got %v", err)
	}
}

func TestBroadcastWithPrepayment(t *testing.T) {
	reg := chain.NewRegistry()
	reg.Register(chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "ethereum", FinalityBlocks: 3, Balance: big.NewInt(0), FeeEstimate: &chain.FeeEstimate{TotalFee: big.NewInt(1)}}))
	stores := memstore.NewAll()
	bus := eventbus.NewBus(stores.Outbox, eventbus.NopPublisher{}, "")
	mock := walletclient.NewMock("0xfunding", 7)
	locks := prepayment.NewCoordinator(prepayment.NewMemRedis(), 0, 0)
	prepay := prepayment.NewManager(mock, locks, 0)
	watcher := mempool.NewWatcher(nil, 0)
	svc := NewService(reg, stores.Broadcast, stores.Confirmation, prepay, watcher, bus, nil, Options{RetryMax: 1})
	resp, err := svc.Broadcast(context.Background(), &Request{
		ChainID:  "ethereum",
		SignedTx: []byte("signed"),
		From:     "0xsnd",
	})
	if err != nil {
		t.Fatalf("broadcast: %v", err)
	}
	if resp.Nonce != 7 {
		t.Errorf("nonce: %d want 7", resp.Nonce)
	}
}

func TestIsTransient(t *testing.T) {
	if !isTransient(errors.New("context deadline exceeded: timeout")) {
		t.Error("timeout should be transient")
	}
	if !isTransient(errors.New("connection reset by peer")) {
		t.Error("connection reset should be transient")
	}
	if isTransient(errors.New("nonce too low")) {
		t.Error("nonce too low should not be transient")
	}
}