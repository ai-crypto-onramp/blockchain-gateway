package confirmation

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ai-crypto-onramp/blockchain-gateway/internal/chain"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/store"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/store/memstore"
)

type captureEmitter struct {
	mu     sync.Mutex
	events []Event
}

func (e *captureEmitter) Emit(_ context.Context, ev Event) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.events = append(e.events, ev)
	return nil
}

func (e *captureEmitter) Events() []Event {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]Event(nil), e.events...)
}

func TestWorkerIndexSticky(t *testing.T) {
	stores := memstore.NewAll()
	stub := chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "ethereum", FinalityBlocks: 3})
	em := &captureEmitter{}
	pool := NewWorkerPool(4, stores.Confirmation, stub, em)
	defer pool.Stop()
	// Same (chain, tx) should always hash to the same worker.
	idx1 := pool.workerIndex("ethereum", "0x1")
	idx2 := pool.workerIndex("ethereum", "0x1")
	if idx1 != idx2 {
		t.Errorf("non-sticky: %d != %d", idx1, idx2)
	}
	// Different txs may go to different workers.
	idx3 := pool.workerIndex("ethereum", "0x2")
	if idx3 < 0 || idx3 >= 4 {
		t.Errorf("worker index out of range: %d", idx3)
	}
}

func TestTrackAndOnHead(t *testing.T) {
	stores := memstore.NewAll()
	stub := chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "ethereum", FinalityBlocks: 3})
	reg := chain.NewRegistry()
	reg.Register(stub)
	reg.AsStub("ethereum").SeedTx(&chain.Tx{ChainID: "ethereum", Hash: "0x1", BlockHeight: 10, Status: chain.StatusConfirmed}, &chain.TxStatus{ChainID: "ethereum", TxHash: "0x1", Status: chain.StatusConfirmed, BlockHeight: 10, Confirmations: 1})
	em := &captureEmitter{}
	pool := NewWorkerPool(4, stores.Confirmation, stub, em)
	defer pool.Stop()
	_ = stores.Confirmation.Upsert(context.Background(), &store.Confirmation{ChainID: "ethereum", TxHash: "0x1", Status: chain.StatusConfirmed, BlockHeight: 10, Confirmations: 1})
	pool.Track("ethereum", "0x1")
	pool.OnHead("ethereum", 13)
	// Wait for async processing.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		c, _ := stores.Confirmation.Get(context.Background(), "ethereum", "0x1")
		if c != nil && c.Status == chain.StatusFinalized {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	c, _ := stores.Confirmation.Get(context.Background(), "ethereum", "0x1")
	if c == nil || c.Status != chain.StatusFinalized {
		t.Fatalf("expected finalized, got %+v", c)
	}
	evs := em.Events()
	if len(evs) == 0 {
		t.Fatal("expected at least one event")
	}
}

func TestTrackedList(t *testing.T) {
	stores := memstore.NewAll()
	stub := chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "ethereum", FinalityBlocks: 3})
	pool := NewWorkerPool(2, stores.Confirmation, stub, nil)
	defer pool.Stop()
	pool.Track("ethereum", "0x1")
	pool.Track("ethereum", "0x2")
	pool.Track("ethereum", "0x1") // dup
	if len(pool.Tracked()) != 2 {
		t.Errorf("tracked: %d want 2", len(pool.Tracked()))
	}
}