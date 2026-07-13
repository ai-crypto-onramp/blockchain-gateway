package confirmation

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ai-crypto-onramp/blockchain-gateway/internal/chain"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/store"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/store/memstore"
)

// errEmitter is an Emitter that always returns an error.
type errEmitter struct{}

func (errEmitter) Emit(_ context.Context, _ Event) error { return errors.New("boom") }

func TestNewWorkerPoolDefaultsWorkers(t *testing.T) {
	stub := chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "ethereum", FinalityBlocks: 3})
	p := NewWorkerPool(0, memstore.NewConfirmationStore(), stub, nil)
	defer p.Stop()
	if p.workers != 4 {
		t.Errorf("workers: %d want 4", p.workers)
	}
}

// TestProcessMempoolToConfirmed exercises the BlockHeight==0 branch where
// the adapter returns a block height and the tx transitions from
// StatusMempool to StatusConfirmed.
func TestProcessMempoolToConfirmed(t *testing.T) {
	stores := memstore.NewAll()
	stub := chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "ethereum", FinalityBlocks: 3})
	reg := chain.NewRegistry()
	reg.Register(stub)
	reg.StubEmitter("ethereum").SeedTx(
		&chain.Tx{ChainID: "ethereum", Hash: "0x1", Status: chain.StatusMempool},
		&chain.TxStatus{ChainID: "ethereum", TxHash: "0x1", Status: chain.StatusConfirmed, BlockHeight: 10, Confirmations: 1},
	)
	em := &captureEmitter{}
	pool := NewWorkerPool(2, stores.Confirmation, stub, em)
	defer pool.Stop()
	_ = stores.Confirmation.Upsert(context.Background(), &store.Confirmation{
		ChainID: "ethereum", TxHash: "0x1", Status: chain.StatusMempool,
	})
	pool.Track("ethereum", "0x1")
	pool.OnHead("ethereum", 10)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c, _ := stores.Confirmation.Get(context.Background(), "ethereum", "0x1")
		if c != nil && c.Status == chain.StatusConfirmed {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	c, _ := stores.Confirmation.Get(context.Background(), "ethereum", "0x1")
	if c == nil || c.Status != chain.StatusConfirmed {
		t.Fatalf("expected confirmed, got %+v", c)
	}
	if c.BlockHeight != 10 {
		t.Errorf("block height: %d want 10", c.BlockHeight)
	}
	evs := em.Events()
	if len(evs) == 0 || evs[0].Type != "tx.confirmed" {
		t.Errorf("expected tx.confirmed event, got %+v", evs)
	}
}

// TestProcessStatusBroadcastToConfirmed exercises the StatusBroadcast ->
// StatusConfirmed transition inside the BlockHeight==0 branch.
func TestProcessStatusBroadcastToConfirmed(t *testing.T) {
	stores := memstore.NewAll()
	stub := chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "ethereum", FinalityBlocks: 3})
	reg := chain.NewRegistry()
	reg.Register(stub)
	reg.StubEmitter("ethereum").SeedTx(
		&chain.Tx{ChainID: "ethereum", Hash: "0xb", Status: chain.StatusBroadcast},
		&chain.TxStatus{ChainID: "ethereum", TxHash: "0xb", Status: chain.StatusConfirmed, BlockHeight: 4, Confirmations: 1},
	)
	em := &captureEmitter{}
	pool := NewWorkerPool(2, stores.Confirmation, stub, em)
	defer pool.Stop()
	_ = stores.Confirmation.Upsert(context.Background(), &store.Confirmation{
		ChainID: "ethereum", TxHash: "0xb", Status: chain.StatusBroadcast,
	})
	pool.Track("ethereum", "0xb")
	pool.OnHead("ethereum", 4)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c, _ := stores.Confirmation.Get(context.Background(), "ethereum", "0xb")
		if c != nil && c.Status == chain.StatusConfirmed {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	c, _ := stores.Confirmation.Get(context.Background(), "ethereum", "0xb")
	if c == nil || c.Status != chain.StatusConfirmed {
		t.Fatalf("expected confirmed, got %+v", c)
	}
}

// TestProcessTerminalSkips ensures process returns early when the stored
// confirmation is already terminal.
func TestProcessTerminalSkips(t *testing.T) {
	stores := memstore.NewAll()
	stub := chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "ethereum", FinalityBlocks: 3})
	em := &captureEmitter{}
	pool := NewWorkerPool(2, stores.Confirmation, stub, em)
	defer pool.Stop()
	_ = stores.Confirmation.Upsert(context.Background(), &store.Confirmation{
		ChainID: "ethereum", TxHash: "0xf", Status: chain.StatusFinalized, BlockHeight: 10, Confirmations: 100,
	})
	pool.Track("ethereum", "0xf")
	pool.OnHead("ethereum", 50)
	time.Sleep(80 * time.Millisecond)
	if len(em.Events()) != 0 {
		t.Errorf("terminal tx should not emit events, got %+v", em.Events())
	}
}

// TestProcessAdapterErrorSkips verifies that when GetTxStatus returns an
// error, process leaves the tx in mempool.
func TestProcessAdapterErrorSkips(t *testing.T) {
	stores := memstore.NewAll()
	stub := chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "ethereum", FinalityBlocks: 3})
	em := &captureEmitter{}
	pool := NewWorkerPool(2, stores.Confirmation, stub, em)
	defer pool.Stop()
	_ = stores.Confirmation.Upsert(context.Background(), &store.Confirmation{
		ChainID: "ethereum", TxHash: "0xmissing", Status: chain.StatusMempool,
	})
	pool.Track("ethereum", "0xmissing")
	pool.OnHead("ethereum", 100)
	time.Sleep(80 * time.Millisecond)
	c, _ := stores.Confirmation.Get(context.Background(), "ethereum", "0xmissing")
	if c == nil || c.Status != chain.StatusMempool {
		t.Errorf("mempool tx should remain, got %+v", c)
	}
	if len(em.Events()) != 0 {
		t.Errorf("no events expected, got %+v", em.Events())
	}
}

// TestProcessStoreGetError exercises the early-return branch when the
// store has no record for the tracked tx.
func TestProcessStoreGetError(t *testing.T) {
	stores := memstore.NewAll()
	stub := chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "ethereum", FinalityBlocks: 3})
	em := &captureEmitter{}
	pool := NewWorkerPool(1, stores.Confirmation, stub, em)
	defer pool.Stop()
	// Track a tx that was never Upserted; process should return early.
	pool.Track("ethereum", "0xghost")
	pool.OnHead("ethereum", 1)
	time.Sleep(50 * time.Millisecond)
	if len(em.Events()) != 0 {
		t.Errorf("no events expected for ghost tx, got %+v", em.Events())
	}
}

// TestProcessAdapterReturnsZeroBlockHeight exercises the branch where
// GetTxStatus returns BlockHeight==0 (still in mempool).
func TestProcessAdapterReturnsZeroBlockHeight(t *testing.T) {
	stores := memstore.NewAll()
	stub := chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "ethereum", FinalityBlocks: 3})
	reg := chain.NewRegistry()
	reg.Register(stub)
	reg.StubEmitter("ethereum").SeedTx(
		&chain.Tx{ChainID: "ethereum", Hash: "0xz", Status: chain.StatusMempool},
		&chain.TxStatus{ChainID: "ethereum", TxHash: "0xz", Status: chain.StatusMempool, BlockHeight: 0},
	)
	em := &captureEmitter{}
	pool := NewWorkerPool(1, stores.Confirmation, stub, em)
	defer pool.Stop()
	_ = stores.Confirmation.Upsert(context.Background(), &store.Confirmation{
		ChainID: "ethereum", TxHash: "0xz", Status: chain.StatusMempool,
	})
	pool.Track("ethereum", "0xz")
	pool.OnHead("ethereum", 5)
	time.Sleep(80 * time.Millisecond)
	c, _ := stores.Confirmation.Get(context.Background(), "ethereum", "0xz")
	if c == nil || c.Status != chain.StatusMempool {
		t.Errorf("should still be mempool, got %+v", c)
	}
}

// TestProcessUpdatesConfirmations verifies the confirmation-count update
// path for an already-confirmed tx that is not yet finalized.
func TestProcessUpdatesConfirmations(t *testing.T) {
	stores := memstore.NewAll()
	stub := chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "ethereum", FinalityBlocks: 10})
	em := &captureEmitter{}
	pool := NewWorkerPool(2, stores.Confirmation, stub, em)
	defer pool.Stop()
	_ = stores.Confirmation.Upsert(context.Background(), &store.Confirmation{
		ChainID:     "ethereum",
		TxHash:      "0xc",
		Status:      chain.StatusConfirmed,
		BlockHeight: 10,
		Confirmations: 1,
	})
	pool.Track("ethereum", "0xc")
	// Tip = 15 -> confirmations = 15 - 10 + 1 = 6 (still < finality 10).
	pool.OnHead("ethereum", 15)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c, _ := stores.Confirmation.Get(context.Background(), "ethereum", "0xc")
		if c != nil && c.Confirmations == 6 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	c, _ := stores.Confirmation.Get(context.Background(), "ethereum", "0xc")
	if c == nil || c.Confirmations != 6 {
		t.Errorf("confirmations: %d want 6", c.Confirmations)
	}
	if c.Status != chain.StatusConfirmed {
		t.Errorf("should still be confirmed, got %s", c.Status)
	}
}

// TestProcessFinalizedTransition verifies the confirmed->finalized
// transition when confirmations reach finality.
func TestProcessFinalizedTransition(t *testing.T) {
	stores := memstore.NewAll()
	stub := chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "ethereum", FinalityBlocks: 3})
	em := &captureEmitter{}
	pool := NewWorkerPool(2, stores.Confirmation, stub, em)
	defer pool.Stop()
	_ = stores.Confirmation.Upsert(context.Background(), &store.Confirmation{
		ChainID:     "ethereum",
		TxHash:      "0xd",
		Status:      chain.StatusConfirmed,
		BlockHeight: 10,
		Confirmations: 1,
	})
	pool.Track("ethereum", "0xd")
	// tip = 12 -> confs = 12 - 10 + 1 = 3 == finality -> finalized.
	pool.OnHead("ethereum", 12)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c, _ := stores.Confirmation.Get(context.Background(), "ethereum", "0xd")
		if c != nil && c.Status == chain.StatusFinalized {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	c, _ := stores.Confirmation.Get(context.Background(), "ethereum", "0xd")
	if c == nil || c.Status != chain.StatusFinalized {
		t.Fatalf("expected finalized, got %+v", c)
	}
	var sawFinalized bool
	for _, ev := range em.Events() {
		if ev.Type == "tx.finalized" {
			sawFinalized = true
		}
	}
	if !sawFinalized {
		t.Error("expected tx.finalized event")
	}
}

// TestProcessEmitErrorIsIgnored ensures a failing emitter does not break
// the processing loop.
func TestProcessEmitErrorIsIgnored(t *testing.T) {
	stores := memstore.NewAll()
	stub := chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "ethereum", FinalityBlocks: 3})
	pool := NewWorkerPool(1, stores.Confirmation, stub, errEmitter{})
	defer pool.Stop()
	_ = stores.Confirmation.Upsert(context.Background(), &store.Confirmation{
		ChainID: "ethereum", TxHash: "0xe", Status: chain.StatusConfirmed, BlockHeight: 10, Confirmations: 1,
	})
	pool.Track("ethereum", "0xe")
	pool.OnHead("ethereum", 12)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c, _ := stores.Confirmation.Get(context.Background(), "ethereum", "0xe")
		if c != nil && c.Status == chain.StatusFinalized {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	c, _ := stores.Confirmation.Get(context.Background(), "ethereum", "0xe")
	if c == nil || c.Status != chain.StatusFinalized {
		t.Errorf("expected finalized despite emit error, got %+v", c)
	}
}

// TestOnHeadDropsWhenQueueFull exercises the default branch in OnHead
// where the worker queue is full.
func TestOnHeadDropsWhenQueueFull(t *testing.T) {
	stores := memstore.NewAll()
	stub := chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "ethereum", FinalityBlocks: 3})
	// Build a pool with a single worker and a tiny queue (capacity 64 by
	// construction). We block the worker by holding a lock on the store so
	// process cannot drain. Easier: just stop the pool's workers by
	// closing nothing — instead we flood OnHead with more jobs than the
	// queue holds and assert no panic.
	pool := NewWorkerPool(1, stores.Confirmation, stub, nil)
	defer pool.Stop()
	for i := 0; i < 200; i++ {
		pool.Track("ethereum", "0xflood")
	}
	// Flood more than queue capacity (64); OnHead must not block.
	done := make(chan struct{})
	go func() {
		for i := 0; i < 200; i++ {
			pool.OnHead("ethereum", uint64(i))
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("OnHead blocked on full queue")
	}
}

// TestEmitNilEmitterNoOp ensures emit is a no-op when emitter is nil.
func TestEmitNilEmitterNoOp(t *testing.T) {
	stores := memstore.NewAll()
	stub := chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "ethereum", FinalityBlocks: 3})
	pool := NewWorkerPool(1, stores.Confirmation, stub, nil)
	defer pool.Stop()
	// Should not panic with a nil emitter.
	pool.emit(context.Background(), Event{Type: "tx.confirmed"})
}

func TestKey(t *testing.T) {
	if got := key("eth", "0x1"); got != "eth|0x1" {
		t.Errorf("key: %s want eth|0x1", got)
	}
}

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