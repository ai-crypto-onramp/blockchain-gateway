package reorg

import (
	"context"
	"errors"
	"testing"

	"github.com/ai-crypto-onramp/blockchain-gateway/internal/chain"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/store"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/store/memstore"
)

type captureEmitter struct {
	events []Event
}

func (e *captureEmitter) EmitReorg(_ context.Context, ev Event) error {
	e.events = append(e.events, ev)
	return nil
}

func TestReorgNoPreviousTip(t *testing.T) {
	stores := memstore.NewAll()
	em := &captureEmitter{}
	d := NewDetector(stores.Tip, stores.Reorg, stores.Confirmation, em)
	res, err := d.OnHead(context.Background(), chain.Head{ChainID: "ethereum", Height: 10, Hash: "0x10", ParentHash: "0x9"})
	if err != nil {
		t.Fatalf("onhead: %v", err)
	}
	if res.Reorged {
		t.Fatal("should not reorg without previous tip")
	}
}

func TestReorgDetected(t *testing.T) {
	stores := memstore.NewAll()
	ctx := context.Background()
	_ = stores.Tip.Upsert(ctx, &store.Tip{ChainID: "ethereum", TipHeight: 10, TipHash: "0xOLD"})
	_ = stores.Confirmation.Upsert(ctx, &store.Confirmation{ChainID: "ethereum", TxHash: "0x1", Status: chain.StatusConfirmed, BlockHeight: 10})
	_ = stores.Confirmation.Upsert(ctx, &store.Confirmation{ChainID: "ethereum", TxHash: "0x2", Status: chain.StatusConfirmed, BlockHeight: 11})
	em := &captureEmitter{}
	d := NewDetector(stores.Tip, stores.Reorg, stores.Confirmation, em)
	res, err := d.OnHead(ctx, chain.Head{ChainID: "ethereum", Height: 11, Hash: "0xNEW", ParentHash: "0xOTHER"})
	if err != nil {
		t.Fatalf("onhead: %v", err)
	}
	if !res.Reorged {
		t.Fatal("expected reorg")
	}
	evs, _ := stores.Reorg.List(ctx, "ethereum")
	if len(evs) != 1 {
		t.Errorf("reorg events: %d want 1", len(evs))
	}
	// Common ancestor = min(11-1, 10) - 1 = 8. Txs above 8: both.
	if len(evs[0].AffectedTxHashes) != 2 {
		t.Errorf("affected: %v", evs[0].AffectedTxHashes)
	}
	// Confirmations should be marked reorged_out.
	c1, _ := stores.Confirmation.Get(ctx, "ethereum", "0x1")
	if c1.Status != chain.StatusReorgedOut {
		t.Errorf("c1 status: %s", c1.Status)
	}
	if len(em.events) == 0 {
		t.Error("expected reorg event emission")
	}
}

func TestNoReorgWhenParentMatches(t *testing.T) {
	stores := memstore.NewAll()
	ctx := context.Background()
	_ = stores.Tip.Upsert(ctx, &store.Tip{ChainID: "ethereum", TipHeight: 10, TipHash: "0xPARENT"})
	d := NewDetector(stores.Tip, stores.Reorg, stores.Confirmation, nil)
	res, err := d.OnHead(ctx, chain.Head{ChainID: "ethereum", Height: 11, Hash: "0x11", ParentHash: "0xPARENT"})
	if err != nil {
		t.Fatalf("onhead: %v", err)
	}
	if res.Reorged {
		t.Fatal("should not reorg when parent matches")
	}
}

type mockRebroadcaster struct {
	calls []struct{ ChainID, TxHash string }
	err   error
}

func (m *mockRebroadcaster) Rebroadcast(_ context.Context, chainID, txHash string) error {
	m.calls = append(m.calls, struct{ ChainID, TxHash string }{chainID, txHash})
	return m.err
}

// TestReorgRebroadcastsOnNextHead verifies that after a reorg marks txs
// as reorged_out, the next OnHead call re-broadcasts those still in
// reorged_out (i.e. absent from the new chain).
func TestReorgRebroadcastsOnNextHead(t *testing.T) {
	stores := memstore.NewAll()
	ctx := context.Background()
	_ = stores.Tip.Upsert(ctx, &store.Tip{ChainID: "ethereum", TipHeight: 10, TipHash: "0xOLD"})
	_ = stores.Confirmation.Upsert(ctx, &store.Confirmation{ChainID: "ethereum", TxHash: "0x1", Status: chain.StatusConfirmed, BlockHeight: 10})
	_ = stores.Confirmation.Upsert(ctx, &store.Confirmation{ChainID: "ethereum", TxHash: "0x2", Status: chain.StatusConfirmed, BlockHeight: 11})
	em := &captureEmitter{}
	d := NewDetector(stores.Tip, stores.Reorg, stores.Confirmation, em)
	rb := &mockRebroadcaster{}
	d.SetRebroadcaster(rb)
	// First head: reorg. Txs 0x1 and 0x2 are marked reorged_out.
	if _, err := d.OnHead(ctx, chain.Head{ChainID: "ethereum", Height: 11, Hash: "0xNEW", ParentHash: "0xOTHER"}); err != nil {
		t.Fatalf("onhead1: %v", err)
	}
	if len(rb.calls) != 0 {
		t.Fatalf("rebroadcast should not fire on reorg head, got %d", len(rb.calls))
	}
	// Second head: re-broadcast pass runs before the parent-hash check.
	// Both txs are still reorged_out, so both are re-broadcast.
	if _, err := d.OnHead(ctx, chain.Head{ChainID: "ethereum", Height: 12, Hash: "0xNEW2", ParentHash: "0xNEW"}); err != nil {
		t.Fatalf("onhead2: %v", err)
	}
	if len(rb.calls) != 2 {
		t.Fatalf("rebroadcast calls: %d want 2", len(rb.calls))
	}
	// After re-broadcast, the txs should be transitioned back to MEMPOOL.
	c1, _ := stores.Confirmation.Get(ctx, "ethereum", "0x1")
	if c1.Status != chain.StatusMempool {
		t.Errorf("c1 status after rebroadcast: %s want MEMPOOL", c1.Status)
	}
}

// TestReorgSkipsReconfirmedTxs verifies that a tx which was reorged_out
// but has since been re-confirmed on the new chain (e.g. by the
// confirmation tracker) is NOT re-broadcast on the next head.
func TestReorgSkipsReconfirmedTxs(t *testing.T) {
	stores := memstore.NewAll()
	ctx := context.Background()
	_ = stores.Tip.Upsert(ctx, &store.Tip{ChainID: "ethereum", TipHeight: 10, TipHash: "0xOLD"})
	_ = stores.Confirmation.Upsert(ctx, &store.Confirmation{ChainID: "ethereum", TxHash: "0x1", Status: chain.StatusConfirmed, BlockHeight: 10})
	_ = stores.Confirmation.Upsert(ctx, &store.Confirmation{ChainID: "ethereum", TxHash: "0x2", Status: chain.StatusConfirmed, BlockHeight: 11})
	d := NewDetector(stores.Tip, stores.Reorg, stores.Confirmation, nil)
	rb := &mockRebroadcaster{}
	d.SetRebroadcaster(rb)
	if _, err := d.OnHead(ctx, chain.Head{ChainID: "ethereum", Height: 11, Hash: "0xNEW", ParentHash: "0xOTHER"}); err != nil {
		t.Fatalf("onhead1: %v", err)
	}
	// Simulate the confirmation tracker re-confirming 0x1 on the new chain.
	_, _, _ = stores.Confirmation.Transition(ctx, "ethereum", "0x1", chain.StatusReorgedOut, chain.StatusConfirmed, nil)
	if _, err := d.OnHead(ctx, chain.Head{ChainID: "ethereum", Height: 12, Hash: "0xNEW2", ParentHash: "0xNEW"}); err != nil {
		t.Fatalf("onhead2: %v", err)
	}
	if len(rb.calls) != 1 || rb.calls[0].TxHash != "0x2" {
		t.Fatalf("rebroadcast calls: %+v want only 0x2", rb.calls)
	}
}

// TestReorgRebroadcastWithoutRebroadcaster verifies the detector does not
// panic when no Rebroadcaster is wired (legacy behavior).
func TestReorgRebroadcastWithoutRebroadcaster(t *testing.T) {
	stores := memstore.NewAll()
	ctx := context.Background()
	_ = stores.Tip.Upsert(ctx, &store.Tip{ChainID: "ethereum", TipHeight: 10, TipHash: "0xOLD"})
	_ = stores.Confirmation.Upsert(ctx, &store.Confirmation{ChainID: "ethereum", TxHash: "0x1", Status: chain.StatusConfirmed, BlockHeight: 10})
	d := NewDetector(stores.Tip, stores.Reorg, stores.Confirmation, nil)
	if _, err := d.OnHead(ctx, chain.Head{ChainID: "ethereum", Height: 11, Hash: "0xNEW", ParentHash: "0xOTHER"}); err != nil {
		t.Fatalf("onhead1: %v", err)
	}
	if _, err := d.OnHead(ctx, chain.Head{ChainID: "ethereum", Height: 12, Hash: "0xNEW2", ParentHash: "0xNEW"}); err != nil {
		t.Fatalf("onhead2: %v", err)
	}
	c1, _ := stores.Confirmation.Get(ctx, "ethereum", "0x1")
	if c1.Status != chain.StatusReorgedOut {
		t.Errorf("c1 status without rebroadcaster: %s want REORGED_OUT", c1.Status)
	}
}

// TestReorgRebroadcastErrorContinues verifies that a rebroadcast error
// for one tx does not abort the pass for the rest.
func TestReorgRebroadcastErrorContinues(t *testing.T) {
	stores := memstore.NewAll()
	ctx := context.Background()
	_ = stores.Tip.Upsert(ctx, &store.Tip{ChainID: "ethereum", TipHeight: 10, TipHash: "0xOLD"})
	_ = stores.Confirmation.Upsert(ctx, &store.Confirmation{ChainID: "ethereum", TxHash: "0x1", Status: chain.StatusConfirmed, BlockHeight: 10})
	_ = stores.Confirmation.Upsert(ctx, &store.Confirmation{ChainID: "ethereum", TxHash: "0x2", Status: chain.StatusConfirmed, BlockHeight: 11})
	d := NewDetector(stores.Tip, stores.Reorg, stores.Confirmation, nil)
	rb := &mockRebroadcaster{err: errors.New("rpc down")}
	d.SetRebroadcaster(rb)
	if _, err := d.OnHead(ctx, chain.Head{ChainID: "ethereum", Height: 11, Hash: "0xNEW", ParentHash: "0xOTHER"}); err != nil {
		t.Fatalf("onhead1: %v", err)
	}
	if _, err := d.OnHead(ctx, chain.Head{ChainID: "ethereum", Height: 12, Hash: "0xNEW2", ParentHash: "0xNEW"}); err != nil {
		t.Fatalf("onhead2: %v", err)
	}
	if len(rb.calls) != 2 {
		t.Fatalf("expected both rebroadcast attempts despite error, got %d", len(rb.calls))
	}
	// The failed txs should remain in reorged_out (not transitioned).
	c1, _ := stores.Confirmation.Get(ctx, "ethereum", "0x1")
	if c1.Status != chain.StatusReorgedOut {
		t.Errorf("c1 status after failed rebroadcast: %s want REORGED_OUT", c1.Status)
	}
}
