package reorg

import (
	"context"
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