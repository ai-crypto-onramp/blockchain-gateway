package memstore

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/ai-crypto-onramp/blockchain-gateway/internal/chain"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/store"
)

func TestBroadcastStore(t *testing.T) {
	s := NewBroadcastStore()
	ctx := context.Background()
	b := &store.Broadcast{ChainID: "ethereum", TxHash: "0x1", SignedTx: []byte("tx"), Value: big.NewInt(100)}
	if err := s.Insert(ctx, b); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := s.Insert(ctx, b); err != nil {
		t.Fatalf("duplicate insert should be no-op: %v", err)
	}
	exists, _ := s.Exists(ctx, "ethereum", "0x1")
	if !exists {
		t.Fatal("should exist")
	}
	got, err := s.GetByTxHash(ctx, "ethereum", "0x1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Value.Cmp(big.NewInt(100)) != 0 {
		t.Errorf("value: %s", got.Value)
	}
	if _, err := s.GetByTxHash(ctx, "ethereum", "0xmissing"); err == nil {
		t.Fatal("expected not found")
	}
}

func TestConfirmationStoreTransitions(t *testing.T) {
	s := NewConfirmationStore()
	ctx := context.Background()
	c := &store.Confirmation{ChainID: "ethereum", TxHash: "0x1", Status: chain.StatusMempool}
	_ = s.Upsert(ctx, c)
	// Valid transition.
	_, ok, err := s.Transition(ctx, "ethereum", "0x1", chain.StatusMempool, chain.StatusConfirmed, func(upd *store.Confirmation) {
		upd.Confirmations = 1
		upd.ConfirmedAt = time.Now()
	})
	if err != nil || !ok {
		t.Fatalf("transition: %v ok=%v", err, ok)
	}
	// Idempotent: second call from Mempool should not transition (status already Confirmed).
	_, ok, _ = s.Transition(ctx, "ethereum", "0x1", chain.StatusMempool, chain.StatusConfirmed, nil)
	if ok {
		t.Fatal("should not transition from stale status")
	}
	// Invalid transition.
	_, _, err = s.Transition(ctx, "ethereum", "0x1", chain.StatusConfirmed, chain.StatusMempool, nil)
	if err == nil {
		t.Fatal("expected invalid transition error")
	}
}

func TestConfirmationStoreListByStatus(t *testing.T) {
	s := NewConfirmationStore()
	ctx := context.Background()
	_ = s.Upsert(ctx, &store.Confirmation{ChainID: "ethereum", TxHash: "0x1", Status: chain.StatusMempool})
	_ = s.Upsert(ctx, &store.Confirmation{ChainID: "ethereum", TxHash: "0x2", Status: chain.StatusConfirmed})
	_ = s.Upsert(ctx, &store.Confirmation{ChainID: "ethereum", TxHash: "0x3", Status: chain.StatusMempool})
	got, err := s.ListByStatus(ctx, "ethereum", chain.StatusMempool)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("mempool count: %d want 2", len(got))
	}
}

func TestConfirmationStoreListAboveHeight(t *testing.T) {
	s := NewConfirmationStore()
	ctx := context.Background()
	_ = s.Upsert(ctx, &store.Confirmation{ChainID: "ethereum", TxHash: "0x1", Status: chain.StatusConfirmed, BlockHeight: 5})
	_ = s.Upsert(ctx, &store.Confirmation{ChainID: "ethereum", TxHash: "0x2", Status: chain.StatusConfirmed, BlockHeight: 10})
	got, _ := s.ListAboveHeight(ctx, "ethereum", 7)
	if len(got) != 1 || got[0].TxHash != "0x2" {
		t.Errorf("above height: %+v", got)
	}
}

func TestTipStore(t *testing.T) {
	s := NewTipStore()
	ctx := context.Background()
	_ = s.Upsert(ctx, &store.Tip{ChainID: "ethereum", TipHeight: 100, TipHash: "0xabc"})
	got, err := s.Get(ctx, "ethereum")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.TipHeight != 100 {
		t.Errorf("height: %d", got.TipHeight)
	}
	_ = s.Upsert(ctx, &store.Tip{ChainID: "ethereum", TipHeight: 101, TipHash: "0xdef"})
	got, _ = s.Get(ctx, "ethereum")
	if got.TipHeight != 101 {
		t.Errorf("upsert height: %d", got.TipHeight)
	}
}

func TestFeeStore(t *testing.T) {
	s := NewFeeStore()
	ctx := context.Background()
	_ = s.Insert(ctx, &store.FeeEstimateRow{ChainID: "ethereum", Priority: chain.PriorityStandard, GasPrice: big.NewInt(1), ComputedAt: time.Now().Add(-time.Minute)})
	_ = s.Insert(ctx, &store.FeeEstimateRow{ChainID: "ethereum", Priority: chain.PriorityStandard, GasPrice: big.NewInt(2), ComputedAt: time.Now()})
	got, err := s.Latest(ctx, "ethereum", chain.PriorityStandard)
	if err != nil {
		t.Fatalf("latest: %v", err)
	}
	if got.GasPrice.Cmp(big.NewInt(2)) != 0 {
		t.Errorf("latest gas price: %s", got.GasPrice)
	}
}

func TestReorgStore(t *testing.T) {
	s := NewReorgStore()
	ctx := context.Background()
	_ = s.Append(ctx, &store.ReorgEvent{ChainID: "ethereum", OldTipHash: "0xold", NewTipHash: "0xnew", AffectedTxHashes: []string{"0x1", "0x2"}})
	got, _ := s.List(ctx, "ethereum")
	if len(got) != 1 || len(got[0].AffectedTxHashes) != 2 {
		t.Errorf("reorg list: %+v", got)
	}
}

func TestOutboxDedup(t *testing.T) {
	s := NewOutboxStore()
	ctx := context.Background()
	e1 := &store.OutboxEntry{ChainID: "ethereum", TxHash: "0x1", Status: chain.StatusConfirmed, BlockHeight: 100, EventType: "tx.confirmed", Payload: []byte("{}")}
	e2 := &store.OutboxEntry{ChainID: "ethereum", TxHash: "0x1", Status: chain.StatusConfirmed, BlockHeight: 100, EventType: "tx.confirmed", Payload: []byte("{}")}
	inserted1, _ := s.Append(ctx, e1)
	inserted2, _ := s.Append(ctx, e2)
	if !inserted1 {
		t.Fatal("first append should insert")
	}
	if inserted2 {
		t.Fatal("duplicate append should not insert")
	}
	pending, _ := s.ListPending(ctx, 10)
	if len(pending) != 1 {
		t.Errorf("pending: %d", len(pending))
	}
	_ = s.MarkEmitted(ctx, pending[0].ID)
	pending, _ = s.ListPending(ctx, 10)
	if len(pending) != 0 {
		t.Errorf("pending after mark: %d", len(pending))
	}
}

func TestAllComposite(t *testing.T) {
	all := NewAll()
	if all.Broadcast == nil || all.Confirmation == nil || all.Tip == nil || all.Fee == nil || all.Reorg == nil || all.Outbox == nil {
		t.Fatal("missing store in composite")
	}
}