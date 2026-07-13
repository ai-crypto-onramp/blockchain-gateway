package mempool

import (
	"context"
	"testing"
	"time"

	"github.com/ai-crypto-onramp/blockchain-gateway/internal/chain"
)

type captureEmitter struct {
	events []Event
}

func (e *captureEmitter) EmitMempool(_ context.Context, ev Event) error {
	e.events = append(e.events, ev)
	return nil
}

func TestWatcherTrackAndExit(t *testing.T) {
	em := &captureEmitter{}
	w := NewWatcher(em, 0)
	w.Track("ethereum", "0x1")
	if len(w.Pending()) != 1 {
		t.Fatalf("pending: %d", len(w.Pending()))
	}
	w.OnEvent(context.Background(), chain.MempoolEvent{ChainID: "ethereum", TxHash: "0x1", Kind: "enter"})
	if len(w.Pending()) != 1 {
		t.Errorf("enter should not remove: %d", len(w.Pending()))
	}
	w.OnEvent(context.Background(), chain.MempoolEvent{ChainID: "ethereum", TxHash: "0x1", Kind: "exit"})
	if len(w.Pending()) != 0 {
		t.Errorf("exit should remove: %d", len(w.Pending()))
	}
	if len(em.events) != 1 || em.events[0].Type != "tx.dropped" {
		t.Errorf("events: %+v", em.events)
	}
}

func TestWatcherIgnoresUnknownTx(t *testing.T) {
	em := &captureEmitter{}
	w := NewWatcher(em, 0)
	w.OnEvent(context.Background(), chain.MempoolEvent{ChainID: "ethereum", TxHash: "0xunknown", Kind: "exit"})
	if len(em.events) != 0 {
		t.Errorf("should ignore unknown tx")
	}
}

func TestWatcherMarkReplaced(t *testing.T) {
	em := &captureEmitter{}
	w := NewWatcher(em, 0)
	w.Track("ethereum", "0x1")
	w.MarkReplaced(context.Background(), "ethereum", "0x1")
	if len(w.Pending()) != 0 {
		t.Errorf("replaced should remove: %d", len(w.Pending()))
	}
	if len(em.events) != 1 || em.events[0].Type != "tx.replaced" {
		t.Errorf("events: %+v", em.events)
	}
}

// TestWatcherRunConsumesAndExitsOnClose exercises Run with a channel that
// emits one event then closes.
func TestWatcherRunConsumesAndExitsOnClose(t *testing.T) {
	em := &captureEmitter{}
	w := NewWatcher(em, 0)
	w.Track("ethereum", "0x1")
	ch := make(chan chain.MempoolEvent, 1)
	// An "exit" event emits a tx.dropped event for a tracked tx.
	ch <- chain.MempoolEvent{ChainID: "ethereum", TxHash: "0x1", Kind: "exit"}
	close(ch)
	done := make(chan struct{})
	go func() {
		w.Run(context.Background(), ch)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit after channel close")
	}
	if len(em.events) != 1 {
		t.Errorf("events: %d want 1", len(em.events))
	}
}

// TestWatcherRunExitsOnContextCancel exercises the ctx.Done() branch.
func TestWatcherRunExitsOnContextCancel(t *testing.T) {
	em := &captureEmitter{}
	w := NewWatcher(em, 0)
	ch := make(chan chain.MempoolEvent)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.Run(ctx, ch)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit on context cancel")
	}
}