// Package mempool monitors the service's own transactions in the chain
// mempool. It flags txs that exit the mempool without confirmation as
// dropped or replaced.
package mempool

import (
	"context"
	"sync"
	"time"

	"github.com/ai-crypto-onramp/blockchain-gateway/internal/chain"
)

// Watcher tracks mempool presence for own txs.
type Watcher struct {
	mu       sync.Mutex
	own      map[string]time.Time // key chain|txhash -> first-seen
	emitter  Emitter
	ttl      time.Duration
}

// Emitter is the event bus surface used by the mempool watcher.
type Emitter interface {
	EmitMempool(ctx context.Context, e Event) error
}

// Event is a mempool lifecycle event (tx.dropped / tx.replaced).
type Event struct {
	Type    string      `json:"type"`
	ChainID string      `json:"chain_id"`
	TxHash  string      `json:"tx_hash"`
	Status  chain.Status `json:"status"`
}

// NewWatcher returns a Watcher with the given presence TTL.
func NewWatcher(emitter Emitter, ttl time.Duration) *Watcher {
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return &Watcher{own: make(map[string]time.Time), emitter: emitter, ttl: ttl}
}

func key(chainID, txHash string) string { return chainID + "|" + txHash }

// Track registers an own tx as pending in the mempool.
func (w *Watcher) Track(chainID, txHash string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.own[key(chainID, txHash)] = time.Now()
}

// OnEvent handles a mempool entry/exit event for an own tx.
func (w *Watcher) OnEvent(ctx context.Context, e chain.MempoolEvent) {
	k := key(e.ChainID, e.TxHash)
	w.mu.Lock()
	_, tracked := w.own[k]
	w.mu.Unlock()
	if !tracked {
		return
	}
	if e.Kind == "exit" {
		w.mu.Lock()
		delete(w.own, k)
		w.mu.Unlock()
		// Determine dropped vs replaced: a re-broadcast of the same nonce
		// with a higher fee would be observed as a replacement. The
		// watcher surfaces both via the emitter; downstream services
		// disambiguate by inspecting the nonce.
		status := chain.StatusDropped
		evt := Event{Type: "tx.dropped", ChainID: e.ChainID, TxHash: e.TxHash, Status: status}
		if w.emitter != nil {
			_ = w.emitter.EmitMempool(ctx, evt)
		}
	}
}

// MarkReplaced transitions a tracked tx to the replaced state (test helper
// / future hook for nonce-replacement detection).
func (w *Watcher) MarkReplaced(ctx context.Context, chainID, txHash string) {
	w.mu.Lock()
	delete(w.own, key(chainID, txHash))
	w.mu.Unlock()
	if w.emitter != nil {
		_ = w.emitter.EmitMempool(ctx, Event{Type: "tx.replaced", ChainID: chainID, TxHash: txHash, Status: chain.StatusReplaced})
	}
}

// Pending returns the currently tracked own tx keys (test helper).
func (w *Watcher) Pending() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]string, 0, len(w.own))
	for k := range w.own {
		out = append(out, k)
	}
	return out
}

// Run consumes the adapter's SubscribeMempool channel until ctx is canceled.
func (w *Watcher) Run(ctx context.Context, sub <-chan chain.MempoolEvent) {
	for {
		select {
		case <-ctx.Done():
			return
		case e, ok := <-sub:
			if !ok {
				return
			}
			w.OnEvent(ctx, e)
		}
	}
}