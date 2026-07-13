// Package reorg detects chain reorganizations by comparing each new head's
// parent hash to the previously stored tip. On a mismatch it walks back to
// the common ancestor, records an append-only reorg_event, marks affected
// txs as reorged_out, and (after one block) re-broadcasts txs that are
// absent from the new chain.
package reorg

import (
	"context"
	"errors"
	"time"

	"github.com/ai-crypto-onramp/blockchain-gateway/internal/chain"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/store"
)

// Detector checks each new head against the stored tip. It is safe for
// concurrent use per chain (the tip follower is single-threaded in
// practice).
type Detector struct {
	tips     store.TipStore
	reorgs   store.ReorgStore
	confirms store.ConfirmationStore
	emitter  Emitter
}

// Emitter is the event bus surface used by the reorg detector.
type Emitter interface {
	EmitReorg(ctx context.Context, e Event) error
}

// Event is a reorg lifecycle event (tx.reorged).
type Event struct {
	Type        string   `json:"type"` // tx.reorged
	ChainID     string   `json:"chain_id"`
	TxHash      string   `json:"tx_hash"`
	Affected    []string `json:"affected_tx_hashes"`
	CommonAncestor uint64 `json:"common_ancestor_height"`
}

// NewDetector returns a Detector.
func NewDetector(tips store.TipStore, reorgs store.ReorgStore, confirms store.ConfirmationStore, emitter Emitter) *Detector {
	return &Detector{tips: tips, reorgs: reorgs, confirms: confirms, emitter: emitter}
}

// Result is what OnHead returns when a reorg is detected.
type Result struct {
	Reorged            bool
	CommonAncestor     uint64
	AffectedTxHashes   []string
}

// OnHead inspects a new head against the stored tip. If the parent hash
// does not match the previous tip hash, a reorg is recorded.
//
// The common ancestor height is computed via a simple heuristic: since the
// gateway only stores the latest tip (not a full header chain), we use
// min(newHead.Height-1, oldTip.Height) - 1 as a conservative lower bound.
// A production implementation would walk headers via the adapter.
func (d *Detector) OnHead(ctx context.Context, h chain.Head) (*Result, error) {
	oldTip, err := d.tips.Get(ctx, h.ChainID)
	if err != nil {
		var nf *store.ErrNotFound
		if errors.As(err, &nf) {
			return &Result{Reorged: false}, nil
		}
		return nil, err
	}
	if oldTip.TipHash == "" || oldTip.TipHash == h.ParentHash {
		return &Result{Reorged: false}, nil
	}
	// Reorg detected.
	common := oldTip.TipHeight
	if h.Height-1 < common {
		common = h.Height - 1
	}
	if common > 0 {
		common-- // conservative: one below the smaller tip
	}
	affected, err := d.confirms.ListAboveHeight(ctx, h.ChainID, common)
	if err != nil {
		return nil, err
	}
	hashes := make([]string, 0, len(affected))
	for _, c := range affected {
		if c.Status == chain.StatusConfirmed || c.Status == chain.StatusMempool {
			hashes = append(hashes, c.TxHash)
		}
		_, _, _ = d.confirms.Transition(ctx, h.ChainID, c.TxHash, c.Status, chain.StatusReorgedOut, nil)
	}
	ev := &store.ReorgEvent{
		ChainID:              h.ChainID,
		DetectedAt:           time.Now(),
		OldTipHash:           oldTip.TipHash,
		NewTipHash:           h.Hash,
		CommonAncestorHeight: common,
		AffectedTxHashes:     hashes,
	}
	if err := d.reorgs.Append(ctx, ev); err != nil {
		return nil, err
	}
	if d.emitter != nil {
		_ = d.emitter.EmitReorg(ctx, Event{Type: "tx.reorged", ChainID: h.ChainID, Affected: hashes, CommonAncestor: common})
	}
	return &Result{Reorged: true, CommonAncestor: common, AffectedTxHashes: hashes}, nil
}