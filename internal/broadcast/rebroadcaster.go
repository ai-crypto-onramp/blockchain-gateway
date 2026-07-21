package broadcast

import (
	"context"
	"errors"
	"fmt"

	"github.com/ai-crypto-onramp/blockchain-gateway/internal/chain"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/store"
)

// Rebroadcaster re-submits a previously-broadcast transaction to the
// chain mempool by looking up the original signed tx bytes from the
// BroadcastStore and re-broadcasting them via the chain adapter. It
// satisfies internal/reorg.Rebroadcaster.
type Rebroadcaster struct {
	registry   *chain.Registry
	broadcasts store.BroadcastStore
}

// NewRebroadcaster returns a Rebroadcaster wired to the given registry
// and broadcast store.
func NewRebroadcaster(reg *chain.Registry, b store.BroadcastStore) *Rebroadcaster {
	return &Rebroadcaster{registry: reg, broadcasts: b}
}

// Rebroadcast re-submits the signed tx bytes for (chainID, txHash) to
// the chain mempool. It returns ErrUnknownTx if no broadcast row exists.
func (r *Rebroadcaster) Rebroadcast(ctx context.Context, chainID, txHash string) error {
	adapter, err := r.registry.Get(chainID)
	if err != nil {
		return err
	}
	b, err := r.broadcasts.GetByTxHash(ctx, chainID, txHash)
	if err != nil {
		return fmt.Errorf("rebroadcast lookup %s/%s: %w", chainID, txHash, err)
	}
	if len(b.SignedTx) == 0 {
		return errors.New("rebroadcast: empty signed tx")
	}
	if _, err := adapter.Broadcast(ctx, b.SignedTx); err != nil {
		return fmt.Errorf("rebroadcast %s/%s: %w", chainID, txHash, err)
	}
	return nil
}
