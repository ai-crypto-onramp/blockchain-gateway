package chain

import (
	"context"
	"errors"
	"math/big"
	"sort"
)

// ErrChainUnknown is the typed error returned by Registry.Get when no adapter
// is registered for the requested chain id.
var ErrChainUnknown = errors.New("chain: unknown chain id")

// Registry maps chain ids to ChainAdapter implementations and provides typed
// lookup. It is the seam by which the chain-agnostic core service resolves a
// concrete adapter for a given chain_id at request time.
//
// The zero-value Registry is not usable; construct one with NewRegistry.
type Registry struct {
	adapters map[string]ChainAdapter
}

// NewRegistry returns an empty Registry. Adapters are added via Register.
func NewRegistry() *Registry {
	return &Registry{adapters: make(map[string]ChainAdapter)}
}

// Register registers an adapter under its ChainID(). Registering an adapter
// whose ChainID() is empty panics; re-registering an existing chain id
// panics.
func (r *Registry) Register(a ChainAdapter) {
	if a == nil {
		panic("chain: cannot register a nil adapter")
	}
	id := a.ChainID()
	if id == "" {
		panic("chain: cannot register an adapter with an empty chain id")
	}
	if _, ok := r.adapters[id]; ok {
		panic("chain: duplicate chain id registration: " + id)
	}
	r.adapters[id] = a
}

// Get returns the adapter registered for chainID, or a typed
// ErrChainUnknown error if no adapter is registered for that chain.
func (r *Registry) Get(chainID string) (ChainAdapter, error) {
	a, ok := r.adapters[chainID]
	if !ok {
		return nil, ErrChainUnknown
	}
	return a, nil
}

// Chains returns the sorted list of chain ids for which adapters are
// registered.
func (r *Registry) Chains() []string {
	out := make([]string, 0, len(r.adapters))
	for id := range r.adapters {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// stubAdapter is a no-op ChainAdapter used by unit tests of the registry and
// config loader. None of its methods perform any work.
type stubAdapter struct {
	id             string
	finalityBlocks uint64
}

// NewStubAdapter returns a no-op ChainAdapter with the given chain id and
// finality-blocks value, for use in unit tests.
func NewStubAdapter(id string, finalityBlocks uint64) ChainAdapter {
	return stubAdapter{id: id, finalityBlocks: finalityBlocks}
}

func (s stubAdapter) ChainID() string        { return s.id }
func (s stubAdapter) FinalityBlocks() uint64 { return s.finalityBlocks }
func (s stubAdapter) Broadcast(ctx context.Context, signedTx []byte) (string, error) {
	return "", nil
}
func (s stubAdapter) GetTx(ctx context.Context, txHash string) (*Tx, error) { return nil, nil }
func (s stubAdapter) GetTxStatus(ctx context.Context, txHash string) (*TxStatus, error) {
	return nil, nil
}
func (s stubAdapter) EstimateFee(ctx context.Context, req FeeEstimateReq) (*FeeEstimate, error) {
	return nil, nil
}
func (s stubAdapter) Height(ctx context.Context) (uint64, error) { return 0, nil }
func (s stubAdapter) Balance(ctx context.Context, addr string) (*big.Int, error) {
	return big.NewInt(0), nil
}
func (s stubAdapter) SubscribeHeads(ctx context.Context) (<-chan Head, func(), error) {
	ch := make(chan Head)
	close(ch)
	return ch, func() {}, nil
}
func (s stubAdapter) SubscribeMempool(ctx context.Context, ownAddrs []string) (<-chan MempoolEvent, func(), error) {
	ch := make(chan MempoolEvent)
	close(ch)
	return ch, func() {}, nil
}
