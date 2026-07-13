package chain

import (
	"context"
	"math/big"
)

// ChainAdapter is the uniform interface every chain implementation must
// satisfy. It mirrors the API surface defined in the project README:
//
//	ChainID, Broadcast, GetTx, GetTxStatus, EstimateFee, Height, Balance,
//	SubscribeHeads, SubscribeMempool, FinalityBlocks.
//
// Implementations MUST be safe for concurrent use. The core service never
// signs transactions — Broadcast only submits an already-signed payload to
// the chain's mempool.
type ChainAdapter interface {
	// ChainID returns the canonical chain identifier (e.g. "ethereum").
	ChainID() string

	// Broadcast submits an already-signed transaction to the chain mempool
	// and returns the transaction hash. Idempotency is the caller's
	// responsibility: broadcasting the same signed bytes MUST yield the
	// same hash.
	Broadcast(ctx context.Context, signedTx []byte) (txHash string, err error)

	// GetTx fetches the chain-agnostic Tx record for the given hash.
	// Returns ErrTxNotFound if the tx is unknown to the node.
	GetTx(ctx context.Context, txHash string) (*Tx, error)

	// GetTxStatus returns the current confirmation status snapshot.
	GetTxStatus(ctx context.Context, txHash string) (*TxStatus, error)

	// EstimateFee produces a fee estimate for the given priority tier.
	EstimateFee(ctx context.Context, req FeeEstimateReq) (*FeeEstimate, error)

	// Height returns the current chain tip height.
	Height(ctx context.Context) (uint64, error)

	// Balance returns the native asset balance of addr in base units.
	Balance(ctx context.Context, addr string) (*big.Int, error)

	// SubscribeHeads opens a subscription to new chain heads. The returned
	// channel emits Head values until the cancel function is called or the
	// context is canceled. Implementations should fall back to polling when
	// a WebSocket subscription is unavailable.
	SubscribeHeads(ctx context.Context) (<-chan Head, func(), error)

	// SubscribeMempool opens a subscription to mempool entry/exit events
	// for transactions involving the service's own addresses. The returned
	// channel emits MempoolEvent values until the cancel function is called
	// or the context is canceled.
	SubscribeMempool(ctx context.Context, ownAddrs []string) (<-chan MempoolEvent, func(), error)

	// FinalityBlocks returns the number of confirmations required for a
	// transaction to be considered finalized on this chain.
	FinalityBlocks() uint64
}