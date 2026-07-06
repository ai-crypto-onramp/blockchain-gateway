package chain

import (
	"context"
	"math/big"
)

// ChainAdapter is the chain-agnostic interface that encapsulates all
// chain-specific behavior for transaction broadcast, querying, fee
// estimation, and tip/mempool streaming.
//
// The gateway's core service depends only on this interface; concrete
// implementations (EVM, Solana, Bitcoin, ...) are registered per chain_id and
// looked up via the Registry (see registry.go).
//
// The method set mirrors the README "ChainAdapter interface" section and the
// REST/gRPC API surface built on top of it:
//
//   - ChainID identifies the chain this adapter serves (e.g. "ethereum",
//     "polygon", "solana", "bitcoin").
//   - Broadcast submits an already-signed transaction (POST
//     /v1/chains/:chain/broadcast). The service never signs; it only
//     broadcasts.
//   - GetTx returns the full transaction record (GET
//     /v1/chains/:chain/tx/:hash).
//   - GetTxStatus returns the lifecycle status (GET
//     /v1/chains/:chain/tx/:hash/status).
//   - EstimateFee returns a fee estimate for a pending tx (POST
//     /v1/chains/:chain/estimate-fee).
//   - Height returns the current chain tip (GET /v1/chains/:chain/height).
//   - Balance returns the native balance of an address.
//   - SubscribeHeads streams new block heads (WS
//     /v1/chains/:chain/heads).
//   - SubscribeMempool streams pending tx events, optionally filtered to a
//     set of own addresses; used by the tip follower / mempool watcher
//     (PROJECT_PLAN.md Stage 7) to detect dropped/replaced txs.
//   - FinalityBlocks returns the chain's finality depth (from per-chain
//     config); a tx is never marked finalized before this many
//     confirmations.
//
// Implementations must be safe for concurrent use.
type ChainAdapter interface {
	// ChainID returns the chain identifier this adapter serves, e.g.
	// "ethereum", "polygon", "solana", "bitcoin". It must match the
	// chain_id under which the adapter is registered.
	ChainID() string

	// Broadcast submits an already-signed transaction to the chain's
	// mempool via JSON-RPC and returns the resulting tx hash.
	//
	// It corresponds to POST /v1/chains/:chain/broadcast. The service never
	// signs transactions; it only broadcasts already-signed payloads.
	// Broadcasting the same signed tx must be idempotent (same tx_hash).
	Broadcast(ctx context.Context, signedTx []byte) (txHash string, err error)

	// GetTx returns the full transaction record for the given hash, or an
	// error if the tx is unknown to the node.
	//
	// It corresponds to GET /v1/chains/:chain/tx/:hash.
	GetTx(ctx context.Context, txHash string) (*Tx, error)

	// GetTxStatus returns the current lifecycle status of a transaction,
	// including the number of confirmations and (if finalized) the
	// finalization timestamp.
	//
	// It corresponds to GET /v1/chains/:chain/tx/:hash/status.
	GetTxStatus(ctx context.Context, txHash string) (*TxStatus, error)

	// EstimateFee returns a fee estimate for a pending tx given the
	// requested priority tier.
	//
	// It corresponds to POST /v1/chains/:chain/estimate-fee.
	EstimateFee(ctx context.Context, req FeeEstimateReq) (*FeeEstimate, error)

	// Height returns the current best block height of the chain.
	//
	// It corresponds to GET /v1/chains/:chain/height.
	Height(ctx context.Context) (uint64, error)

	// Balance returns the native balance of the given address in the
	// chain's smallest unit (wei, lamports, satoshis).
	Balance(ctx context.Context, addr string) (*big.Int, error)

	// SubscribeHeads streams new block heads (Head) as they are produced.
	// The returned channel is closed when the context is cancelled or the
	// subscription is unsubscribed via the returned cleanup func.
	//
	// It corresponds to WS /v1/chains/:chain/heads.
	SubscribeHeads(ctx context.Context) (<-chan Head, func(), error)

	// SubscribeMempool streams pending transaction events from the node's
	// mempool, optionally filtered to the given set of own addresses (empty
	// slice = all pending txs). The returned channel is closed when the
	// context is cancelled or the subscription is unsubscribed via the
	// returned cleanup func.
	//
	// Used by the mempool watcher (Stage 7) to detect dropped/replaced txs.
	SubscribeMempool(ctx context.Context, ownAddrs []string) (<-chan MempoolEvent, func(), error)

	// FinalityBlocks returns the chain's finality depth (from per-chain
	// config FINALITY_BLOCKS_<CHAIN>). A tx is never marked finalized
	// before this many confirmations.
	FinalityBlocks() uint64
}
