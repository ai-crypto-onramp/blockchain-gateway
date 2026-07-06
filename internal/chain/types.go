// Package chain defines the chain-agnostic domain types referenced by the
// ChainAdapter interface (see adapter.go).
//
// The types here mirror the data model described in the README:
//   - Head: a chain tip block header (height, hash, parent_hash, timestamp),
//     streamed via SubscribeHeads and exposed by GET /v1/chains/:chain/height.
//   - MempoolEvent: an event emitted by SubscribeMempool for pending
//     transactions, optionally filtered to a set of own addresses.
//   - Tx: a transaction as returned by GetTx / GET /v1/chains/:chain/tx/:hash.
//   - TxStatus: the lifecycle status of a tx as returned by GetTxStatus /
//     GET /v1/chains/:chain/tx/:hash/status.
//   - FeeEstimateReq / FeeEstimate: input and output of EstimateFee /
//     POST /v1/chains/:chain/estimate-fee.
//
// All chain-specific behavior is encapsulated behind the ChainAdapter
// interface; these types are the lingua franca exchanged across it.
package chain

import (
	"math/big"
	"time"
)

// FeePriority is the urgency of a fee estimate request, mapping to the
// `priority` field of POST /v1/chains/:chain/estimate-fee
// ("low" | "standard" | "high").
type FeePriority string

const (
	FeePriorityLow      FeePriority = "low"
	FeePriorityStandard FeePriority = "standard"
	FeePriorityHigh     FeePriority = "high"
)

// FeeEstimateReq is the input to ChainAdapter.EstimateFee.
//
// It corresponds to the body of POST /v1/chains/:chain/estimate-fee:
// `{"to", "amount", "priority": "low|standard|high"}`.
type FeeEstimateReq struct {
	// To is the recipient address in the chain's native address format.
	To string
	// Amount is the transfer amount in the chain's smallest unit (wei, lamports,
	// satoshis). May be nil/zero for non-transfer invocations.
	Amount *big.Int
	// Priority selects the fee tier; defaults to FeePriorityStandard when empty.
	Priority FeePriority
}

// FeeEstimate is the output of ChainAdapter.EstimateFee.
//
// It corresponds to the response of POST /v1/chains/:chain/estimate-fee:
// `{"gas_limit", "max_fee_per_gas", "max_priority_fee_per_gas", "gas_price",
//
//	"total_fee", "priority"}`. EVM-style fields are populated for EVM chains;
//
// non-EVM chains populate the analogous fields (e.g. Solana priority fee in
// max_priority_fee_per_gas) and leave the rest zero.
type FeeEstimate struct {
	// GasLimit is the units of work the tx will consume (gas for EVM,
	// compute units for Solana, virtual bytes for Bitcoin).
	GasLimit uint64
	// MaxFeePerGas is the EIP-1559 max fee per gas unit (cap). Zero for
	// non-EIP-1559 chains.
	MaxFeePerGas *big.Int
	// MaxPriorityFeePerGas is the EIP-1559 priority tip paid to validators.
	// Zero for non-EIP-1559 chains.
	MaxPriorityFeePerGas *big.Int
	// GasPrice is the legacy gas price (type-0/1 txs) or the effective price
	// for non-EVM chains.
	GasPrice *big.Int
	// TotalFee is the estimated total fee for the tx in the chain's smallest
	// unit.
	TotalFee *big.Int
	// Priority echoes the requested priority tier.
	Priority FeePriority
}

// Head is a chain tip block header, produced by ChainAdapter.SubscribeHeads
// and exposed by GET /v1/chains/:chain/height.
//
// It mirrors the WS /v1/chains/:chain/heads stream payload:
// `{"height", "hash", "parent_hash", "timestamp"}`.
type Head struct {
	// Height is the block number / slot / height.
	Height uint64
	// Hash is the block hash in the chain's native format (e.g. 0x... for EVM).
	Hash string
	// ParentHash is the hash of the parent block; used by the tip follower to
	// detect reorgs (see PROJECT_PLAN.md Stage 6).
	ParentHash string
	// Timestamp is the wall-clock time the block was produced.
	Timestamp time.Time
}

// MempoolEvent is emitted by ChainAdapter.SubscribeMempool for pending
// transactions observed in the node's mempool, optionally filtered to a set
// of own addresses.
type MempoolEvent struct {
	// TxHash is the pending transaction hash in the chain's native format.
	TxHash string
	// From is the sender address (when derivable; empty for some chains).
	From string
	// To is the recipient address (when derivable; empty for some chains).
	To string
	// Value is the transferred value in the chain's smallest unit; nil when
	// not a simple value transfer.
	Value *big.Int
	// Fee is the observed fee in the chain's smallest unit (gas price * gas
	// limit, priority fee, or fee rate).
	Fee *big.Int
	// SeenAt is when the event was observed.
	SeenAt time.Time
}

// Tx is the full transaction record returned by ChainAdapter.GetTx and the
// GET /v1/chains/:chain/tx/:hash endpoint:
// `{"tx_hash", "status", "block_height", "confirmations", "from", "to",
//
//	"value", "fee", "raw"}`.
type Tx struct {
	// TxHash is the transaction hash in the chain's native format.
	TxHash string
	// Status is the current lifecycle status of the tx.
	Status TxStatus
	// BlockHeight is the height of the block containing the tx, or 0 while
	// the tx is in the mempool (not yet confirmed).
	BlockHeight uint64
	// Confirmations is the number of confirmations accumulated since
	// BlockHeight.
	Confirmations uint64
	// From is the sender address.
	From string
	// To is the recipient address.
	To string
	// Value is the transferred value in the chain's smallest unit.
	Value *big.Int
	// Fee is the effective fee paid in the chain's smallest unit.
	Fee *big.Int
	// Raw is the raw signed transaction bytes as accepted by Broadcast.
	Raw []byte
}

// TxStatus is the lifecycle status of a transaction, as exposed by
// ChainAdapter.GetTxStatus and the GET /v1/chains/:chain/tx/:hash/status
// endpoint.
//
// The lifecycle is a state machine (see README §"Tx status lifecycle"):
//
//	broadcast -> mempool -> confirmed -> finalized
//
// with terminal error states `dropped`, `replaced`, `reorged_out`, and
// `failed`.
type TxStatus struct {
	// Status is the current lifecycle state.
	Status TxState
	// Confirmations is the number of confirmations accumulated (0 while in
	// mempool).
	Confirmations uint64
	// FinalizedAt is the wall-clock time the tx reached FinalityBlocks
	// confirmations and was marked finalized; nil otherwise.
	FinalizedAt *time.Time
}

// TxState is the discrete lifecycle state of a transaction.
type TxState string

const (
	// TxStateBroadcast: tx has been submitted to the node but not yet
	// observed in the mempool.
	TxStateBroadcast TxState = "broadcast"
	// TxStateMempool: tx is pending in the node's mempool.
	TxStateMempool TxState = "mempool"
	// TxStateConfirmed: tx is included in a block but has not yet reached
	// finality depth (FinalityBlocks).
	TxStateConfirmed TxState = "confirmed"
	// TxStateFinalized: tx has reached FinalityBlocks confirmations and is
	// considered final.
	TxStateFinalized TxState = "finalized"
	// TxStateDropped: tx was removed from the mempool without being mined
	// (e.g. expired, low fee).
	TxStateDropped TxState = "dropped"
	// TxStateReplaced: tx was replaced by a conflicting tx with the same
	// nonce and a higher fee (RBF / similar).
	TxStateReplaced TxState = "replaced"
	// TxStateReorgedOut: tx was included in a block that was later reorged
	// out; it may be re-broadcast.
	TxStateReorgedOut TxState = "reorged_out"
	// TxStateFailed: tx was included in a block but reverted / failed
	// execution.
	TxStateFailed TxState = "failed"
)
