// Package chain defines the chain-agnostic core of the Blockchain Gateway:
// the ChainAdapter interface, per-chain configuration, the adapter registry,
// and the domain types shared across broadcast, fee, confirmation, reorg,
// mempool, and tip-following subsystems.
//
// The interface intentionally mirrors the API surface documented in the
// project README so every chain implementation (EVM, Solana, Bitcoin, ...)
// exposes identical behavior to the rest of the gateway.
package chain

import (
	"errors"
	"math/big"
	"time"
)

// Status is the lifecycle state of a broadcast transaction.
//
// The happy path is broadcast -> mempool -> confirmed -> finalized.
// The remaining values are terminal error states.
type Status string

const (
	// StatusBroadcast indicates the tx has been submitted to the mempool.
	StatusBroadcast Status = "broadcast"
	// StatusMempool indicates the tx is pending in the mempool.
	StatusMempool Status = "mempool"
	// StatusConfirmed indicates the tx is included in a block but not yet
	// final.
	StatusConfirmed Status = "confirmed"
	// StatusFinalized indicates the tx is finalized per the chain finality
	// gadget.
	StatusFinalized Status = "finalized"
	// StatusDropped indicates the tx was dropped from the mempool without
	// inclusion.
	StatusDropped Status = "dropped"
	// StatusReplaced indicates the tx was replaced by a higher-fee tx.
	StatusReplaced Status = "replaced"
	// StatusReorgedOut indicates the tx's block was reorged out.
	StatusReorgedOut Status = "reorged_out"
	// StatusFailed indicates the tx was included but reverted.
	StatusFailed Status = "failed"
)

// IsTerminal reports whether s is a terminal lifecycle state.
func (s Status) IsTerminal() bool {
	switch s {
	case StatusFinalized, StatusDropped, StatusReplaced, StatusFailed:
		return true
	}
	return false
}

// CanTransitionTo reports whether a transition from s to next is permitted by
// the lifecycle state machine. ReorgedOut is allowed to return to Confirmed
// or transition to Failed/Dropped.
func (s Status) CanTransitionTo(next Status) bool {
	if s == next {
		return true
	}
	switch s {
	case StatusBroadcast:
		return next == StatusMempool || next == StatusDropped ||
			next == StatusFailed || next == StatusConfirmed
	case StatusMempool:
		return next == StatusConfirmed || next == StatusDropped ||
			next == StatusReplaced || next == StatusFailed
	case StatusConfirmed:
		return next == StatusFinalized || next == StatusReorgedOut ||
			next == StatusFailed
	case StatusReorgedOut:
		return next == StatusConfirmed || next == StatusFailed ||
			next == StatusDropped || next == StatusMempool
	case StatusFinalized, StatusDropped, StatusReplaced, StatusFailed:
		return false
	}
	return false
}

// Priority is a fee priority tier.
type Priority string

const (
	PriorityLow      Priority = "low"
	PriorityStandard Priority = "standard"
	PriorityHigh     Priority = "high"
)

// Head represents a new chain head emitted by SubscribeHeads.
type Head struct {
	ChainID    string    `json:"chain_id"`
	Height     uint64    `json:"height"`
	Hash       string    `json:"hash"`
	ParentHash string    `json:"parent_hash"`
	Timestamp  time.Time `json:"timestamp"`
}

// MempoolEvent represents an entry/exit event for a transaction in the
// mempool. Kind is "enter" or "exit".
type MempoolEvent struct {
	ChainID string    `json:"chain_id"`
	TxHash  string    `json:"tx_hash"`
	Kind    string    `json:"kind"`
	SeenAt  time.Time `json:"seen_at"`
}

// Tx is the chain-agnostic representation of a transaction.
type Tx struct {
	ChainID     string   `json:"chain_id"`
	Hash        string   `json:"hash"`
	From        string   `json:"from"`
	To          string   `json:"to"`
	Value       *big.Int `json:"value"`
	Fee         *big.Int `json:"fee"`
	Nonce       uint64   `json:"nonce"`
	BlockHeight uint64   `json:"block_height"`
	BlockHash   string   `json:"block_hash"`
	Status      Status   `json:"status"`
	Raw         []byte   `json:"-"`
}

// TxStatus is the confirmation status snapshot for a tx.
type TxStatus struct {
	ChainID       string    `json:"chain_id"`
	TxHash        string    `json:"tx_hash"`
	Status        Status    `json:"status"`
	Confirmations uint64    `json:"confirmations"`
	BlockHeight   uint64    `json:"block_height"`
	BlockHash     string    `json:"block_hash"`
	FinalizedAt   time.Time `json:"finalized_at,omitempty"`
}

// FeeEstimateReq is the input to EstimateFee.
type FeeEstimateReq struct {
	To       string   `json:"to"`
	Amount   *big.Int `json:"amount"`
	Priority Priority `json:"priority"`
}

// FeeEstimate is the output of EstimateFee. EVM chains populate
// MaxFeePerGas/MaxPriorityFeePerGas; legacy and non-EVM chains populate
// GasPrice. TotalFee is an estimate of the total fee for the default gas
// limit.
type FeeEstimate struct {
	ChainID              string   `json:"chain_id"`
	Priority             Priority `json:"priority"`
	GasLimit             uint64   `json:"gas_limit"`
	MaxFeePerGas         *big.Int `json:"max_fee_per_gas"`
	MaxPriorityFeePerGas *big.Int `json:"max_priority_fee_per_gas"`
	GasPrice             *big.Int `json:"gas_price"`
	TotalFee             *big.Int `json:"total_fee"`
	Strategy             string   `json:"strategy"`
}

// ChainConfig is the per-chain configuration record. It is populated from
// environment variables (or a YAML file in future revisions) and used to
// construct adapters in the registry.
type ChainConfig struct {
	ChainID        string   `json:"chain_id"`
	RPCURLs        []string `json:"rpc_urls"`
	WSURLs         []string `json:"ws_urls"`
	FinalityBlocks uint64   `json:"finality_blocks"`
	GasStrategy    string   `json:"gas_strategy"`
}

// ErrUnknownChain is returned by the registry when no adapter is registered
// for the requested chain id.
var ErrUnknownChain = errors.New("unknown chain")

// ErrTxNotFound is returned by adapters when a transaction lookup misses.
var ErrTxNotFound = errors.New("transaction not found")

// ErrInsufficientFunds is returned when a sender cannot cover gas.
var ErrInsufficientFunds = errors.New("insufficient funds for gas")