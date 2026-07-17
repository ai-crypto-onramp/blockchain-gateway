// Package store defines storage interfaces for the Blockchain Gateway.
// The package exposes interfaces (BroadcastStore, ConfirmationStore,
// TipStore, FeeStore, ReorgStore, OutboxStore) so the service can run
// against either a real PostgreSQL backend (internal/store/postgres) or an
// in-memory mock (used by tests and the e2e smoke harness).
package store

import (
	"context"
	"math/big"
	"time"

	"github.com/ai-crypto-onramp/blockchain-gateway/internal/chain"
	"github.com/google/uuid"
)

// Broadcast is the persisted record of one broadcast attempt.
type Broadcast struct {
	ID          uuid.UUID `json:"id"`
	ChainID     string    `json:"chain_id"`
	TxHash      string    `json:"tx_hash"`
	SignedTx    []byte    `json:"signed_tx"`
	FromAddr    string    `json:"from_addr"`
	ToAddr      string    `json:"to_addr"`
	Value       *big.Int  `json:"value"`
	Nonce       uint64    `json:"nonce"`
	SubmittedAt time.Time `json:"submitted_at"`
	SubmittedBy string    `json:"submitted_by"`
}

// BroadcastStore persists broadcast attempts and enforces idempotency on
// (chain_id, tx_hash).
type BroadcastStore interface {
	Insert(ctx context.Context, b *Broadcast) error
	GetByTxHash(ctx context.Context, chainID, txHash string) (*Broadcast, error)
	Exists(ctx context.Context, chainID, txHash string) (bool, error)
}

// Confirmation is the persisted confirmation status of a broadcast.
type Confirmation struct {
	ID            uuid.UUID     `json:"id"`
	ChainID       string        `json:"chain_id"`
	TxHash        string        `json:"tx_hash"`
	Status        chain.Status  `json:"status"`
	BlockHeight   uint64        `json:"block_height"`
	BlockHash     string        `json:"block_hash"`
	Confirmations uint64        `json:"confirmations"`
	FirstSeenAt   time.Time     `json:"first_seen_at"`
	ConfirmedAt   time.Time     `json:"confirmed_at"`
	FinalizedAt   time.Time     `json:"finalized_at"`
}

// ConfirmationStore persists and updates confirmation state.
type ConfirmationStore interface {
	Upsert(ctx context.Context, c *Confirmation) error
	Get(ctx context.Context, chainID, txHash string) (*Confirmation, error)
	ListByStatus(ctx context.Context, chainID string, status chain.Status) ([]*Confirmation, error)
	ListAboveHeight(ctx context.Context, chainID string, height uint64) ([]*Confirmation, error)
	Transition(ctx context.Context, chainID, txHash string, from chain.Status, to chain.Status, mutator func(*Confirmation)) (*Confirmation, bool, error)
}

// Tip is the persisted chain tip.
type Tip struct {
	ID              uuid.UUID `json:"id"`
	ChainID         string    `json:"chain_id"`
	TipHeight       uint64    `json:"tip_height"`
	TipHash         string    `json:"tip_hash"`
	FinalizedHeight uint64    `json:"finalized_height"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// TipStore persists the current tip per chain.
type TipStore interface {
	Upsert(ctx context.Context, t *Tip) error
	Get(ctx context.Context, chainID string) (*Tip, error)
}

// FeeEstimateRow is the persisted fee estimate sample.
type FeeEstimateRow struct {
	ID                   uuid.UUID     `json:"id"`
	ChainID              string        `json:"chain_id"`
	Priority             chain.Priority `json:"priority"`
	GasLimit             uint64        `json:"gas_limit"`
	MaxFeePerGas         *big.Int      `json:"max_fee_per_gas"`
	MaxPriorityFeePerGas *big.Int      `json:"max_priority_fee_per_gas"`
	GasPrice             *big.Int      `json:"gas_price"`
	TotalFee             *big.Int      `json:"total_fee"`
	SampleCount          int           `json:"sample_count"`
	ComputedAt           time.Time     `json:"computed_at"`
	Strategy             string        `json:"strategy"`
}

// FeeStore persists fee estimate samples for trend analysis.
type FeeStore interface {
	Insert(ctx context.Context, r *FeeEstimateRow) error
	Latest(ctx context.Context, chainID string, priority chain.Priority) (*FeeEstimateRow, error)
}

// ReorgEvent is an append-only audit record of a chain reorg.
type ReorgEvent struct {
	ID                   uuid.UUID `json:"id"`
	ChainID              string    `json:"chain_id"`
	DetectedAt           time.Time `json:"detected_at"`
	OldTipHash           string    `json:"old_tip_hash"`
	NewTipHash           string    `json:"new_tip_hash"`
	CommonAncestorHeight uint64    `json:"common_ancestor_height"`
	AffectedTxHashes     []string  `json:"affected_tx_hashes"`
}

// ReorgStore appends and reads reorg events.
type ReorgStore interface {
	Append(ctx context.Context, e *ReorgEvent) error
	List(ctx context.Context, chainID string) ([]*ReorgEvent, error)
}

// OutboxEntry is a deduped outbound event awaiting emission.
type OutboxEntry struct {
	ID          uuid.UUID    `json:"id"`
	ChainID     string       `json:"chain_id"`
	TxHash      string       `json:"tx_hash"`
	Status      chain.Status `json:"status"`
	BlockHeight uint64       `json:"block_height"`
	EventType   string       `json:"event_type"`
	Payload     []byte       `json:"payload"`
	CreatedAt   time.Time    `json:"created_at"`
	EmittedAt   time.Time    `json:"emitted_at"`
}

// OutboxStore persists events for at-least-once, deduped emission.
type OutboxStore interface {
	// Append inserts an entry unless a dedup key (chain_id, tx_hash,
	// status, block_height) already exists. Returns true if inserted.
	Append(ctx context.Context, e *OutboxEntry) (bool, error)
	ListPending(ctx context.Context, limit int) ([]*OutboxEntry, error)
	MarkEmitted(ctx context.Context, id uuid.UUID) error
}

// ErrNotFound is returned by stores when a row lookup misses.
type ErrNotFound struct{ Chain, Key string }

func (e *ErrNotFound) Error() string { return "not found: " + e.Chain + "/" + e.Key }