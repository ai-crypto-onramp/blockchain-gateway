// Package memstore is an in-memory implementation of the store interfaces,
// used by unit tests and the e2e smoke harness so `go test ./...` passes
// without requiring Docker or a live Postgres. It is safe for concurrent
// use.
//
// Each interface is implemented by a dedicated struct to avoid method-name
// collisions across interfaces (e.g. Insert / Get / Upsert / Append). A
// composite Store bundles them for convenient wiring.
package memstore

import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ai-crypto-onramp/blockchain-gateway/internal/chain"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/store"
)

func key(chainID, txHash string) string { return chainID + "|" + txHash }

// BroadcastStore implements store.BroadcastStore in memory.
type BroadcastStore struct {
	mu         sync.Mutex
	broadcasts map[string]*store.Broadcast
}

// NewBroadcastStore returns an empty in-memory BroadcastStore.
func NewBroadcastStore() *BroadcastStore {
	return &BroadcastStore{broadcasts: make(map[string]*store.Broadcast)}
}

func (s *BroadcastStore) Insert(_ context.Context, b *store.Broadcast) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(b.ChainID, b.TxHash)
	if _, ok := s.broadcasts[k]; ok {
		return nil
	}
	c := *b
	if c.SubmittedAt.IsZero() {
		c.SubmittedAt = time.Now()
	}
	s.broadcasts[k] = &c
	return nil
}

func (s *BroadcastStore) GetByTxHash(_ context.Context, chainID, txHash string) (*store.Broadcast, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.broadcasts[key(chainID, txHash)]
	if !ok {
		return nil, &store.ErrNotFound{Chain: chainID, Key: txHash}
	}
	c := *b
	return &c, nil
}

func (s *BroadcastStore) Exists(_ context.Context, chainID, txHash string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.broadcasts[key(chainID, txHash)]
	return ok, nil
}

// ConfirmationStore implements store.ConfirmationStore in memory.
type ConfirmationStore struct {
	mu            sync.Mutex
	confirmations map[string]*store.Confirmation
}

// NewConfirmationStore returns an empty in-memory ConfirmationStore.
func NewConfirmationStore() *ConfirmationStore {
	return &ConfirmationStore{confirmations: make(map[string]*store.Confirmation)}
}

func (s *ConfirmationStore) Upsert(_ context.Context, c *store.Confirmation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(c.ChainID, c.TxHash)
	clone := *c
	if clone.FirstSeenAt.IsZero() {
		clone.FirstSeenAt = time.Now()
	}
	s.confirmations[k] = &clone
	return nil
}

func (s *ConfirmationStore) Get(_ context.Context, chainID, txHash string) (*store.Confirmation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.confirmations[key(chainID, txHash)]
	if !ok {
		return nil, &store.ErrNotFound{Chain: chainID, Key: txHash}
	}
	clone := *c
	return &clone, nil
}

func (s *ConfirmationStore) ListByStatus(_ context.Context, chainID string, status chain.Status) ([]*store.Confirmation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*store.Confirmation
	for _, c := range s.confirmations {
		if c.ChainID == chainID && c.Status == status {
			clone := *c
			out = append(out, &clone)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].TxHash < out[j].TxHash })
	return out, nil
}

func (s *ConfirmationStore) ListAboveHeight(_ context.Context, chainID string, height uint64) ([]*store.Confirmation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*store.Confirmation
	for _, c := range s.confirmations {
		if c.ChainID == chainID && c.BlockHeight > height {
			clone := *c
			out = append(out, &clone)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].TxHash < out[j].TxHash })
	return out, nil
}

func (s *ConfirmationStore) Transition(_ context.Context, chainID, txHash string, from, to chain.Status, mutator func(*store.Confirmation)) (*store.Confirmation, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(chainID, txHash)
	c, ok := s.confirmations[k]
	if !ok {
		return nil, false, &store.ErrNotFound{Chain: chainID, Key: txHash}
	}
	if c.Status != from {
		return nil, false, nil
	}
	if !from.CanTransitionTo(to) {
		return nil, false, fmt.Errorf("invalid transition %s -> %s", from, to)
	}
	clone := *c
	clone.Status = to
	if mutator != nil {
		mutator(&clone)
	}
	s.confirmations[k] = &clone
	return &clone, true, nil
}

// TipStore implements store.TipStore in memory.
type TipStore struct {
	mu   sync.Mutex
	tips map[string]*store.Tip
}

// NewTipStore returns an empty in-memory TipStore.
func NewTipStore() *TipStore {
	return &TipStore{tips: make(map[string]*store.Tip)}
}

func (s *TipStore) Upsert(_ context.Context, t *store.Tip) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	clone := *t
	if clone.UpdatedAt.IsZero() {
		clone.UpdatedAt = time.Now()
	}
	s.tips[t.ChainID] = &clone
	return nil
}

func (s *TipStore) Get(_ context.Context, chainID string) (*store.Tip, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tips[chainID]
	if !ok {
		return nil, &store.ErrNotFound{Chain: chainID, Key: "tip"}
	}
	clone := *t
	return &clone, nil
}

// FeeStore implements store.FeeStore in memory.
type FeeStore struct {
	mu    sync.Mutex
	rows  map[string][]*store.FeeEstimateRow
}

// NewFeeStore returns an empty in-memory FeeStore.
func NewFeeStore() *FeeStore {
	return &FeeStore{rows: make(map[string][]*store.FeeEstimateRow)}
}

func feeKey(chainID string, p chain.Priority) string {
	return chainID + "|" + string(p)
}

func (s *FeeStore) Insert(_ context.Context, r *store.FeeEstimateRow) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	clone := *r
	if clone.ComputedAt.IsZero() {
		clone.ComputedAt = time.Now()
	}
	k := feeKey(r.ChainID, r.Priority)
	s.rows[k] = append(s.rows[k], &clone)
	return nil
}

func (s *FeeStore) Latest(_ context.Context, chainID string, p chain.Priority) (*store.FeeEstimateRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows := s.rows[feeKey(chainID, p)]
	if len(rows) == 0 {
		return nil, &store.ErrNotFound{Chain: chainID, Key: string(p)}
	}
	clone := *rows[len(rows)-1]
	return &clone, nil
}

// ReorgStore implements store.ReorgStore in memory.
type ReorgStore struct {
	mu      sync.Mutex
	entries map[string][]*store.ReorgEvent
}

// NewReorgStore returns an empty in-memory ReorgStore.
func NewReorgStore() *ReorgStore {
	return &ReorgStore{entries: make(map[string][]*store.ReorgEvent)}
}

func (s *ReorgStore) Append(_ context.Context, e *store.ReorgEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	clone := *e
	if clone.DetectedAt.IsZero() {
		clone.DetectedAt = time.Now()
	}
	if clone.AffectedTxHashes == nil {
		clone.AffectedTxHashes = []string{}
	}
	s.entries[e.ChainID] = append(s.entries[e.ChainID], &clone)
	return nil
}

func (s *ReorgStore) List(_ context.Context, chainID string) ([]*store.ReorgEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	src := s.entries[chainID]
	out := make([]*store.ReorgEvent, len(src))
	for i, e := range src {
		clone := *e
		out[i] = &clone
	}
	return out, nil
}

// OutboxStore implements store.OutboxStore in memory.
type OutboxStore struct {
	mu       sync.Mutex
	entries  []*store.OutboxEntry
	seen     map[string]bool
	seq      int64
}

// NewOutboxStore returns an empty in-memory OutboxStore.
func NewOutboxStore() *OutboxStore {
	return &OutboxStore{seen: make(map[string]bool)}
}

func outboxKey(chainID, txHash string, status chain.Status, bh uint64) string {
	return strings.Join([]string{chainID, txHash, string(status), fmt.Sprintf("%d", bh)}, "|")
}

func (s *OutboxStore) Append(_ context.Context, e *store.OutboxEntry) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	dk := outboxKey(e.ChainID, e.TxHash, e.Status, e.BlockHeight)
	if s.seen[dk] {
		return false, nil
	}
	s.seq++
	clone := *e
	clone.ID = s.seq
	if clone.CreatedAt.IsZero() {
		clone.CreatedAt = time.Now()
	}
	s.entries = append(s.entries, &clone)
	s.seen[dk] = true
	return true, nil
}

func (s *OutboxStore) ListPending(_ context.Context, limit int) ([]*store.OutboxEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*store.OutboxEntry
	for _, e := range s.entries {
		if e.EmittedAt.IsZero() {
			clone := *e
			out = append(out, &clone)
			if len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func (s *OutboxStore) MarkEmitted(_ context.Context, id int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range s.entries {
		if e.ID == id {
			e.EmittedAt = time.Now()
			return nil
		}
	}
	return &store.ErrNotFound{Chain: "outbox", Key: fmt.Sprintf("%d", id)}
}

// Snapshot returns a copy of all outbox entries (test helper).
func (s *OutboxStore) Snapshot() []*store.OutboxEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*store.OutboxEntry, len(s.entries))
	for i, e := range s.entries {
		clone := *e
		out[i] = &clone
	}
	return out
}

// All is a composite of all in-memory stores for convenient wiring in tests
// and the e2e smoke harness.
type All struct {
	Broadcast    *BroadcastStore
	Confirmation *ConfirmationStore
	Tip          *TipStore
	Fee          *FeeStore
	Reorg        *ReorgStore
	Outbox       *OutboxStore
}

// NewAll returns a fully wired set of in-memory stores.
func NewAll() *All {
	return &All{
		Broadcast:    NewBroadcastStore(),
		Confirmation: NewConfirmationStore(),
		Tip:          NewTipStore(),
		Fee:          NewFeeStore(),
		Reorg:        NewReorgStore(),
		Outbox:       NewOutboxStore(),
	}
}

func bigSafe(b *big.Int) *big.Int {
	if b == nil {
		return nil
	}
	return new(big.Int).Set(b)
}

var _ = bigSafe