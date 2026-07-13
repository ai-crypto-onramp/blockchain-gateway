// Package confirmation tracks confirmations for each broadcast up to the
// chain's FinalityBlocks. It implements the broadcast -> mempool ->
// confirmed -> finalized state machine with sticky worker assignment so
// each (chain, tx_hash) is updated by at most one worker.
package confirmation

import (
	"context"
	"hash/fnv"
	"sort"
	"sync"
	"time"

	"github.com/ai-crypto-onramp/blockchain-gateway/internal/chain"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/store"
)

// WorkerPool runs a fixed number of sticky workers. Stickiness is achieved
// by hashing (chain, tx_hash) to a worker index; each head is fanned out
// to the worker owning each tracked tx.
type WorkerPool struct {
	workers   int
	queue     []chan job
	wg        sync.WaitGroup
	store     store.ConfirmationStore
	adapter   chain.ChainAdapter
	finality  uint64
	mu        sync.Mutex
	tracked   map[string]tracked // key = chain|txhash
	emitter   Emitter
}

type tracked struct {
	chainID string
	txHash  string
}

type job struct {
	chainID string
	txHash  string
	tip     uint64
}

// Emitter is the subset of the event bus used by the confirmation worker
// to publish status transitions.
type Emitter interface {
	Emit(ctx context.Context, e Event) error
}

// Event is a confirmation lifecycle event.
type Event struct {
	Type         string      `json:"type"` // tx.confirmed, tx.finalized, ...
	ChainID      string      `json:"chain_id"`
	TxHash       string      `json:"tx_hash"`
	Status       chain.Status `json:"status"`
	PrevStatus   chain.Status `json:"prev_status"`
	BlockHeight  uint64      `json:"block_height"`
	Confirmations uint64     `json:"confirmations"`
	FinalizedAt  time.Time   `json:"finalized_at,omitempty"`
}

// NewWorkerPool returns a pool with n workers. n must be > 0.
func NewWorkerPool(n int, s store.ConfirmationStore, a chain.ChainAdapter, emitter Emitter) *WorkerPool {
	if n <= 0 {
		n = 4
	}
	p := &WorkerPool{
		workers:  n,
		queue:    make([]chan job, n),
		store:    s,
		adapter:  a,
		finality: a.FinalityBlocks(),
		tracked:  make(map[string]tracked),
		emitter:  emitter,
	}
	for i := 0; i < n; i++ {
		p.queue[i] = make(chan job, 64)
		p.wg.Add(1)
		go p.worker(i)
	}
	return p
}

// workerIndex returns the sticky worker index for (chain, txHash).
func (p *WorkerPool) workerIndex(chainID, txHash string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(chainID))
	_, _ = h.Write([]byte(txHash))
	return int(h.Sum32() % uint32(p.workers))
}

func (p *WorkerPool) worker(i int) {
	defer p.wg.Done()
	for j := range p.queue[i] {
		p.process(j)
	}
}

// Track registers a tx for confirmation tracking. Idempotent.
func (p *WorkerPool) Track(chainID, txHash string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	k := key(chainID, txHash)
	if _, ok := p.tracked[k]; ok {
		return
	}
	p.tracked[k] = tracked{chainID: chainID, txHash: txHash}
}

// Tracked returns the list of tracked (chain, txHash) pairs (test helper).
func (p *WorkerPool) Tracked() []tracked {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]tracked, 0, len(p.tracked))
	for _, t := range p.tracked {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].txHash < out[j].txHash })
	return out
}

// OnHead fans a new head out to the sticky worker for each tracked tx on
// the given chain. Non-blocking; if a worker queue is full the head is
// dropped (the next poll/WS head will catch up).
func (p *WorkerPool) OnHead(chainID string, tip uint64) {
	p.mu.Lock()
	tracked := make([]tracked, 0, len(p.tracked))
	for _, t := range p.tracked {
		if t.chainID == chainID {
			tracked = append(tracked, t)
		}
	}
	p.mu.Unlock()
	for _, t := range tracked {
		idx := p.workerIndex(t.chainID, t.txHash)
		select {
		case p.queue[idx] <- job{chainID: t.chainID, txHash: t.txHash, tip: tip}:
		default:
		}
	}
}

// process is the per-tx update logic run on the sticky worker.
func (p *WorkerPool) process(j job) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := p.store.Get(ctx, j.chainID, j.txHash)
	if err != nil {
		return
	}
	if c.Status.IsTerminal() {
		return
	}
	if c.BlockHeight == 0 {
		// Still in mempool; ask the adapter for inclusion.
		status, err := p.adapter.GetTxStatus(ctx, j.txHash)
		if err != nil || status == nil || status.BlockHeight == 0 {
			return
		}
		c.BlockHeight = status.BlockHeight
		c.BlockHash = status.BlockHash
		c.Confirmations = 1
		if c.Status == chain.StatusMempool || c.Status == chain.StatusBroadcast {
			if _, ok, _ := p.store.Transition(ctx, j.chainID, j.txHash, c.Status, chain.StatusConfirmed, func(upd *store.Confirmation) {
				upd.BlockHeight = c.BlockHeight
				upd.BlockHash = c.BlockHash
				upd.Confirmations = c.Confirmations
				upd.ConfirmedAt = time.Now()
			}); ok {
				p.emit(ctx, Event{Type: "tx.confirmed", ChainID: j.chainID, TxHash: j.txHash, Status: chain.StatusConfirmed, PrevStatus: c.Status, BlockHeight: c.BlockHeight, Confirmations: c.Confirmations})
			}
			return
		}
	}
	// Update confirmations from tip.
	confs := j.tip - c.BlockHeight + 1
	if confs > c.Confirmations {
		c.Confirmations = confs
		_ = p.store.Upsert(ctx, c)
	}
	if c.Status == chain.StatusConfirmed && c.Confirmations >= p.finality {
		if _, ok, _ := p.store.Transition(ctx, j.chainID, j.txHash, chain.StatusConfirmed, chain.StatusFinalized, func(upd *store.Confirmation) {
			upd.Confirmations = c.Confirmations
			upd.FinalizedAt = time.Now()
		}); ok {
			p.emit(ctx, Event{Type: "tx.finalized", ChainID: j.chainID, TxHash: j.txHash, Status: chain.StatusFinalized, PrevStatus: chain.StatusConfirmed, BlockHeight: c.BlockHeight, Confirmations: c.Confirmations, FinalizedAt: time.Now()})
		}
	}
}

func (p *WorkerPool) emit(ctx context.Context, e Event) {
	if p.emitter == nil {
		return
	}
	_ = p.emitter.Emit(ctx, e)
}

// Stop shuts down all workers and blocks until they finish.
func (p *WorkerPool) Stop() {
	for i := range p.queue {
		close(p.queue[i])
	}
	p.wg.Wait()
}

func key(chainID, txHash string) string { return chainID + "|" + txHash }