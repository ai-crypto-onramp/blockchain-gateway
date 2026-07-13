// Package tip follows the chain tip per chain: it subscribes to adapter
// heads (with polling fallback), persists the tip to TipStore, publishes
// heads to internal consumers (confirmation workers, reorg detector, fee
// estimator, WS stream), and exposes the live tip to REST/WS handlers.
package tip

import (
	"context"
	"sync"
	"time"

	"github.com/ai-crypto-onramp/blockchain-gateway/internal/chain"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/store"
)

// Follower follows one chain's tip.
type Follower struct {
	chainID  string
	adapter  chain.ChainAdapter
	tips     store.TipStore
	detector Detector
	confirm  Confirmer
	subs     *Subscriber
	poll     time.Duration
}

// Detector is the reorg surface used by the follower.
type Detector interface {
	OnHead(ctx context.Context, h chain.Head) (interface{}, error)
}

// Confirmer is the confirmation surface used by the follower.
type Confirmer interface {
	OnHead(chainID string, tip uint64)
}

// NewFollower returns a Follower for the given chain.
func NewFollower(adapter chain.ChainAdapter, tips store.TipStore, poll time.Duration) *Follower {
	if poll <= 0 {
		poll = 2 * time.Second
	}
	return &Follower{
		chainID: adapter.ChainID(),
		adapter: adapter,
		tips:    tips,
		subs:    NewSubscriber(),
		poll:    poll,
	}
}

// SetDetector wires a reorg detector.
func (f *Follower) SetDetector(d Detector) { f.detector = d }

// SetConfirmer wires a confirmation worker pool.
func (f *Follower) SetConfirmer(c Confirmer) { f.confirm = c }

// Subscriber returns the head subscriber hub.
func (f *Follower) Subscriber() *Subscriber { return f.subs }

// Run blocks until ctx is canceled, subscribing to adapter heads and
// falling back to polling if the subscription is unavailable.
func (f *Follower) Run(ctx context.Context) error {
	sub, cancel, err := f.adapter.SubscribeHeads(ctx)
	if err != nil || sub == nil {
		return f.runPoll(ctx)
	}
	defer cancel()
	pollTicker := time.NewTicker(f.poll)
	defer pollTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case h, ok := <-sub:
			if !ok {
				return f.runPoll(ctx)
			}
			f.handleHead(ctx, h)
		case <-pollTicker.C:
			// Fallback poll in case WS goes quiet.
			h, err := f.adapter.Height(ctx)
			if err != nil || h == 0 {
				continue
			}
			f.handleHead(ctx, chain.Head{ChainID: f.chainID, Height: h, Timestamp: time.Now()})
		}
	}
}

func (f *Follower) runPoll(ctx context.Context) error {
	ticker := time.NewTicker(f.poll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			h, err := f.adapter.Height(ctx)
			if err != nil || h == 0 {
				continue
			}
			f.handleHead(ctx, chain.Head{ChainID: f.chainID, Height: h, Timestamp: time.Now()})
		}
	}
}

func (f *Follower) handleHead(ctx context.Context, h chain.Head) {
	// Persist tip.
	prev, _ := f.tips.Get(ctx, h.ChainID)
	if prev != nil && prev.TipHash != "" && h.ParentHash != "" && prev.TipHash != h.ParentHash {
		// Reorg: defer to detector if wired.
		if f.detector != nil {
			_, _ = f.detector.OnHead(ctx, h)
		}
	}
	_ = f.tips.Upsert(ctx, &store.Tip{
		ChainID:   h.ChainID,
		TipHeight: h.Height,
		TipHash:   h.Hash,
		UpdatedAt: time.Now(),
	})
	// Fan out to subscribers + confirmer.
	f.subs.publish(h)
	if f.confirm != nil {
		f.confirm.OnHead(h.ChainID, h.Height)
	}
}

// Subscriber is a fan-out hub for head events. Subscribers are buffered
// channels; a slow subscriber does not block the follower (its oldest event
// is dropped).
type Subscriber struct {
	mu     sync.Mutex
	nextID int
	subs   map[int]chan chain.Head
}

// NewSubscriber returns an empty hub.
func NewSubscriber() *Subscriber {
	return &Subscriber{subs: make(map[int]chan chain.Head)}
}

// Subscribe returns a buffered head channel + cancel func.
func (s *Subscriber) Subscribe(buf int) (<-chan chain.Head, func()) {
	if buf <= 0 {
		buf = 16
	}
	s.mu.Lock()
	s.nextID++
	id := s.nextID
	ch := make(chan chain.Head, buf)
	s.subs[id] = ch
	s.mu.Unlock()
	cancel := func() {
		s.mu.Lock()
		if c, ok := s.subs[id]; ok {
			delete(s.subs, id)
			close(c)
		}
		s.mu.Unlock()
	}
	return ch, cancel
}

func (s *Subscriber) publish(h chain.Head) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ch := range s.subs {
		select {
		case ch <- h:
		default:
			// Backpressure: drop the oldest event to make room.
			select {
			case <-ch:
			default:
			}
			select {
			case ch <- h:
			default:
			}
		}
	}
}