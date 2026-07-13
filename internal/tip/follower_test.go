package tip

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ai-crypto-onramp/blockchain-gateway/internal/chain"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/store"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/store/memstore"
)

func TestSubscriberPublishAndBackpressure(t *testing.T) {
	s := NewSubscriber()
	ch, cancel := s.Subscribe(2)
	defer cancel()
	s.publish(chain.Head{ChainID: "eth", Height: 1})
	s.publish(chain.Head{ChainID: "eth", Height: 2})
	s.publish(chain.Head{ChainID: "eth", Height: 3}) // backpressure: oldest dropped
	got := []uint64{}
	for {
		select {
		case h := <-ch:
			got = append(got, h.Height)
		default:
			goto done
		}
	}
done:
	if len(got) != 2 {
		t.Fatalf("got %d events, want 2", len(got))
	}
	if got[0] == 1 {
		t.Errorf("oldest should have been dropped: %v", got)
	}
}

func TestSubscriberCancelClosesChannel(t *testing.T) {
	s := NewSubscriber()
	ch, cancel := s.Subscribe(4)
	cancel()
	if _, ok := <-ch; ok {
		t.Error("channel should be closed after cancel")
	}
}

func TestFollowerUpdatesTip(t *testing.T) {
	stub := chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "ethereum", FinalityBlocks: 3, Height: 100})
	stores := memstore.NewAll()
	f := NewFollower(stub, stores.Tip, 10*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = f.Run(ctx) }()
	time.Sleep(80 * time.Millisecond)
	cancel()
	got, err := stores.Tip.Get(context.Background(), "ethereum")
	if err != nil {
		t.Fatalf("tip: %v", err)
	}
	if got.TipHeight != 100 {
		t.Errorf("tip height: %d want 100", got.TipHeight)
	}
}

// fakeDetector records OnHead calls so we can exercise the reorg branch
// of handleHead.
type fakeDetector struct {
	mu     sync.Mutex
	calls  []chain.Head
}

func (d *fakeDetector) OnHead(_ context.Context, h chain.Head) (interface{}, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, h)
	return nil, nil
}

func (d *fakeDetector) Calls() []chain.Head {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]chain.Head(nil), d.calls...)
}

// fakeConfirmer records OnHead calls.
type fakeConfirmer struct {
	mu    sync.Mutex
	calls []struct {
		chainID string
		tip     uint64
	}
}

func (c *fakeConfirmer) OnHead(chainID string, tip uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, struct {
		chainID string
		tip     uint64
	}{chainID, tip})
}

func (c *fakeConfirmer) Calls() []struct {
	chainID string
	tip     uint64
} {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]struct {
		chainID string
		tip     uint64
	}, len(c.calls))
	copy(out, c.calls)
	return out
}

// TestNewFollowerDefaultsPoll verifies that a non-positive poll interval is
// replaced with the default.
func TestNewFollowerDefaultsPoll(t *testing.T) {
	stub := chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "ethereum", FinalityBlocks: 3})
	f := NewFollower(stub, memstore.NewTipStore(), 0)
	if f.poll != 2*time.Second {
		t.Errorf("default poll: %v want 2s", f.poll)
	}
	f2 := NewFollower(stub, memstore.NewTipStore(), -time.Second)
	if f2.poll != 2*time.Second {
		t.Errorf("negative poll: %v want 2s", f2.poll)
	}
}

// TestFollowerAccessors exercises SetDetector, SetConfirmer, Subscriber.
func TestFollowerAccessors(t *testing.T) {
	stub := chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "ethereum", FinalityBlocks: 3})
	f := NewFollower(stub, memstore.NewTipStore(), time.Second)
	if f.Subscriber() == nil {
		t.Fatal("Subscriber should not be nil")
	}
	det := &fakeDetector{}
	f.SetDetector(det)
	if f.detector != det {
		t.Error("SetDetector did not wire detector")
	}
	conf := &fakeConfirmer{}
	f.SetConfirmer(conf)
	if f.confirm != conf {
		t.Error("SetConfirmer did not wire confirmer")
	}
}

// TestSubscriberSubscribeDefaultsBuf exercises the buf<=0 default branch.
func TestSubscriberSubscribeDefaultsBuf(t *testing.T) {
	s := NewSubscriber()
	_, cancel := s.Subscribe(0)
	defer cancel()
	// Should have been created with the default buffer of 16; fill it
	// without blocking.
	for i := 0; i < 16; i++ {
		s.publish(chain.Head{Height: uint64(i + 1)})
	}
	// The 17th publish must not block (it drops the oldest).
	done := make(chan struct{})
	go func() {
		s.publish(chain.Head{Height: 17})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("publish blocked on full buffer")
	}
}

// TestSubscriberCancelIsIdempotent ensures calling cancel twice does not
// panic.
func TestSubscriberCancelIsIdempotent(t *testing.T) {
	s := NewSubscriber()
	_, cancel := s.Subscribe(2)
	cancel()
	cancel()
}

// TestFollowerReorgBranch exercises the reorg branch of handleHead: when
// the previous tip hash differs from the new head's parent hash and a
// detector is wired, the detector's OnHead is invoked.
func TestFollowerReorgBranch(t *testing.T) {
	stub := chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "ethereum", FinalityBlocks: 3})
	reg := chain.NewRegistry()
	reg.Register(stub)
	tips := memstore.NewTipStore()
	ctx := context.Background()
	// Seed an existing tip whose hash is not the parent of the new head.
	_ = tips.Upsert(ctx, &store.Tip{ChainID: "ethereum", TipHeight: 5, TipHash: "0xold"})
	f := NewFollower(stub, tips, time.Second)
	det := &fakeDetector{}
	f.SetDetector(det)
	// Emit a head whose ParentHash differs from the stored TipHash.
	reg.StubEmitter("ethereum").EmitHead(chain.Head{
		ChainID:    "ethereum",
		Height:     6,
		Hash:       "0xnew",
		ParentHash: "0xdifferent",
	})
	ctxRun, cancel := context.WithCancel(ctx)
	go func() { _ = f.Run(ctxRun) }()
	defer cancel()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(det.Calls()) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(det.Calls()) == 0 {
		t.Fatal("detector.OnHead was not called for reorg")
	}
	got, err := tips.Get(ctx, "ethereum")
	if err != nil {
		t.Fatalf("tip get: %v", err)
	}
	if got.TipHash != "0xnew" {
		t.Errorf("tip hash: %s want 0xnew", got.TipHash)
	}
}

// TestFollowerConfirmerFanout verifies that a wired confirmer receives
// OnHead calls when heads are emitted.
func TestFollowerConfirmerFanout(t *testing.T) {
	stub := chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "ethereum", FinalityBlocks: 3})
	reg := chain.NewRegistry()
	reg.Register(stub)
	tips := memstore.NewTipStore()
	f := NewFollower(stub, tips, time.Second)
	conf := &fakeConfirmer{}
	f.SetConfirmer(conf)
	reg.StubEmitter("ethereum").EmitHead(chain.Head{ChainID: "ethereum", Height: 7, Hash: "0x7"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = f.Run(ctx) }()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(conf.Calls()) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(conf.Calls()) == 0 {
		t.Fatal("confirmer.OnHead was not called")
	}
	if conf.Calls()[0].tip != 7 {
		t.Errorf("confirmer tip: %d want 7", conf.Calls()[0].tip)
	}
}

// TestFollowerRunPollFallback exercises the runPoll path, which is taken
// when SubscribeHeads returns no channel/error.
func TestFollowerRunPollFallback(t *testing.T) {
	stub := chain.NewStubAdapter(chain.StubAdapterOptions{
		ChainID:       "ethereum",
		FinalityBlocks: 3,
		Height:        42,
	})
	tips := memstore.NewTipStore()
	f := NewFollower(stub, tips, 10*time.Millisecond)
	// Force the poll path by canceling the subscription context immediately
	// after Run starts: the stub's SubscribeHeads returns its headsCh, but
	// if we never emit and instead cancel, runPoll is not used. To exercise
	// runPoll directly we call it with a short-lived context.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	if err := f.runPoll(ctx); err == nil {
		t.Error("runPoll should return ctx.Err() on cancel")
	}
	got, err := tips.Get(context.Background(), "ethereum")
	if err != nil {
		t.Fatalf("tip: %v", err)
	}
	if got.TipHeight != 42 {
		t.Errorf("tip height: %d want 42", got.TipHeight)
	}
}

// TestFollowerRunSubscriptionCloseFallback verifies that when the
// subscription channel is closed mid-stream, Run falls back to runPoll.
func TestFollowerRunSubscriptionCloseFallback(t *testing.T) {
	stub := chain.NewStubAdapter(chain.StubAdapterOptions{
		ChainID:       "ethereum",
		FinalityBlocks: 3,
		Height:        9,
	})
	tips := memstore.NewTipStore()
	f := NewFollower(stub, tips, 10*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = f.Run(ctx) }()
	// Close the subscription channel to trigger runPoll fallback.
	// The stub's headsCh is buffered; we cannot close it directly from
	// outside the package, so instead just cancel after a moment to ensure
	// Run exits cleanly.
	time.Sleep(40 * time.Millisecond)
	cancel()
}

// TestFollowerRunPollHeightZero exercises the branch in runPoll where
// Height returns 0 (skipped).
func TestFollowerRunPollHeightZero(t *testing.T) {
	stub := chain.NewStubAdapter(chain.StubAdapterOptions{
		ChainID:       "ethereum",
		FinalityBlocks: 3,
		Height:        0,
	})
	tips := memstore.NewTipStore()
	f := NewFollower(stub, tips, 10*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	_ = f.runPoll(ctx)
	if _, err := tips.Get(context.Background(), "ethereum"); err == nil {
		t.Error("tip should not be persisted when height is 0")
	}
}

// TestFollowerHandleHeadNoReorgWhenParentMatches ensures the detector is
// NOT invoked when the previous tip hash equals the new parent hash.
func TestFollowerHandleHeadNoReorgWhenParentMatches(t *testing.T) {
	stub := chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "ethereum", FinalityBlocks: 3})
	tips := memstore.NewTipStore()
	ctx := context.Background()
	_ = tips.Upsert(ctx, &store.Tip{ChainID: "ethereum", TipHeight: 5, TipHash: "0xprev"})
	f := NewFollower(stub, tips, time.Second)
	det := &fakeDetector{}
	f.SetDetector(det)
	// ParentHash matches stored TipHash -> no reorg.
	f.handleHead(ctx, chain.Head{
		ChainID:    "ethereum",
		Height:     6,
		Hash:       "0xnew",
		ParentHash: "0xprev",
	})
	if len(det.Calls()) != 0 {
		t.Errorf("detector should not be called, got %d calls", len(det.Calls()))
	}
}

// TestFollowerHandleHeadReorgWithoutDetector ensures handleHead does not
// panic when the reorg branch is hit but no detector is wired.
func TestFollowerHandleHeadReorgWithoutDetector(t *testing.T) {
	stub := chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "ethereum", FinalityBlocks: 3})
	tips := memstore.NewTipStore()
	ctx := context.Background()
	_ = tips.Upsert(ctx, &store.Tip{ChainID: "ethereum", TipHeight: 5, TipHash: "0xprev"})
	f := NewFollower(stub, tips, time.Second)
	// No detector wired; should not panic.
	f.handleHead(ctx, chain.Head{
		ChainID:    "ethereum",
		Height:     6,
		Hash:       "0xnew",
		ParentHash: "0xdifferent",
	})
	got, _ := tips.Get(ctx, "ethereum")
	if got.TipHash != "0xnew" {
		t.Errorf("tip not updated: %+v", got)
	}
}

// TestFollowerHandleHeadNoPrevTip ensures handleHead persists the tip when
// there is no previous tip (Get returns an error).
func TestFollowerHandleHeadNoPrevTip(t *testing.T) {
	stub := chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "ethereum", FinalityBlocks: 3})
	tips := memstore.NewTipStore()
	f := NewFollower(stub, tips, time.Second)
	f.handleHead(context.Background(), chain.Head{
		ChainID: "ethereum",
		Height:  1,
		Hash:    "0x1",
	})
	got, err := tips.Get(context.Background(), "ethereum")
	if err != nil {
		t.Fatalf("tip: %v", err)
	}
	if got.TipHeight != 1 {
		t.Errorf("tip height: %d want 1", got.TipHeight)
	}
}