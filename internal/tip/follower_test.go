package tip

import (
	"context"
	"testing"
	"time"

	"github.com/ai-crypto-onramp/blockchain-gateway/internal/chain"
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