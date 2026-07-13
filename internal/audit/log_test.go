package audit

import (
	"context"
	"testing"

	"github.com/ai-crypto-onramp/blockchain-gateway/internal/chain"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/eventbus"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/store/memstore"
)

func TestLogRecordAndRecent(t *testing.T) {
	outbox := memstore.NewOutboxStore()
	bus := eventbus.NewBus(outbox, eventbus.NopPublisher{}, "")
	log := New(bus, 4)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if err := log.Record(ctx, eventbus.Event{Type: "tx.confirmed", ChainID: "ethereum", TxHash: "0x1", Status: chain.StatusConfirmed, BlockHeight: uint64(100 + i)}); err != nil {
			t.Fatalf("record: %v", err)
		}
	}
	recent := log.Recent()
	if len(recent) != 4 {
		t.Errorf("recent: %d want 4 (ring cap)", len(recent))
	}
}