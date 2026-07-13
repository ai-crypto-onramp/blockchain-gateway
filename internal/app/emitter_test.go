package app

import (
	"context"
	"testing"
	"time"

	"github.com/ai-crypto-onramp/blockchain-gateway/internal/audit"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/chain"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/confirmation"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/eventbus"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/mempool"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/reorg"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/store/memstore"
)

func TestBusEmitterEmit(t *testing.T) {
	stores := memstore.NewAll()
	bus := eventbus.NewBus(stores.Outbox, eventbus.NopPublisher{}, "")
	log := audit.New(bus, 4)
	em := &busEmitter{bus: bus, audit: log}
	if err := em.Emit(context.Background(), confirmation.Event{Type: "tx.confirmed", ChainID: "ethereum", TxHash: "0x1", Status: chain.StatusConfirmed, BlockHeight: 100}); err != nil {
		t.Fatalf("emit: %v", err)
	}
	if err := em.EmitMempool(context.Background(), mempool.Event{Type: "tx.dropped", ChainID: "ethereum", TxHash: "0x1", Status: chain.StatusDropped}); err != nil {
		t.Fatalf("emit mempool: %v", err)
	}
	if err := em.EmitReorg(context.Background(), reorg.Event{Type: "tx.reorged", ChainID: "ethereum", Affected: []string{"0x1", "0x2"}, CommonAncestor: 50}); err != nil {
		t.Fatalf("emit reorg: %v", err)
	}
}

func TestDetectorAdapterOnHead(t *testing.T) {
	stores := memstore.NewAll()
	em := &busEmitter{bus: eventbus.NewBus(stores.Outbox, eventbus.NopPublisher{}, ""), audit: audit.New(nil, 4)}
	d := reorg.NewDetector(stores.Tip, stores.Reorg, stores.Confirmation, em)
	da := &detectorAdapter{det: d}
	res, err := da.OnHead(context.Background(), chain.Head{ChainID: "ethereum", Height: 10, Hash: "0x10"})
	if err != nil {
		t.Fatalf("onhead: %v", err)
	}
	if res == nil {
		t.Fatal("nil result")
	}
}

func TestBuildAdapterSelection(t *testing.T) {
	// EVM
	a := buildAdapter(chain.ChainConfig{ChainID: "ethereum", RPCURLs: []string{"http://x"}, FinalityBlocks: 64})
	if _, ok := a.(*chain.EVMAdapter); !ok {
		t.Errorf("expected EVMAdapter for ethereum")
	}
	// Solana
	a = buildAdapter(chain.ChainConfig{ChainID: "solana", RPCURLs: []string{"http://x"}, FinalityBlocks: 1})
	if _, ok := a.(*chain.SolanaAdapter); !ok {
		t.Errorf("expected SolanaAdapter for solana")
	}
	// Bitcoin
	a = buildAdapter(chain.ChainConfig{ChainID: "bitcoin", RPCURLs: []string{"http://x"}, FinalityBlocks: 6})
	if _, ok := a.(*chain.BitcoinAdapter); !ok {
		t.Errorf("expected BitcoinAdapter for bitcoin")
	}
	// Unknown -> stub
	a = buildAdapter(chain.ChainConfig{ChainID: "cardano", FinalityBlocks: 10})
	if a.ChainID() != "cardano" {
		t.Errorf("expected stub for cardano")
	}
}

func TestEnvOrWithSet(t *testing.T) {
	t.Setenv("MY_TEST_VAR", "hello")
	if envOr("MY_TEST_VAR", "def") != "hello" {
		t.Error("envOr should read set var")
	}
}

func TestEnvDurWithSet(t *testing.T) {
	t.Setenv("MY_DUR", "5s")
	if envDur("MY_DUR", time.Second) != 5*time.Second {
		t.Error("envDur should parse set var")
	}
}

func TestEnvIntWithSet(t *testing.T) {
	t.Setenv("MY_INT", "42")
	if envInt("MY_INT", 0) != 42 {
		t.Error("envInt should parse set var")
	}
}

func TestEnvIntMalformed(t *testing.T) {
	t.Setenv("MY_BAD_INT", "notanumber")
	if envInt("MY_BAD_INT", 9) != 9 {
		t.Error("envInt should use default on malformed")
	}
}

func TestEnvDurMalformed(t *testing.T) {
	t.Setenv("MY_BAD_DUR", "notaduration")
	if envDur("MY_BAD_DUR", time.Second) != time.Second {
		t.Error("envDur should use default on malformed")
	}
}