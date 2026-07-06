package chain

import (
	"math/big"
	"testing"
)

func TestLoadConfigs_FullFourChains(t *testing.T) {
	t.Setenv("CHAINS_SUPPORTED", "ethereum,polygon,solana,bitcoin")
	t.Setenv("RPC_URLS_ETHEREUM", "https://eth.example.com,https://eth2.example.com")
	t.Setenv("WS_URLS_ETHEREUM", "wss://eth.example.com")
	t.Setenv("FINALITY_BLOCKS_ETHEREUM", "64")
	t.Setenv("GAS_STRATEGY_ETHEREUM", "eip1559_dynamic")

	t.Setenv("RPC_URLS_POLYGON", "https://poly.example.com")
	t.Setenv("FINALITY_BLOCKS_POLYGON", "256")
	// no per-chain gas strategy -> falls back to default

	t.Setenv("RPC_URLS_SOLANA", "https://sol.example.com")
	t.Setenv("WS_URLS_SOLANA", "wss://sol.example.com")
	t.Setenv("FINALITY_BLOCKS_SOLANA", "1")
	t.Setenv("GAS_STRATEGY_SOLANA", "solana_priority_fee")

	t.Setenv("RPC_URLS_BITCOIN", "https://btc.example.com")
	t.Setenv("FINALITY_BLOCKS_BITCOIN", "6")
	t.Setenv("GAS_STRATEGY_BITCOIN", "bitcoin_rbf")
	// global default should not be used because all chains override

	cfgs, err := LoadConfigs()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cfgs) != 4 {
		t.Fatalf("expected 4 configs, got %d: %+v", len(cfgs), cfgs)
	}

	want := map[string]ChainConfig{
		"ethereum": {
			ChainID:        "ethereum",
			RPCURLs:        []string{"https://eth.example.com", "https://eth2.example.com"},
			WSURLs:         []string{"wss://eth.example.com"},
			FinalityBlocks: 64,
			GasStrategy:    "eip1559_dynamic",
		},
		"polygon": {
			ChainID:        "polygon",
			RPCURLs:        []string{"https://poly.example.com"},
			FinalityBlocks: 256,
			GasStrategy:    DefaultGasStrategy,
		},
		"solana": {
			ChainID:        "solana",
			RPCURLs:        []string{"https://sol.example.com"},
			WSURLs:         []string{"wss://sol.example.com"},
			FinalityBlocks: 1,
			GasStrategy:    "solana_priority_fee",
		},
		"bitcoin": {
			ChainID:        "bitcoin",
			RPCURLs:        []string{"https://btc.example.com"},
			FinalityBlocks: 6,
			GasStrategy:    "bitcoin_rbf",
		},
	}

	got := make(map[string]ChainConfig, len(cfgs))
	for _, c := range cfgs {
		got[c.ChainID] = c
	}

	for id, w := range want {
		g, ok := got[id]
		if !ok {
			t.Errorf("missing config for %q", id)
			continue
		}
		if g.FinalityBlocks != w.FinalityBlocks {
			t.Errorf("%q finality_blocks = %d, want %d", id, g.FinalityBlocks, w.FinalityBlocks)
		}
		if g.GasStrategy != w.GasStrategy {
			t.Errorf("%q gas_strategy = %q, want %q", id, g.GasStrategy, w.GasStrategy)
		}
		if !sliceEq(g.RPCURLs, w.RPCURLs) {
			t.Errorf("%q rpc_urls = %v, want %v", id, g.RPCURLs, w.RPCURLs)
		}
		if !sliceEq(g.WSURLs, w.WSURLs) {
			t.Errorf("%q ws_urls = %v, want %v", id, g.WSURLs, w.WSURLs)
		}
	}
}

func TestLoadConfigs_GlobalGasStrategyDefault(t *testing.T) {
	t.Setenv("CHAINS_SUPPORTED", "ethereum")
	t.Setenv("RPC_URLS_ETHEREUM", "https://eth.example.com")
	t.Setenv("FINALITY_BLOCKS_ETHEREUM", "64")
	t.Setenv("GAS_STRATEGY", "eip1559_legacy_fallback")

	cfgs, err := LoadConfigs()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cfgs) != 1 {
		t.Fatalf("expected 1 config, got %d", len(cfgs))
	}
	if cfgs[0].GasStrategy != "eip1559_legacy_fallback" {
		t.Fatalf("gas_strategy = %q, want eip1559_legacy_fallback", cfgs[0].GasStrategy)
	}
}

func TestLoadConfigs_EmptySupported(t *testing.T) {
	t.Setenv("CHAINS_SUPPORTED", "")
	cfgs, err := LoadConfigs()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cfgs) != 0 {
		t.Fatalf("expected 0 configs, got %d", len(cfgs))
	}
}

func TestLoadConfigs_MissingRequired(t *testing.T) {
	t.Setenv("CHAINS_SUPPORTED", "ethereum,polygon")
	t.Setenv("RPC_URLS_ETHEREUM", "https://eth.example.com")
	t.Setenv("FINALITY_BLOCKS_ETHEREUM", "64")
	// polygon missing RPC and FINALITY_BLOCKS

	cfgs, err := LoadConfigs()
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	cerr, ok := err.(*ConfigLoadError)
	if !ok {
		t.Fatalf("expected *ConfigLoadError, got %T", err)
	}
	if _, ok := cerr.Errors["polygon"]; !ok {
		t.Fatalf("expected polygon error, got %v", cerr.Errors)
	}
	// ethereum should still be present and valid
	if len(cfgs) != 1 {
		t.Fatalf("expected 1 valid config (ethereum), got %d", len(cfgs))
	}
	if cfgs[0].ChainID != "ethereum" {
		t.Fatalf("expected ethereum config, got %q", cfgs[0].ChainID)
	}
}

func TestLoadConfigs_BadFinalityBlocks(t *testing.T) {
	t.Setenv("CHAINS_SUPPORTED", "ethereum")
	t.Setenv("RPC_URLS_ETHEREUM", "https://eth.example.com")
	t.Setenv("FINALITY_BLOCKS_ETHEREUM", "not-a-number")

	_, err := LoadConfigs()
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	cerr, ok := err.(*ConfigLoadError)
	if !ok {
		t.Fatalf("expected *ConfigLoadError, got %T", err)
	}
	if _, ok := cerr.Errors["ethereum"]; !ok {
		t.Fatalf("expected ethereum error, got %v", cerr.Errors)
	}
}

func TestStubAdapter(t *testing.T) {
	a := NewStubAdapter("ethereum", 64)
	if a.ChainID() != "ethereum" {
		t.Fatalf("ChainID = %q, want ethereum", a.ChainID())
	}
	if a.FinalityBlocks() != 64 {
		t.Fatalf("FinalityBlocks = %d, want 64", a.FinalityBlocks())
	}
	h, err := a.Height(nil)
	if err != nil || h != 0 {
		t.Fatalf("Height = (%d, %v), want (0, nil)", h, err)
	}
	bal, err := a.Balance(nil, "")
	if err != nil || bal.Cmp(big.NewInt(0)) != 0 {
		t.Fatalf("Balance = (%v, %v), want (0, nil)", bal, err)
	}
	txh, err := a.Broadcast(nil, []byte("tx"))
	if err != nil || txh != "" {
		t.Fatalf("Broadcast = (%q, %v), want (\"\", nil)", txh, err)
	}
}

func sliceEq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
