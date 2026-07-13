package chain

import (
	"context"
	"errors"
	"math/big"
	"os"
	"testing"
	"time"
)

func TestStatusLifecycle(t *testing.T) {
	cases := []struct {
		from, to Status
		ok       bool
	}{
		{StatusBroadcast, StatusMempool, true},
		{StatusBroadcast, StatusConfirmed, true},
		{StatusMempool, StatusConfirmed, true},
		{StatusMempool, StatusDropped, true},
		{StatusMempool, StatusReplaced, true},
		{StatusConfirmed, StatusFinalized, true},
		{StatusConfirmed, StatusReorgedOut, true},
		{StatusReorgedOut, StatusConfirmed, true},
		{StatusFinalized, StatusMempool, false},
		{StatusDropped, StatusMempool, false},
		{StatusReplaced, StatusConfirmed, false},
		{StatusFailed, StatusConfirmed, false},
	}
	for _, c := range cases {
		if got := c.from.CanTransitionTo(c.to); got != c.ok {
			t.Errorf("CanTransitionTo(%s->%s)=%v want %v", c.from, c.to, got, c.ok)
		}
	}
}

func TestStatusIsTerminal(t *testing.T) {
	terminal := []Status{StatusFinalized, StatusDropped, StatusReplaced, StatusFailed}
	for _, s := range terminal {
		if !s.IsTerminal() {
			t.Errorf("%s should be terminal", s)
		}
	}
	nonTerminal := []Status{StatusBroadcast, StatusMempool, StatusConfirmed, StatusReorgedOut}
	for _, s := range nonTerminal {
		if s.IsTerminal() {
			t.Errorf("%s should not be terminal", s)
		}
	}
}

func TestConfigLoader(t *testing.T) {
	env := map[string]string{
		"CHAINS_SUPPORTED":        "ethereum,polygon,solana,bitcoin",
		"RPC_URLS_ETHEREUM":       "https://a.io,https://b.io",
		"WS_URLS_ETHEREUM":        "wss://a.io,wss://b.io",
		"FINALITY_BLOCKS_ETHEREUM": "64",
		"FINALITY_BLOCKS_POLYGON":  "256",
		"FINALITY_BLOCKS_SOLANA":   "1",
		"FINALITY_BLOCKS_BITCOIN":  "6",
		"GAS_STRATEGY_SOLANA":      "solana_priority_fee",
		"GAS_STRATEGY":             "eip1559_dynamic",
	}
	loader := newConfigLoaderWithEnv(func(k string) string { return env[k] })
	cfgs, err := loader.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(cfgs) != 4 {
		t.Fatalf("expected 4 chains, got %d", len(cfgs))
	}
	byID := map[string]ChainConfig{}
	for _, c := range cfgs {
		byID[c.ChainID] = c
	}
	if len(byID["ethereum"].RPCURLs) != 2 {
		t.Errorf("ethereum rpc urls: %v", byID["ethereum"].RPCURLs)
	}
	if byID["ethereum"].FinalityBlocks != 64 {
		t.Errorf("ethereum finality: %d", byID["ethereum"].FinalityBlocks)
	}
	if byID["solana"].GasStrategy != "solana_priority_fee" {
		t.Errorf("solana strategy: %s", byID["solana"].GasStrategy)
	}
	if byID["polygon"].GasStrategy != "eip1559_legacy_fallback" {
		t.Errorf("polygon strategy default: %s", byID["polygon"].GasStrategy)
	}
}

func TestConfigLoaderMissingSupported(t *testing.T) {
	loader := newConfigLoaderWithEnv(func(string) string { return "" })
	if _, err := loader.Load(); err == nil {
		t.Fatal("expected error when CHAINS_SUPPORTED missing")
	}
}

func TestRegistry(t *testing.T) {
	r := NewRegistry()
	if _, err := r.Get("nope"); err == nil {
		t.Fatal("expected error for unknown chain")
	}
	stub := NewStubAdapter(StubAdapterOptions{ChainID: "stub", FinalityBlocks: 3})
	r.Register(stub)
	got, err := r.Get("stub")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.ChainID() != "stub" {
		t.Errorf("chain id: %s", got.ChainID())
	}
	if len(r.Chains()) != 1 {
		t.Errorf("chains: %v", r.Chains())
	}
}

func TestStubAdapterBroadcast(t *testing.T) {
	a := NewStubAdapter(StubAdapterOptions{ChainID: "stub", FinalityBlocks: 3})
	h1, err := a.Broadcast(context.Background(), []byte("signed-tx-payload"))
	if err != nil {
		t.Fatalf("broadcast: %v", err)
	}
	h2, err := a.Broadcast(context.Background(), []byte("signed-tx-payload"))
	if err != nil {
		t.Fatalf("broadcast2: %v", err)
	}
	if h1 != h2 {
		t.Errorf("idempotency broken: %s != %s", h1, h2)
	}
	if a.(*stubAdapter).BroadcastCount() != 2 {
		t.Errorf("broadcast count")
	}
}

func TestStubAdapterBalanceAndHeight(t *testing.T) {
	a := NewStubAdapter(StubAdapterOptions{ChainID: "stub", FinalityBlocks: 3, Height: 100, Balance: big.NewInt(1_000_000_000)})
	h, err := a.Height(context.Background())
	if err != nil || h != 100 {
		t.Fatalf("height: %v %d", err, h)
	}
	b, err := a.Balance(context.Background(), "0xabc")
	if err != nil || b.Cmp(big.NewInt(1_000_000_000)) != 0 {
		t.Fatalf("balance: %v %s", err, b)
	}
}

func TestStubAdapterEstimateFee(t *testing.T) {
	a := NewStubAdapter(StubAdapterOptions{ChainID: "stub", FinalityBlocks: 3})
	fe, err := a.EstimateFee(context.Background(), FeeEstimateReq{Priority: PriorityHigh})
	if err != nil {
		t.Fatalf("estimate: %v", err)
	}
	if fe.GasLimit != 21000 {
		t.Errorf("gas limit: %d", fe.GasLimit)
	}
}

func TestStubAdapterHeadsAndMempool(t *testing.T) {
	a := NewStubAdapter(StubAdapterOptions{ChainID: "stub", FinalityBlocks: 3})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	heads, _, err := a.SubscribeHeads(ctx)
	if err != nil {
		t.Fatalf("subscribe heads: %v", err)
	}
	a.(*stubAdapter).EmitHead(Head{ChainID: "stub", Height: 1})
	select {
	case h := <-heads:
		if h.Height != 1 {
			t.Errorf("head height: %d", h.Height)
		}
	default:
		t.Fatal("no head received")
	}
	mp, _, err := a.SubscribeMempool(ctx, nil)
	if err != nil {
		t.Fatalf("subscribe mempool: %v", err)
	}
	a.(*stubAdapter).EmitMempool(MempoolEvent{ChainID: "stub", TxHash: "0x1", Kind: "enter"})
	select {
	case e := <-mp:
		if e.TxHash != "0x1" {
			t.Errorf("mempool tx: %s", e.TxHash)
		}
	default:
		t.Fatal("no mempool event received")
	}
}

func TestStubAdapterGetTxNotFound(t *testing.T) {
	a := NewStubAdapter(StubAdapterOptions{ChainID: "stub", FinalityBlocks: 3})
	if _, err := a.GetTx(context.Background(), "0xmissing"); err != ErrTxNotFound {
		t.Fatalf("expected ErrTxNotFound, got %v", err)
	}
}

func TestParseHexQuantity(t *testing.T) {
	cases := []struct{ in string; want uint64 }{
		{"0x0", 0},
		{"0x1", 1},
		{"0xa", 10},
		{"0x64", 100},
		{"", 0},
	}
	for _, c := range cases {
		got, err := parseHexQuantity(c.in)
		if err != nil || got != c.want {
			t.Errorf("parseHexQuantity(%q)=%d,%v want %d", c.in, got, err, c.want)
		}
	}
}

func TestParseHexBig(t *testing.T) {
	n, err := parseHexBig("0xff")
	if err != nil || n.Cmp(big.NewInt(255)) != 0 {
		t.Fatalf("parseHexBig: %v %s", err, n)
	}
}

func TestEncodeBase64(t *testing.T) {
	out := encodeBase64([]byte("hello"))
	if out != "aGVsbG8=" {
		t.Errorf("encodeBase64: %s", out)
	}
}

func TestNewConfigLoader(t *testing.T) {
	l := NewConfigLoader()
	if l == nil {
		t.Fatal("NewConfigLoader returned nil")
	}
	if l.env("CHAINS_SUPPORTED") != "" && os.Getenv("CHAINS_SUPPORTED") == "" {
		t.Error("env function mismatch")
	}
}

func TestConfigLoaderUnknownChain(t *testing.T) {
	env := map[string]string{
		"CHAINS_SUPPORTED": "cosmos",
	}
	loader := newConfigLoaderWithEnv(func(k string) string { return env[k] })
	cfgs, err := loader.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(cfgs) != 1 || cfgs[0].ChainID != "cosmos" {
		t.Fatalf("cfgs: %+v", cfgs)
	}
	if cfgs[0].GasStrategy != "eip1559_dynamic" {
		t.Errorf("default strategy: %s", cfgs[0].GasStrategy)
	}
}

func TestConfigLoaderBadFinality(t *testing.T) {
	env := map[string]string{
		"CHAINS_SUPPORTED":        "ethereum",
		"FINALITY_BLOCKS_ETHEREUM": "notanumber",
	}
	loader := newConfigLoaderWithEnv(func(k string) string { return env[k] })
	if _, err := loader.Load(); err == nil {
		t.Fatal("expected error for bad finality")
	}
}

func TestConfigLoaderDefaultGasStrategy(t *testing.T) {
	env := map[string]string{
		"CHAINS_SUPPORTED": "cosmos",
		"GAS_STRATEGY":     "custom",
	}
	loader := newConfigLoaderWithEnv(func(k string) string { return env[k] })
	cfgs, err := loader.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfgs[0].GasStrategy != "custom" {
		t.Errorf("custom default strategy: %s", cfgs[0].GasStrategy)
	}
}

func TestConfigLoaderWhitespaceInChains(t *testing.T) {
	env := map[string]string{
		"CHAINS_SUPPORTED": "  ethereum  ,  polygon  ",
	}
	loader := newConfigLoaderWithEnv(func(k string) string { return env[k] })
	cfgs, err := loader.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(cfgs) != 2 {
		t.Fatalf("expected 2 chains, got %d", len(cfgs))
	}
}

func TestSplitCSV(t *testing.T) {
	if out := splitCSV(""); out != nil {
		t.Errorf("empty splitCSV: %v", out)
	}
	if out := splitCSV("a, b ,c"); len(out) != 3 || out[0] != "a" || out[1] != "b" || out[2] != "c" {
		t.Errorf("splitCSV: %v", out)
	}
}

func TestRegistryChainsSorted(t *testing.T) {
	r := NewRegistry()
	r.Register(NewStubAdapter(StubAdapterOptions{ChainID: "zchain", FinalityBlocks: 1}))
	r.Register(NewStubAdapter(StubAdapterOptions{ChainID: "achain", FinalityBlocks: 1}))
	chains := r.Chains()
	if len(chains) != 2 {
		t.Fatalf("chains: %v", chains)
	}
}

func TestRegistryAsStubPanicsOnUnknown(t *testing.T) {
	r := NewRegistry()
	defer func() {
		if recover() == nil {
			t.Error("expected panic for unknown chain")
		}
	}()
	_ = r.AsStub("nope")
}

func TestRegistryAsStubPanicsOnNonStub(t *testing.T) {
	r := NewRegistry()
	r.Register(NewEVMAdapter(ChainConfig{ChainID: "ethereum", RPCURLs: nil, FinalityBlocks: 64}))
	defer func() {
		if recover() == nil {
			t.Error("expected panic for non-stub adapter")
		}
	}()
	_ = r.AsStub("ethereum")
}

// TestRegistryStubEmitter exercises the StubEmitter method which delegates
// to AsStub.
func TestRegistryStubEmitter(t *testing.T) {
	r := NewRegistry()
	r.Register(NewStubAdapter(StubAdapterOptions{ChainID: "stub", FinalityBlocks: 3}))
	em := r.StubEmitter("stub")
	if em == nil {
		t.Fatal("StubEmitter returned nil")
	}
	em.EmitHead(Head{ChainID: "stub", Height: 1})
	em.EmitMempool(MempoolEvent{ChainID: "stub", TxHash: "0x1", Kind: "enter"})
	if em.BroadcastCount() != 0 {
		t.Errorf("broadcast count: %d want 0", em.BroadcastCount())
	}
}

// TestRegistryStubEmitterPanicsOnUnknown ensures StubEmitter panics for an
// unknown chain (delegating to AsStub).
func TestRegistryStubEmitterPanicsOnUnknown(t *testing.T) {
	r := NewRegistry()
	defer func() {
		if recover() == nil {
			t.Error("expected panic for unknown chain")
		}
	}()
	_ = r.StubEmitter("nope")
}

func TestStubAdapterBroadcastFn(t *testing.T) {
	a := NewStubAdapter(StubAdapterOptions{
		ChainID:     "stub",
		FinalityBlocks: 3,
		BroadcastFn: func(_ context.Context, _ []byte) (string, error) {
			return "0xcustom", nil
		},
	})
	h, err := a.Broadcast(context.Background(), []byte("payload"))
	if err != nil || h != "0xcustom" {
		t.Fatalf("broadcast fn: %v %s", err, h)
	}
}

func TestStubAdapterBroadcastErr(t *testing.T) {
	a := NewStubAdapter(StubAdapterOptions{
		ChainID:      "stub",
		FinalityBlocks: 3,
		BroadcastErr: errors.New("boom"),
	})
	if _, err := a.Broadcast(context.Background(), []byte("payload")); err == nil {
		t.Fatal("expected broadcast error")
	}
}

func TestStubAdapterEstimateFeeCustom(t *testing.T) {
	a := NewStubAdapter(StubAdapterOptions{
		ChainID:     "stub",
		FinalityBlocks: 3,
		FeeEstimate: &FeeEstimate{ChainID: "stub", GasLimit: 999, GasPrice: big.NewInt(7), TotalFee: big.NewInt(7000), Strategy: "custom"},
	})
	fe, err := a.EstimateFee(context.Background(), FeeEstimateReq{Priority: PriorityHigh})
	if err != nil {
		t.Fatalf("estimate: %v", err)
	}
	if fe.GasLimit != 999 {
		t.Errorf("gas limit: %d want 999", fe.GasLimit)
	}
}

func TestStubAdapterGetTxStatusNotFound(t *testing.T) {
	a := NewStubAdapter(StubAdapterOptions{ChainID: "stub", FinalityBlocks: 3})
	if _, err := a.(*stubAdapter).GetTxStatus(context.Background(), "0xmissing"); err != ErrTxNotFound {
		t.Fatalf("expected ErrTxNotFound, got %v", err)
	}
}

func TestStubAdapterFinalityBlocks(t *testing.T) {
	a := NewStubAdapter(StubAdapterOptions{ChainID: "stub", FinalityBlocks: 42})
	if a.FinalityBlocks() != 42 {
		t.Errorf("finality: %d want 42", a.FinalityBlocks())
	}
}

func TestStubAdapterEmitHeadBackpressure(t *testing.T) {
	a := NewStubAdapter(StubAdapterOptions{ChainID: "stub", FinalityBlocks: 3}).(*stubAdapter)
	// Fill the heads channel (capacity 16) then emit one more; EmitHead
	// should not block.
	for i := 0; i < 16; i++ {
		a.EmitHead(Head{ChainID: "stub", Height: uint64(i + 1)})
	}
	done := make(chan struct{})
	go func() {
		a.EmitHead(Head{ChainID: "stub", Height: 99})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("EmitHead blocked on full channel")
	}
}

func TestStubAdapterEmitMempoolBackpressure(t *testing.T) {
	a := NewStubAdapter(StubAdapterOptions{ChainID: "stub", FinalityBlocks: 3}).(*stubAdapter)
	for i := 0; i < 16; i++ {
		a.EmitMempool(MempoolEvent{ChainID: "stub", TxHash: "0x1"})
	}
	done := make(chan struct{})
	go func() {
		a.EmitMempool(MempoolEvent{ChainID: "stub", TxHash: "0x2"})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("EmitMempool blocked on full channel")
	}
}