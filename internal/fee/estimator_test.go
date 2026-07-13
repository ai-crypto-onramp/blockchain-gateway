package fee

import (
	"context"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/ai-crypto-onramp/blockchain-gateway/internal/chain"
)

func makeFees(vals ...int64) []*big.Int {
	out := make([]*big.Int, len(vals))
	for i, v := range vals {
		out[i] = big.NewInt(v)
	}
	return out
}

func TestPercentileNearestRank(t *testing.T) {
	// 0..99 inclusive (100 values). Nearest-rank: index = ceil(p/100*n) - 1.
	// p10 -> ceil(10) - 1 = 9 -> value 9.
	// p50 -> ceil(50) - 1 = 49 -> value 49.
	// p90 -> ceil(90) - 1 = 89 -> value 89.
	xs := makeFees(seq(0, 100)...)
	if got := Percentile(xs, 10); got.Cmp(big.NewInt(9)) != 0 {
		t.Errorf("p10=%s want 9", got)
	}
	if got := Percentile(xs, 50); got.Cmp(big.NewInt(49)) != 0 {
		t.Errorf("p50=%s want 49", got)
	}
	if got := Percentile(xs, 90); got.Cmp(big.NewInt(89)) != 0 {
		t.Errorf("p90=%s want 89", got)
	}
}

func TestPercentileEdgeCases(t *testing.T) {
	if got := Percentile(nil, 50); got != nil {
		t.Errorf("nil should yield nil, got %s", got)
	}
	xs := makeFees(7)
	if got := Percentile(xs, 50); got.Cmp(big.NewInt(7)) != 0 {
		t.Errorf("single p50=%s want 7", got)
	}
	if got := Percentile(xs, 0); got.Cmp(big.NewInt(7)) != 0 {
		t.Errorf("single p0=%s want 7", got)
	}
	if got := Percentile(xs, 100); got.Cmp(big.NewInt(7)) != 0 {
		t.Errorf("single p100=%s want 7", got)
	}
}

func seq(start, n int) []int64 {
	out := make([]int64, n)
	for i := 0; i < n; i++ {
		out[i] = int64(start + i)
	}
	return out
}

func TestEstimatorEIP1559(t *testing.T) {
	e := NewEstimator("ethereum", "eip1559_dynamic", 64)
	e.PushSample(BlockSample{
		Number:       1,
		BaseFee:      big.NewInt(10_000_000_000), // 10 gwei
		PriorityFees: makeFees(seq(1_000_000_000, 100)...), // 1..100 gwei
	})
	for _, p := range []chain.Priority{chain.PriorityLow, chain.PriorityStandard, chain.PriorityHigh} {
		fe, err := e.Estimate(context.Background(), nil, chain.FeeEstimateReq{Priority: p})
		if err != nil {
			t.Fatalf("estimate %s: %v", p, err)
		}
		if fe.MaxFeePerGas == nil || fe.MaxPriorityFeePerGas == nil {
			t.Fatalf("missing fee fields for %s", p)
		}
		// maxFee = 2*base + priority.
		want := new(big.Int).Add(new(big.Int).Mul(big.NewInt(10_000_000_000), big.NewInt(2)), fe.MaxPriorityFeePerGas)
		if fe.MaxFeePerGas.Cmp(want) != 0 {
			t.Errorf("%s maxFee=%s want %s", p, fe.MaxFeePerGas, want)
		}
		if fe.GasLimit != 21000 {
			t.Errorf("gas limit: %d", fe.GasLimit)
		}
	}
	// Priority ordering: low < standard < high.
	feLow, _ := e.Estimate(context.Background(), nil, chain.FeeEstimateReq{Priority: chain.PriorityLow})
	feStd, _ := e.Estimate(context.Background(), nil, chain.FeeEstimateReq{Priority: chain.PriorityStandard})
	feHigh, _ := e.Estimate(context.Background(), nil, chain.FeeEstimateReq{Priority: chain.PriorityHigh})
	if feLow.MaxPriorityFeePerGas.Cmp(feStd.MaxPriorityFeePerGas) >= 0 {
		t.Errorf("low priority should be < standard")
	}
	if feStd.MaxPriorityFeePerGas.Cmp(feHigh.MaxPriorityFeePerGas) >= 0 {
		t.Errorf("standard priority should be < high")
	}
}

func TestEstimatorNoSamples(t *testing.T) {
	e := NewEstimator("ethereum", "eip1559_dynamic", 64)
	if _, err := e.Estimate(context.Background(), nil, chain.FeeEstimateReq{Priority: chain.PriorityStandard}); err == nil {
		t.Fatal("expected error with no samples")
	}
}

func TestEstimatorLegacyFallback(t *testing.T) {
	e := NewEstimator("polygon", "eip1559_legacy_fallback", 256)
	// No samples -> should fall back to adapter.
	stub := chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "polygon", FinalityBlocks: 256})
	fe, err := e.Estimate(context.Background(), stub, chain.FeeEstimateReq{Priority: chain.PriorityStandard})
	if err != nil {
		t.Fatalf("fallback: %v", err)
	}
	if fe.GasPrice == nil {
		t.Fatal("expected gas price from fallback")
	}
}

func TestEstimatorLegacyOnly(t *testing.T) {
	e := NewEstimator("bitcoin", "legacy_only", 6)
	stub := chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "bitcoin", FinalityBlocks: 6})
	fe, err := e.Estimate(context.Background(), stub, chain.FeeEstimateReq{Priority: chain.PriorityStandard})
	if err != nil {
		t.Fatalf("legacy: %v", err)
	}
	if fe.GasPrice == nil {
		t.Fatal("expected gas price")
	}
}

func TestEstimatorSampleFIFO(t *testing.T) {
	e := NewEstimator("ethereum", "eip1559_dynamic", 64)
	for i := 0; i < e.maxSamples+5; i++ {
		e.PushSample(BlockSample{Number: uint64(i), BaseFee: big.NewInt(int64(i))})
	}
	s := e.Samples()
	if len(s) != e.maxSamples {
		t.Errorf("samples kept: %d want %d", len(s), e.maxSamples)
	}
	// FIFO: oldest kept should be sample 5.
	if s[0].Number != 5 {
		t.Errorf("oldest sample: %d want 5", s[0].Number)
	}
}

// fakeFeeStore is a FeeStoreAdapter that records InsertFee calls.
type fakeFeeStore struct {
	mu   sync.Mutex
	rows []FeeRow
}

func (f *fakeFeeStore) InsertFee(_ context.Context, r FeeRow) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rows = append(f.rows, r)
	return nil
}

func (f *fakeFeeStore) Rows() []FeeRow {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]FeeRow(nil), f.rows...)
}

func TestRecomputeLoop(t *testing.T) {
	e := NewEstimator("ethereum", "eip1559_dynamic", 64)
	e.PushSample(BlockSample{Number: 1, BaseFee: big.NewInt(10_000_000_000), PriorityFees: makeFees(1_000_000_000, 2_000_000_000, 3_000_000_000)})
	stub := chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "ethereum", FinalityBlocks: 64})
	fs := &fakeFeeStore{}
	ctx, cancel := context.WithCancel(context.Background())
	go e.RecomputeLoop(ctx, stub, fs, []chain.Priority{chain.PriorityStandard}, 0)
	// The loop ticks immediately on the first interval; since interval=0
	// defaults to 15s, cancel and assert no panic.
	cancel()
}

// TestEstimatorEIP1559DefaultPriority ensures an empty Priority is
// promoted to PriorityStandard inside Estimate.
func TestEstimatorEIP1559DefaultPriority(t *testing.T) {
	e := NewEstimator("ethereum", "eip1559_dynamic", 64)
	e.PushSample(BlockSample{Number: 1, BaseFee: big.NewInt(10_000_000_000), PriorityFees: makeFees(1_000_000_000)})
	fe, err := e.Estimate(context.Background(), nil, chain.FeeEstimateReq{Priority: ""})
	if err != nil {
		t.Fatalf("estimate: %v", err)
	}
	if fe.Priority != chain.PriorityStandard {
		t.Errorf("priority: %s want standard", fe.Priority)
	}
}

// TestEstimatorEIP1559NilBaseFeeNoFallback exercises the missing-base-fee
// branch without fallback (returns an error).
func TestEstimatorEIP1559NilBaseFeeNoFallback(t *testing.T) {
	e := NewEstimator("ethereum", "eip1559_dynamic", 64)
	e.PushSample(BlockSample{Number: 1, BaseFee: nil, PriorityFees: makeFees(1_000_000_000)})
	if _, err := e.Estimate(context.Background(), nil, chain.FeeEstimateReq{Priority: chain.PriorityStandard}); err == nil {
		t.Fatal("expected error for missing base fee")
	}
}

// TestEstimatorEIP1559NilBaseFeeFallback exercises the missing-base-fee
// branch with fallback (returns an error so the caller falls back to the
// adapter).
func TestEstimatorEIP1559NilBaseFeeFallback(t *testing.T) {
	e := NewEstimator("polygon", "eip1559_legacy_fallback", 256)
	e.PushSample(BlockSample{Number: 1, BaseFee: nil, PriorityFees: makeFees(1_000_000_000)})
	stub := chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "polygon", FinalityBlocks: 256})
	fe, err := e.Estimate(context.Background(), stub, chain.FeeEstimateReq{Priority: chain.PriorityStandard})
	if err != nil {
		t.Fatalf("fallback: %v", err)
	}
	if fe == nil {
		t.Fatal("expected non-nil fee from fallback adapter")
	}
}

// TestEstimatorEIP1559EmptyPriorityFees exercises the branch where the
// latest sample has no priority fees (percentile returns nil -> default
// 1 gwei).
func TestEstimatorEIP1559EmptyPriorityFees(t *testing.T) {
	e := NewEstimator("ethereum", "eip1559_dynamic", 64)
	e.PushSample(BlockSample{Number: 1, BaseFee: big.NewInt(10_000_000_000), PriorityFees: nil})
	fe, err := e.Estimate(context.Background(), nil, chain.FeeEstimateReq{Priority: chain.PriorityStandard})
	if err != nil {
		t.Fatalf("estimate: %v", err)
	}
	if fe.MaxPriorityFeePerGas == nil || fe.MaxPriorityFeePerGas.Cmp(big.NewInt(1_000_000_000)) != 0 {
		t.Errorf("default priority fee: %s want 1 gwei", fe.MaxPriorityFeePerGas)
	}
}

// TestEstimatorSolanaStrategy exercises the solana_priority_fee strategy
// branch which delegates to the adapter.
func TestEstimatorSolanaStrategy(t *testing.T) {
	e := NewEstimator("solana", "solana_priority_fee", 1)
	stub := chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "solana", FinalityBlocks: 1})
	fe, err := e.Estimate(context.Background(), stub, chain.FeeEstimateReq{Priority: chain.PriorityHigh})
	if err != nil {
		t.Fatalf("solana estimate: %v", err)
	}
	if fe == nil {
		t.Fatal("nil fee")
	}
}

// TestEstimatorBitcoinStrategy exercises the bitcoin_rbf strategy branch.
func TestEstimatorBitcoinStrategy(t *testing.T) {
	e := NewEstimator("bitcoin", "bitcoin_rbf", 6)
	stub := chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "bitcoin", FinalityBlocks: 6})
	fe, err := e.Estimate(context.Background(), stub, chain.FeeEstimateReq{Priority: chain.PriorityLow})
	if err != nil {
		t.Fatalf("bitcoin estimate: %v", err)
	}
	if fe == nil {
		t.Fatal("nil fee")
	}
}

// TestEstimatorCustomStrategy exercises the custom strategy branch.
func TestEstimatorCustomStrategy(t *testing.T) {
	e := NewEstimator("custom", "custom", 1)
	stub := chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "custom", FinalityBlocks: 1})
	fe, err := e.Estimate(context.Background(), stub, chain.FeeEstimateReq{Priority: chain.PriorityStandard})
	if err != nil {
		t.Fatalf("custom estimate: %v", err)
	}
	if fe == nil {
		t.Fatal("nil fee")
	}
}

// TestEstimatorUnknownStrategyDefaultsToAdapter verifies that an unknown
// strategy falls through to adapter.EstimateFee.
func TestEstimatorUnknownStrategyDefaultsToAdapter(t *testing.T) {
	e := NewEstimator("foo", "unknown_strategy", 1)
	stub := chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "foo", FinalityBlocks: 1})
	fe, err := e.Estimate(context.Background(), stub, chain.FeeEstimateReq{Priority: chain.PriorityStandard})
	if err != nil {
		t.Fatalf("estimate: %v", err)
	}
	if fe == nil {
		t.Fatal("nil fee")
	}
}

// TestEstimatorEIP1559FallbackNoError verifies that when the EIP-1559
// estimate succeeds, the fallback adapter is NOT consulted.
func TestEstimatorEIP1559FallbackNoError(t *testing.T) {
	e := NewEstimator("polygon", "eip1559_legacy_fallback", 256)
	e.PushSample(BlockSample{Number: 1, BaseFee: big.NewInt(10_000_000_000), PriorityFees: makeFees(1_000_000_000)})
	stub := chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "polygon", FinalityBlocks: 256})
	fe, err := e.Estimate(context.Background(), stub, chain.FeeEstimateReq{Priority: chain.PriorityStandard})
	if err != nil {
		t.Fatalf("estimate: %v", err)
	}
	// EIP-1559 path populates MaxFeePerGas; the stub adapter populates
	// GasPrice. We expect MaxFeePerGas to be set.
	if fe.MaxFeePerGas == nil {
		t.Error("expected MaxFeePerGas from EIP-1559 path, not adapter fallback")
	}
}

// TestPercentileForPriority maps priority tiers to percentiles.
func TestPercentileForPriority(t *testing.T) {
	if got := percentileForPriority(chain.PriorityLow); got != 10 {
		t.Errorf("low: %v want 10", got)
	}
	if got := percentileForPriority(chain.PriorityHigh); got != 90 {
		t.Errorf("high: %v want 90", got)
	}
	if got := percentileForPriority(chain.PriorityStandard); got != 50 {
		t.Errorf("standard: %v want 50", got)
	}
	if got := percentileForPriority(chain.Priority("weird")); got != 50 {
		t.Errorf("unknown: %v want 50", got)
	}
}

// TestPercentilePriorityClamping exercises the p<0 and p>100 clamping
// branches.
func TestPercentilePriorityClamping(t *testing.T) {
	xs := makeFees(1, 2, 3, 4, 5)
	if got := percentilePriority(xs, -10); got.Cmp(big.NewInt(1)) != 0 {
		t.Errorf("p-10: %s want 1", got)
	}
	if got := percentilePriority(xs, 200); got.Cmp(big.NewInt(5)) != 0 {
		t.Errorf("p200: %s want 5", got)
	}
}

// TestRecomputeLoopInsertsRows drives RecomputeLoop with a short interval
// and asserts rows are inserted into the fee store via toRow.
func TestRecomputeLoopInsertsRows(t *testing.T) {
	e := NewEstimator("ethereum", "eip1559_dynamic", 64)
	e.PushSample(BlockSample{Number: 1, BaseFee: big.NewInt(10_000_000_000), PriorityFees: makeFees(1_000_000_000, 2_000_000_000, 3_000_000_000)})
	stub := chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "ethereum", FinalityBlocks: 64})
	fs := &fakeFeeStore{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go e.RecomputeLoop(ctx, stub, fs, []chain.Priority{chain.PriorityStandard}, 10*time.Millisecond)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(fs.Rows()) > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(fs.Rows()) == 0 {
		t.Fatal("expected at least one row inserted by RecomputeLoop")
	}
	r := fs.Rows()[0]
	if r.ChainID != "ethereum" {
		t.Errorf("row chain id: %s", r.ChainID)
	}
	if r.MaxFeePerGas == nil {
		t.Error("row MaxFeePerGas should be set for eip1559")
	}
}

// TestRecomputeLoopDefaultsPriorities ensures an empty priorities slice is
// replaced with the default set.
func TestRecomputeLoopDefaultsPriorities(t *testing.T) {
	e := NewEstimator("ethereum", "legacy_only", 64)
	stub := chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "ethereum", FinalityBlocks: 64})
	fs := &fakeFeeStore{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go e.RecomputeLoop(ctx, stub, fs, nil, 10*time.Millisecond)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(fs.Rows()) >= 3 { // low + standard + high
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(fs.Rows()) < 3 {
		t.Errorf("expected >=3 rows for default priorities, got %d", len(fs.Rows()))
	}
}

// TestRecomputeLoopNilFeeStoreNoPanic ensures RecomputeLoop does not panic
// when feeStore is nil.
func TestRecomputeLoopNilFeeStoreNoPanic(t *testing.T) {
	e := NewEstimator("ethereum", "legacy_only", 64)
	stub := chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "ethereum", FinalityBlocks: 64})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go e.RecomputeLoop(ctx, stub, nil, []chain.Priority{chain.PriorityStandard}, 10*time.Millisecond)
	time.Sleep(40 * time.Millisecond)
}

// TestRecomputeLoopEstimateErrorSkipped ensures RecomputeLoop skips
// priorities whose Estimate returns an error (eip1559 with no samples).
func TestRecomputeLoopEstimateErrorSkipped(t *testing.T) {
	e := NewEstimator("ethereum", "eip1559_dynamic", 64)
	// No samples -> Estimate returns an error.
	stub := chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "ethereum", FinalityBlocks: 64})
	fs := &fakeFeeStore{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go e.RecomputeLoop(ctx, stub, fs, []chain.Priority{chain.PriorityStandard}, 10*time.Millisecond)
	time.Sleep(80 * time.Millisecond)
	if len(fs.Rows()) != 0 {
		t.Errorf("expected no rows when estimate errors, got %d", len(fs.Rows()))
	}
}

// TestToRow exercises the toRow helper directly.
func TestToRow(t *testing.T) {
	maxFee := big.NewInt(100)
	priority := big.NewInt(10)
	gasPrice := big.NewInt(50)
	total := big.NewInt(21000 * 100)
	ts := time.Now()
	r := toRow(&chain.FeeEstimate{
		ChainID:              "ethereum",
		Priority:             chain.PriorityHigh,
		GasLimit:             21000,
		MaxFeePerGas:         maxFee,
		MaxPriorityFeePerGas: priority,
		GasPrice:             gasPrice,
		TotalFee:             total,
		Strategy:             "eip1559_dynamic",
	}, 5, ts)
	if r.ChainID != "ethereum" || r.Priority != chain.PriorityHigh {
		t.Errorf("row: %+v", r)
	}
	if r.GasLimit != 21000 || r.SampleCount != 5 {
		t.Errorf("gas/sample: %d %d", r.GasLimit, r.SampleCount)
	}
	if r.MaxFeePerGas.Cmp(maxFee) != 0 || r.MaxPriorityFeePerGas.Cmp(priority) != 0 {
		t.Errorf("max fees: %s %s", r.MaxFeePerGas, r.MaxPriorityFeePerGas)
	}
	if r.GasPrice.Cmp(gasPrice) != 0 || r.TotalFee.Cmp(total) != 0 {
		t.Errorf("gas price/total: %s %s", r.GasPrice, r.TotalFee)
	}
	if r.Strategy != "eip1559_dynamic" || !r.ComputedAt.Equal(ts) {
		t.Errorf("strategy/ts: %s %v", r.Strategy, r.ComputedAt)
	}
}