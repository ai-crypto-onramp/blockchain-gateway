package fee

import (
	"context"
	"math/big"
	"testing"

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
	rows []FeeRow
}

func (f *fakeFeeStore) InsertFee(_ context.Context, r FeeRow) error {
	f.rows = append(f.rows, r)
	return nil
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