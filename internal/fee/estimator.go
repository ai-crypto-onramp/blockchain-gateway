// Package fee implements per-chain fee estimation. EVM chains use the
// EIP-1559 model: maxFeePerGas = baseFee * 2 + priorityFee, with
// maxPriorityFeePerGas derived from a percentile of recent block priority
// fees. The percentile math is the heart of the estimator and is fully
// covered by fixture-based unit tests (no live node required).
//
// Strategies:
//
//   - eip1559_dynamic          : EIP-1559 with dynamic priority percentile
//   - eip1559_legacy_fallback  : EIP-1559 if available, else gasPrice
//   - legacy_only              : eth_gasPrice only
//   - solana_priority_fee      : Solana priority fee
//   - bitcoin_rbf              : Bitcoin RBF fee
//   - custom                   : caller-supplied
package fee

import (
	"context"
	"errors"
	"math/big"
	"sort"
	"sync"
	"time"

	"github.com/ai-crypto-onramp/blockchain-gateway/internal/chain"
)

// BlockSample is a recent block's fee-relevant fields used by the
// percentile estimator.
type BlockSample struct {
	Number     uint64
	BaseFee    *big.Int
	GasUsed    uint64
	GasLimit   uint64
	// PriorityFees is one entry per transaction in the block (effective
	// priority fee = miner tip), in wei.
	PriorityFees []*big.Int
}

// Estimator computes fee estimates for a single chain. It is safe for
// concurrent use.
type Estimator struct {
	mu          sync.Mutex
	chainID     string
	strategy    string
	finality    uint64
	samples     []BlockSample
	maxSamples  int
	defaultGas  uint64
}

// NewEstimator returns an Estimator for the given chain + strategy.
func NewEstimator(chainID, strategy string, finalityBlocks uint64) *Estimator {
	return &Estimator{
		chainID:    chainID,
		strategy:   strategy,
		finality:   finalityBlocks,
		maxSamples: 20,
		defaultGas: 21000,
	}
}

// PushSample records a recent block sample. At most maxSamples are kept
// (FIFO).
func (e *Estimator) PushSample(s BlockSample) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.samples = append(e.samples, s)
	if len(e.samples) > e.maxSamples {
		e.samples = e.samples[len(e.samples)-e.maxSamples:]
	}
}

// Samples returns a copy of the current block samples (test helper).
func (e *Estimator) Samples() []BlockSample {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]BlockSample, len(e.samples))
	copy(out, e.samples)
	return out
}

// Estimate computes a fee estimate for the given priority tier. For
// eip1559_dynamic it requires at least one sample; for legacy_only /
// fallback it can derive from a single baseFee.
func (e *Estimator) Estimate(ctx context.Context, adapter chain.ChainAdapter, req chain.FeeEstimateReq) (*chain.FeeEstimate, error) {
	if req.Priority == "" {
		req.Priority = chain.PriorityStandard
	}
	switch e.strategy {
	case "eip1559_dynamic":
		return e.estimateEIP1559(ctx, adapter, req, false)
	case "eip1559_legacy_fallback":
		fe, err := e.estimateEIP1559(ctx, adapter, req, true)
		if err != nil {
			return adapter.EstimateFee(ctx, req)
		}
		return fe, nil
	case "legacy_only":
		return adapter.EstimateFee(ctx, req)
	case "solana_priority_fee", "bitcoin_rbf", "custom":
		return adapter.EstimateFee(ctx, req)
	}
	return adapter.EstimateFee(ctx, req)
}

func (e *Estimator) estimateEIP1559(_ context.Context, _ chain.ChainAdapter, req chain.FeeEstimateReq, allowFallback bool) (*chain.FeeEstimate, error) {
	e.mu.Lock()
	samples := append([]BlockSample(nil), e.samples...)
	e.mu.Unlock()
	if len(samples) == 0 {
		if allowFallback {
			return nil, errors.New("no samples; fallback to legacy")
		}
		return nil, errors.New("no block samples for eip1559 estimate")
	}
	latest := samples[len(samples)-1]
	if latest.BaseFee == nil {
		if allowFallback {
			return nil, errors.New("no base fee; fallback to legacy")
		}
		return nil, errors.New("missing base fee in latest sample")
	}
	pct := percentileForPriority(req.Priority)
	priority := percentilePriority(latest.PriorityFees, pct)
	if priority == nil {
		priority = big.NewInt(1_000_000_000) // 1 gwei default
	}
	maxFee := new(big.Int).Mul(latest.BaseFee, big.NewInt(2))
	maxFee.Add(maxFee, priority)
	total := new(big.Int).Mul(maxFee, new(big.Int).SetUint64(e.defaultGas))
	return &chain.FeeEstimate{
		ChainID:              e.chainID,
		Priority:             req.Priority,
		GasLimit:             e.defaultGas,
		MaxFeePerGas:         maxFee,
		MaxPriorityFeePerGas: priority,
		GasPrice:             nil,
		TotalFee:             total,
		Strategy:             e.strategy,
	}, nil
}

// percentileForPriority maps a priority tier to a percentile in [0,100].
// low = 10th, standard = 50th (median), high = 90th.
func percentileForPriority(p chain.Priority) float64 {
	switch p {
	case chain.PriorityLow:
		return 10
	case chain.PriorityHigh:
		return 90
	default:
		return 50
	}
}

// Percentile computes the p-th percentile (p in [0,100]) of xs using the
// nearest-rank method. Returns nil if xs is empty.
func Percentile(xs []*big.Int, p float64) *big.Int {
	return percentilePriority(xs, p)
}

// percentilePriority computes the nearest-rank percentile of a list of
// priority fees. The input slice is NOT mutated.
func percentilePriority(xs []*big.Int, p float64) *big.Int {
	if len(xs) == 0 {
		return nil
	}
	sorted := make([]*big.Int, len(xs))
	copy(sorted, xs)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Cmp(sorted[j]) < 0 })
	if p < 0 {
		p = 0
	}
	if p > 100 {
		p = 100
	}
	// Nearest-rank: index = ceil(p/100 * n) - 1, clamped to [0, n-1].
	n := len(sorted)
	idx := int((p/100.0)*float64(n) + 0.999999)
	if idx < 1 {
		idx = 1
	}
	if idx > n {
		idx = n
	}
	return new(big.Int).Set(sorted[idx-1])
}

// RecomputeLoop periodically refreshes estimates for the given priorities
// using the adapter and persists them to feeStore. It blocks until ctx is
// canceled.
func (e *Estimator) RecomputeLoop(ctx context.Context, adapter chain.ChainAdapter, feeStore FeeStoreAdapter, priorities []chain.Priority, interval time.Duration) {
	if interval <= 0 {
		interval = 15 * time.Second
	}
	if len(priorities) == 0 {
		priorities = []chain.Priority{chain.PriorityLow, chain.PriorityStandard, chain.PriorityHigh}
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, p := range priorities {
				fe, err := e.Estimate(ctx, adapter, chain.FeeEstimateReq{Priority: p})
				if err != nil || fe == nil {
					continue
				}
				if feeStore != nil {
					_ = feeStore.InsertFee(ctx, toRow(fe, 0, time.Now()))
				}
			}
		}
	}
}

func toRow(fe *chain.FeeEstimate, sampleCount int, ts time.Time) FeeRow {
	return FeeRow{
		ChainID:              fe.ChainID,
		Priority:             fe.Priority,
		GasLimit:             fe.GasLimit,
		MaxFeePerGas:         fe.MaxFeePerGas,
		MaxPriorityFeePerGas: fe.MaxPriorityFeePerGas,
		GasPrice:             fe.GasPrice,
		TotalFee:             fe.TotalFee,
		SampleCount:          sampleCount,
		Strategy:             fe.Strategy,
		ComputedAt:           ts,
	}
}

// FeeRow is the persistence shape used by RecomputeLoop. It mirrors
// store.FeeEstimateRow but avoids a circular import on the store package.
type FeeRow struct {
	ChainID              string
	Priority             chain.Priority
	GasLimit             uint64
	MaxFeePerGas         *big.Int
	MaxPriorityFeePerGas *big.Int
	GasPrice             *big.Int
	TotalFee             *big.Int
	SampleCount          int
	Strategy             string
	ComputedAt           time.Time
}

// FeeStoreAdapter is the subset of store.FeeStore used by the estimator to
// avoid a circular dependency.
type FeeStoreAdapter interface {
	InsertFee(ctx context.Context, r FeeRow) error
}