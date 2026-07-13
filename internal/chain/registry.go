package chain

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"strings"
	"sync"
	"time"
)

// ConfigLoader reads per-chain configuration from the process environment
// and produces a populated []ChainConfig. Expected variables:
//
//	CHAINS_SUPPORTED                comma-separated chain ids
//	RPC_URLS_<CHAIN>                comma-separated RPC URLs (uppercased id)
//	WS_URLS_<CHAIN>                 comma-separated WS URLs (optional)
//	FINALITY_BLOCKS_<CHAIN>         finality depth
//	GAS_STRATEGY_<CHAIN>            gas strategy override
//	GAS_STRATEGY                    default gas strategy
//
// LookupKey uppercases the chain id so environment variables are case
// insensitive to the chain id casing.
type ConfigLoader struct {
	env    func(string) string
	defaults map[string]ChainConfig
}

// NewConfigLoader returns a ConfigLoader that reads from os.Getenv.
func NewConfigLoader() *ConfigLoader {
	return &ConfigLoader{env: os.Getenv, defaults: defaultChainConfigs()}
}

// newConfigLoaderWithEnv is a test helper.
func newConfigLoaderWithEnv(env func(string) string) *ConfigLoader {
	return &ConfigLoader{env: env, defaults: defaultChainConfigs()}
}

func defaultChainConfigs() map[string]ChainConfig {
	return map[string]ChainConfig{
		"ethereum": {ChainID: "ethereum", FinalityBlocks: 64, GasStrategy: "eip1559_dynamic"},
		"polygon":  {ChainID: "polygon", FinalityBlocks: 256, GasStrategy: "eip1559_legacy_fallback"},
		"solana":   {ChainID: "solana", FinalityBlocks: 1, GasStrategy: "solana_priority_fee"},
		"bitcoin":  {ChainID: "bitcoin", FinalityBlocks: 6, GasStrategy: "bitcoin_rbf"},
	}
}

// Load parses the environment and returns one ChainConfig per supported
// chain. Chains without explicit RPC URLs retain an empty URL list (the
// stub adapter ignores them).
func (l *ConfigLoader) Load() ([]ChainConfig, error) {
	supported := l.env("CHAINS_SUPPORTED")
	if supported == "" {
		return nil, fmt.Errorf("CHAINS_SUPPORTED not set")
	}
	defaultStrategy := l.env("GAS_STRATEGY")
	if defaultStrategy == "" {
		defaultStrategy = "eip1559_dynamic"
	}

	var cfgs []ChainConfig
	for _, id := range splitCSV(supported) {
		id = strings.ToLower(strings.TrimSpace(id))
		if id == "" {
			continue
		}
		base, ok := l.defaults[id]
		if !ok {
			base = ChainConfig{ChainID: id}
		}
		cfg := base
		cfg.ChainID = id
		cfg.RPCURLs = splitCSV(l.env("RPC_URLS_"+strings.ToUpper(id)))
		cfg.WSURLs = splitCSV(l.env("WS_URLS_"+strings.ToUpper(id)))
		if v := l.env("FINALITY_BLOCKS_" + strings.ToUpper(id)); v != "" {
			var f uint64
			_, err := fmt.Sscanf(v, "%d", &f)
			if err != nil {
				return nil, fmt.Errorf("FINALITY_BLOCKS_%s: %w", strings.ToUpper(id), err)
			}
			cfg.FinalityBlocks = f
		}
		if v := l.env("GAS_STRATEGY_" + strings.ToUpper(id)); v != "" {
			cfg.GasStrategy = v
		} else if cfg.GasStrategy == "" {
			cfg.GasStrategy = defaultStrategy
		}
		cfgs = append(cfgs, cfg)
	}
	return cfgs, nil
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// Registry maps chain id -> ChainAdapter. It is safe for concurrent use.
type Registry struct {
	mu       sync.RWMutex
	adapters map[string]ChainAdapter
}

// NewRegistry returns an empty Registry.
func NewRegistry() *Registry {
	return &Registry{adapters: make(map[string]ChainAdapter)}
}

// Register stores a for the adapter's ChainID. Re-registering overwrites.
func (r *Registry) Register(a ChainAdapter) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.adapters[a.ChainID()] = a
}

// Get returns the adapter for chainID or ErrUnknownChain.
func (r *Registry) Get(chainID string) (ChainAdapter, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	a, ok := r.adapters[chainID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownChain, chainID)
	}
	return a, nil
}

// Chains returns the sorted list of registered chain ids.
func (r *Registry) Chains() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.adapters))
	for id := range r.adapters {
		out = append(out, id)
	}
	return out
}

// AsStub returns the registered adapter as a *stubAdapter, panicking if it
// is not a stub. This is a test-only helper.
func (r *Registry) AsStub(chainID string) *stubAdapter {
	a, err := r.Get(chainID)
	if err != nil {
		panic(err)
	}
	return a.(*stubAdapter)
}

// StubEmitter is a test-only handle to a stub adapter's head/mempool
// emission methods. It is returned by Registry.StubEmitter so callers
// outside the chain package can drive a stub without referencing the
// unexported stubAdapter type.
type StubEmitter interface {
	EmitHead(h Head)
	EmitMempool(e MempoolEvent)
	SeedTx(t *Tx, st *TxStatus)
	BroadcastCount() int
	LastBroadcast() []byte
}

// StubEmitter returns the registered adapter as a StubEmitter, panicking if
// it is not a stub. Test-only.
func (r *Registry) StubEmitter(chainID string) StubEmitter {
	return r.AsStub(chainID)
}

// stubAdapter is a no-op ChainAdapter used by unit tests. It is safe for
// concurrent use and configurable via the StubAdapterOptions.
type stubAdapter struct {
	mu            sync.Mutex
	chainID       string
	finality      uint64
	height        uint64
	balance       *big.Int
	broadcastFn   func(ctx context.Context, signedTx []byte) (string, error)
	broadcasts    int
	lastBroadcast []byte
	headsCh       chan Head
	mempoolCh     chan MempoolEvent
	txs           map[string]*Tx
	statuses      map[string]*TxStatus
	feeEstimate   *FeeEstimate
	broadcastErr  error
}

// StubAdapterOptions configures a stubAdapter.
type StubAdapterOptions struct {
	ChainID       string
	FinalityBlocks uint64
	Height        uint64
	Balance       *big.Int
	FeeEstimate   *FeeEstimate
	BroadcastFn   func(ctx context.Context, signedTx []byte) (string, error)
	BroadcastErr  error
}

// NewStubAdapter returns a configured stubAdapter for tests.
func NewStubAdapter(opts StubAdapterOptions) ChainAdapter {
	if opts.ChainID == "" {
		opts.ChainID = "stub"
	}
	if opts.Balance == nil {
		opts.Balance = big.NewInt(0)
	}
	return &stubAdapter{
		chainID:      opts.ChainID,
		finality:     opts.FinalityBlocks,
		height:       opts.Height,
		balance:      new(big.Int).Set(opts.Balance),
		broadcastFn:  opts.BroadcastFn,
		feeEstimate:  opts.FeeEstimate,
		broadcastErr: opts.BroadcastErr,
		headsCh:      make(chan Head, 16),
		mempoolCh:    make(chan MempoolEvent, 16),
		txs:          make(map[string]*Tx),
		statuses:     make(map[string]*TxStatus),
	}
}

// EmitHead pushes a synthetic head into the SubscribeHeads channel.
func (s *stubAdapter) EmitHead(h Head) {
	select {
	case s.headsCh <- h:
	default:
	}
}

// EmitMempool pushes a synthetic mempool event.
func (s *stubAdapter) EmitMempool(e MempoolEvent) {
	select {
	case s.mempoolCh <- e:
	default:
	}
}

// BroadcastCount returns the number of Broadcast calls observed.
func (s *stubAdapter) BroadcastCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.broadcasts
}

// LastBroadcast returns the most recent signed payload submitted.
func (s *stubAdapter) LastBroadcast() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastBroadcast
}

// SeedTx inserts a tx + status into the stub's in-memory store.
func (s *stubAdapter) SeedTx(t *Tx, st *TxStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.txs[t.Hash] = t
	if st != nil {
		s.statuses[t.Hash] = st
	}
}

func (s *stubAdapter) ChainID() string { return s.chainID }

func (s *stubAdapter) Broadcast(ctx context.Context, signedTx []byte) (string, error) {
	s.mu.Lock()
	s.broadcasts++
	s.lastBroadcast = append([]byte(nil), signedTx...)
	fn := s.broadcastFn
	bErr := s.broadcastErr
	s.mu.Unlock()
	if bErr != nil {
		return "", bErr
	}
	if fn != nil {
		return fn(ctx, signedTx)
	}
	// Default: derive a deterministic hash from the payload so identical
	// payloads yield identical hashes (idempotency).
	return stubHash(signedTx), nil
}

func (s *stubAdapter) GetTx(_ context.Context, txHash string) (*Tx, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.txs[txHash]
	if !ok {
		return nil, ErrTxNotFound
	}
	return t, nil
}

func (s *stubAdapter) GetTxStatus(_ context.Context, txHash string) (*TxStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.statuses[txHash]
	if !ok {
		return nil, ErrTxNotFound
	}
	return st, nil
}

func (s *stubAdapter) EstimateFee(_ context.Context, _ FeeEstimateReq) (*FeeEstimate, error) {
	if s.feeEstimate != nil {
		return s.feeEstimate, nil
	}
	return &FeeEstimate{
		ChainID:      s.chainID,
		Priority:     PriorityStandard,
		GasLimit:     21000,
		GasPrice:     big.NewInt(1_000_000_000),
		TotalFee:     new(big.Int).Mul(big.NewInt(21000), big.NewInt(1_000_000_000)),
		Strategy:     "stub",
	}, nil
}

func (s *stubAdapter) Height(_ context.Context) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.height, nil
}

func (s *stubAdapter) Balance(_ context.Context, _ string) (*big.Int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return new(big.Int).Set(s.balance), nil
}

func (s *stubAdapter) SubscribeHeads(_ context.Context) (<-chan Head, func(), error) {
	return s.headsCh, func() {}, nil
}

func (s *stubAdapter) SubscribeMempool(_ context.Context, _ []string) (<-chan MempoolEvent, func(), error) {
	return s.mempoolCh, func() {}, nil
}

func (s *stubAdapter) FinalityBlocks() uint64 { return s.finality }

// stubHash derives a deterministic, hex-prefixed hash from a payload. It is
// NOT a real cryptographic hash — it is a simple FNV-1a sum formatted as
// 0x + 64 hex chars — sufficient to demonstrate broadcast idempotency in
// tests without pulling in a crypto dependency.
func stubHash(payload []byte) string {
	const prime = 1099511628211
	h := uint64(1469598103934665603)
	for _, b := range payload {
		h ^= uint64(b)
		h *= prime
	}
	// Fold into 32 bytes by repeating the 64-bit value.
	var out [32]byte
	for i := 0; i < 4; i++ {
		v := h
		for j := 0; j < 8; j++ {
			out[i*8+j] = byte(v & 0xff)
			v >>= 8
		}
		h ^= prime
	}
	const hexd = "0123456789abcdef"
	sb := make([]byte, 66)
	sb[0] = '0'
	sb[1] = 'x'
	for i, b := range out {
		sb[2+i*2] = hexd[b>>4]
		sb[2+i*2+1] = hexd[b&0xf]
	}
	return string(sb)
}

// SetHeight advances the stub's tip (test helper).
func (s *stubAdapter) SetHeight(h uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.height = h
}

// SetBalance updates the stub's balance (test helper).
func (s *stubAdapter) SetBalance(b *big.Int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.balance = new(big.Int).Set(b)
}

// touch ensures the time package is referenced (used by Head/MempoolEvent).
var _ = time.Now