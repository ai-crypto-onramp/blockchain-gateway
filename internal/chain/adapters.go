// Package chain provides adapter scaffolds for EVM, Solana, and Bitcoin
// chains. These scaffolds issue real JSON-RPC calls but are intentionally
// thin; production hardening (batching, retries, provider failover) is
// layered on top by internal/provider. Unit tests use the stubAdapter, not
// these scaffolds, so no live node is required.
package chain

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"
)

// rpcClient is a minimal JSON-RPC client shared by the EVM/Solana/Bitcoin
// scaffolds. It does NOT implement failover — that is the job of
// internal/provider.ProviderPool, which wraps adapters. Keeping this here
// avoids importing the provider package from the chain package.
type rpcClient struct {
	urls    []string
	idx     int
	mu      sync.Mutex
	timeout time.Duration
}

func newRPCClient(urls []string, timeout time.Duration) *rpcClient {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &rpcClient{urls: urls, timeout: timeout}
}

type rpcRequest struct {
	JSONRPC string      `json:"jsonrpc"`
	Method  string      `json:"method"`
	Params  []any       `json:"params"`
	ID      int64       `json:"id"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int64           `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *rpcError       `json:"error"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (c *rpcClient) call(ctx context.Context, method string, params []any) (json.RawMessage, error) {
	if len(c.urls) == 0 {
		return nil, fmt.Errorf("no rpc urls configured")
	}
	c.mu.Lock()
	url := c.urls[c.idx%len(c.urls)]
	c.mu.Unlock()

	body, err := json.Marshal(rpcRequest{JSONRPC: "2.0", Method: method, Params: params, ID: 1})
	if err != nil {
		return nil, err
	}
	cctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(cctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("rpc http %d: %s", resp.StatusCode, string(raw))
	}
	var r rpcResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return nil, err
	}
	if r.Error != nil {
		return nil, fmt.Errorf("rpc error %d: %s", r.Error.Code, r.Error.Message)
	}
	return r.Result, nil
}

// --- EVM adapter ---

// EVMAdapter is a thin EVM JSON-RPC adapter scaffold. Tests use stubAdapter
// instead; this scaffold exists for production wiring and future expansion.
type EVMAdapter struct {
	cfg     ChainConfig
	rpc     *rpcClient
	chainID string
}

// NewEVMAdapter constructs an EVM adapter from cfg.
func NewEVMAdapter(cfg ChainConfig) *EVMAdapter {
	return &EVMAdapter{
		cfg:     cfg,
		rpc:     newRPCClient(cfg.RPCURLs, 5*time.Second),
		chainID: cfg.ChainID,
	}
}

func (e *EVMAdapter) ChainID() string       { return e.chainID }
func (e *EVMAdapter) FinalityBlocks() uint64 { return e.cfg.FinalityBlocks }

func (e *EVMAdapter) Broadcast(ctx context.Context, signedTx []byte) (string, error) {
	hexStr := "0x" + hex.EncodeToString(signedTx)
	res, err := e.rpc.call(ctx, "eth_sendRawTransaction", []any{hexStr})
	if err != nil {
		return "", err
	}
	var hash string
	if err := json.Unmarshal(res, &hash); err != nil {
		return "", err
	}
	return hash, nil
}

func (e *EVMAdapter) Height(ctx context.Context) (uint64, error) {
	res, err := e.rpc.call(ctx, "eth_blockNumber", nil)
	if err != nil {
		return 0, err
	}
	var hexStr string
	if err := json.Unmarshal(res, &hexStr); err != nil {
		return 0, err
	}
	return parseHexQuantity(hexStr)
}

func (e *EVMAdapter) Balance(ctx context.Context, addr string) (*big.Int, error) {
	res, err := e.rpc.call(ctx, "eth_getBalance", []any{addr, "latest"})
	if err != nil {
		return nil, err
	}
	var hexStr string
	if err := json.Unmarshal(res, &hexStr); err != nil {
		return nil, err
	}
	return parseHexBig(hexStr)
}

func (e *EVMAdapter) GetTx(ctx context.Context, txHash string) (*Tx, error) {
	res, err := e.rpc.call(ctx, "eth_getTransactionByHash", []any{txHash})
	if err != nil {
		return nil, err
	}
	if len(res) == 0 || string(res) == "null" {
		return nil, ErrTxNotFound
	}
	var raw struct {
		Hash      string `json:"hash"`
		From      string `json:"from"`
		To        string `json:"to"`
		Value     string `json:"value"`
		Nonce     string `json:"nonce"`
		BlockNum  string `json:"blockNumber"`
		BlockHash string `json:"blockHash"`
	}
	if err := json.Unmarshal(res, &raw); err != nil {
		return nil, err
	}
	if raw.Hash == "" {
		return nil, ErrTxNotFound
	}
	nonce, _ := parseHexQuantity(raw.Nonce)
	bh, _ := parseHexQuantity(raw.BlockNum)
	val, _ := parseHexBig(raw.Value)
	return &Tx{
		ChainID:     e.chainID,
		Hash:        raw.Hash,
		From:        raw.From,
		To:          raw.To,
		Value:       val,
		Nonce:       nonce,
		BlockHeight: bh,
		BlockHash:   raw.BlockHash,
		Status:      StatusMempool,
	}, nil
}

func (e *EVMAdapter) GetTxStatus(ctx context.Context, txHash string) (*TxStatus, error) {
	t, err := e.GetTx(ctx, txHash)
	if err != nil {
		return nil, err
	}
	st := &TxStatus{ChainID: t.ChainID, TxHash: t.Hash, BlockHeight: t.BlockHeight, BlockHash: t.BlockHash}
	if t.BlockHeight == 0 {
		st.Status = StatusMempool
		return st, nil
	}
	tip, err := e.Height(ctx)
	if err != nil {
		return nil, err
	}
	st.Confirmations = tip - t.BlockHeight + 1
	if st.Confirmations >= e.cfg.FinalityBlocks {
		st.Status = StatusFinalized
	} else {
		st.Status = StatusConfirmed
	}
	return st, nil
}

func (e *EVMAdapter) EstimateFee(ctx context.Context, req FeeEstimateReq) (*FeeEstimate, error) {
	// Delegate to internal/fee via the strategy encoded in cfg.GasStrategy.
	// The scaffold falls back to eth_gasPrice; the fee package wraps this
	// with EIP-1559 percentile math when invoked through the estimator.
	res, err := e.rpc.call(ctx, "eth_gasPrice", nil)
	if err != nil {
		return nil, err
	}
	var hexStr string
	if err := json.Unmarshal(res, &hexStr); err != nil {
		return nil, err
	}
	gp, err := parseHexBig(hexStr)
	if err != nil {
		return nil, err
	}
	if req.Priority == "" {
		req.Priority = PriorityStandard
	}
	multiplier := big.NewInt(1)
	switch req.Priority {
	case PriorityLow:
		multiplier = big.NewInt(90)
	case PriorityHigh:
		multiplier = big.NewInt(130)
	}
	gp.Mul(gp, multiplier).Div(gp, big.NewInt(100))
	return &FeeEstimate{
		ChainID:  e.chainID,
		Priority: req.Priority,
		GasLimit: 21000,
		GasPrice: gp,
		TotalFee: new(big.Int).Mul(gp, big.NewInt(21000)),
		Strategy: e.cfg.GasStrategy,
	}, nil
}

func (e *EVMAdapter) SubscribeHeads(ctx context.Context) (<-chan Head, func(), error) {
	out := make(chan Head, 16)
	// Scaffold: poll eth_blockNumber at 2s. Production replaces this with a
	// WebSocket newHeads subscription via internal/tip.
	go func() {
		defer close(out)
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		var lastHeight uint64
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				h, err := e.Height(ctx)
				if err != nil {
					continue
				}
				if h != lastHeight {
					lastHeight = h
					select {
					case out <- Head{ChainID: e.chainID, Height: h, Timestamp: time.Now()}:
					default:
					}
				}
			}
		}
	}()
	return out, func() {}, nil
}

func (e *EVMAdapter) SubscribeMempool(_ context.Context, _ []string) (<-chan MempoolEvent, func(), error) {
	// EVM mempool subscriptions require WebSocket + eth_newPendingTransactionFilter.
	// Scaffold returns an idle channel; internal/mempool layers real behavior.
	return make(chan MempoolEvent), func() {}, nil
}

// --- Solana adapter ---

// SolanaAdapter is a thin Solana JSON-RPC adapter scaffold.
type SolanaAdapter struct {
	cfg     ChainConfig
	rpc     *rpcClient
	chainID string
}

// NewSolanaAdapter constructs a Solana adapter from cfg.
func NewSolanaAdapter(cfg ChainConfig) *SolanaAdapter {
	return &SolanaAdapter{cfg: cfg, rpc: newRPCClient(cfg.RPCURLs, 5*time.Second), chainID: cfg.ChainID}
}

func (s *SolanaAdapter) ChainID() string        { return s.chainID }
func (s *SolanaAdapter) FinalityBlocks() uint64 { return s.cfg.FinalityBlocks }

func (s *SolanaAdapter) Broadcast(ctx context.Context, signedTx []byte) (string, error) {
	b64 := encodeBase64(signedTx)
	res, err := s.rpc.call(ctx, "sendTransaction", []any{b64, map[string]any{"encoding": "base64"}})
	if err != nil {
		return "", err
	}
	var hash string
	if err := json.Unmarshal(res, &hash); err != nil {
		return "", err
	}
	return hash, nil
}

func (s *SolanaAdapter) Height(ctx context.Context) (uint64, error) {
	res, err := s.rpc.call(ctx, "getSlot", []any{})
	if err != nil {
		return 0, err
	}
	var slot uint64
	if err := json.Unmarshal(res, &slot); err != nil {
		return 0, err
	}
	return slot, nil
}

func (s *SolanaAdapter) Balance(ctx context.Context, addr string) (*big.Int, error) {
	res, err := s.rpc.call(ctx, "getBalance", []any{addr})
	if err != nil {
		return nil, err
	}
	var resp struct {
		Value uint64 `json:"value"`
	}
	if err := json.Unmarshal(res, &resp); err != nil {
		return nil, err
	}
	return new(big.Int).SetUint64(resp.Value), nil
}

func (s *SolanaAdapter) GetTx(ctx context.Context, txHash string) (*Tx, error) {
	res, err := s.rpc.call(ctx, "getTransaction", []any{txHash, map[string]any{"encoding": "json"}})
	if err != nil {
		return nil, err
	}
	if len(res) == 0 || string(res) == "null" {
		return nil, ErrTxNotFound
	}
	var raw struct {
		Slot uint64 `json:"slot"`
	}
	if err := json.Unmarshal(res, &raw); err != nil {
		return nil, err
	}
	return &Tx{ChainID: s.chainID, Hash: txHash, BlockHeight: raw.Slot, Status: StatusConfirmed}, nil
}

func (s *SolanaAdapter) GetTxStatus(ctx context.Context, txHash string) (*TxStatus, error) {
	t, err := s.GetTx(ctx, txHash)
	if err != nil {
		return nil, err
	}
	return &TxStatus{ChainID: t.ChainID, TxHash: t.Hash, Status: t.Status, BlockHeight: t.BlockHeight, Confirmations: 1}, nil
}

func (s *SolanaAdapter) EstimateFee(_ context.Context, req FeeEstimateReq) (*FeeEstimate, error) {
	if req.Priority == "" {
		req.Priority = PriorityStandard
	}
	mult := big.NewInt(5000)
	switch req.Priority {
	case PriorityLow:
		mult = big.NewInt(2500)
	case PriorityHigh:
		mult = big.NewInt(10000)
	}
	return &FeeEstimate{ChainID: s.chainID, Priority: req.Priority, GasPrice: mult, TotalFee: mult, Strategy: s.cfg.GasStrategy}, nil
}

func (s *SolanaAdapter) SubscribeHeads(ctx context.Context) (<-chan Head, func(), error) {
	out := make(chan Head, 16)
	go func() {
		defer close(out)
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		var last uint64
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				h, err := s.Height(ctx)
				if err != nil {
					continue
				}
				if h != last {
					last = h
					select {
					case out <- Head{ChainID: s.chainID, Height: h, Timestamp: time.Now()}:
					default:
					}
				}
			}
		}
	}()
	return out, func() {}, nil
}

func (s *SolanaAdapter) SubscribeMempool(_ context.Context, _ []string) (<-chan MempoolEvent, func(), error) {
	return make(chan MempoolEvent), func() {}, nil
}

// --- Bitcoin adapter ---

// BitcoinAdapter is a thin Bitcoin JSON-RPC adapter scaffold (bitcoind).
type BitcoinAdapter struct {
	cfg     ChainConfig
	rpc     *rpcClient
	chainID string
}

// NewBitcoinAdapter constructs a Bitcoin adapter from cfg.
func NewBitcoinAdapter(cfg ChainConfig) *BitcoinAdapter {
	return &BitcoinAdapter{cfg: cfg, rpc: newRPCClient(cfg.RPCURLs, 5*time.Second), chainID: cfg.ChainID}
}

func (b *BitcoinAdapter) ChainID() string        { return b.chainID }
func (b *BitcoinAdapter) FinalityBlocks() uint64 { return b.cfg.FinalityBlocks }

func (b *BitcoinAdapter) Broadcast(ctx context.Context, signedTx []byte) (string, error) {
	hexStr := hex.EncodeToString(signedTx)
	res, err := b.rpc.call(ctx, "sendrawtransaction", []any{hexStr})
	if err != nil {
		return "", err
	}
	var hash string
	if err := json.Unmarshal(res, &hash); err != nil {
		return "", err
	}
	return hash, nil
}

func (b *BitcoinAdapter) Height(ctx context.Context) (uint64, error) {
	res, err := b.rpc.call(ctx, "getblockcount", nil)
	if err != nil {
		return 0, err
	}
	var h uint64
	if err := json.Unmarshal(res, &h); err != nil {
		return 0, err
	}
	return h, nil
}

func (b *BitcoinAdapter) Balance(ctx context.Context, addr string) (*big.Int, error) {
	res, err := b.rpc.call(ctx, "getreceivedbyaddress", []any{addr, 1})
	if err != nil {
		return nil, err
	}
	var v float64
	if err := json.Unmarshal(res, &v); err != nil {
		return nil, err
	}
	sat := new(big.Int).SetUint64(uint64(v * 1e8))
	return sat, nil
}

func (b *BitcoinAdapter) GetTx(_ context.Context, txHash string) (*Tx, error) {
	return &Tx{ChainID: b.chainID, Hash: txHash, Status: StatusMempool}, nil
}

func (b *BitcoinAdapter) GetTxStatus(_ context.Context, txHash string) (*TxStatus, error) {
	return &TxStatus{ChainID: b.chainID, TxHash: txHash, Status: StatusMempool, Confirmations: 0}, nil
}

func (b *BitcoinAdapter) EstimateFee(ctx context.Context, req FeeEstimateReq) (*FeeEstimate, error) {
	res, err := b.rpc.call(ctx, "estimateSmartFee", []any{b.cfg.FinalityBlocks, "CONSERVATIVE"})
	if err != nil {
		return nil, err
	}
	var resp struct {
		FeeRate float64 `json:"feerate"`
	}
	if err := json.Unmarshal(res, &resp); err != nil {
		return nil, err
	}
	if req.Priority == "" {
		req.Priority = PriorityStandard
	}
	mult := 1.0
	switch req.Priority {
	case PriorityLow:
		mult = 0.8
	case PriorityHigh:
		mult = 1.5
	}
	rate := resp.FeeRate * mult
	satPerByte := rate * 1e5 // BTC/kB -> sat/byte (approx)
	gp := new(big.Int).SetUint64(uint64(satPerByte))
	return &FeeEstimate{ChainID: b.chainID, Priority: req.Priority, GasPrice: gp, TotalFee: new(big.Int).Mul(gp, big.NewInt(250)), Strategy: b.cfg.GasStrategy}, nil
}

func (b *BitcoinAdapter) SubscribeHeads(ctx context.Context) (<-chan Head, func(), error) {
	out := make(chan Head, 16)
	go func() {
		defer close(out)
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		var last uint64
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				h, err := b.Height(ctx)
				if err != nil {
					continue
				}
				if h != last {
					last = h
					select {
					case out <- Head{ChainID: b.chainID, Height: h, Timestamp: time.Now()}:
					default:
					}
				}
			}
		}
	}()
	return out, func() {}, nil
}

func (b *BitcoinAdapter) SubscribeMempool(_ context.Context, _ []string) (<-chan MempoolEvent, func(), error) {
	return make(chan MempoolEvent), func() {}, nil
}

// --- helpers ---

func parseHexQuantity(s string) (uint64, error) {
	s = strings.TrimPrefix(s, "0x")
	if s == "" {
		return 0, nil
	}
	var v uint64
	_, err := fmt.Sscanf(s, "%x", &v)
	return v, err
}

func parseHexBig(s string) (*big.Int, error) {
	s = strings.TrimPrefix(s, "0x")
	if s == "" {
		return big.NewInt(0), nil
	}
	n, ok := new(big.Int).SetString(s, 16)
	if !ok {
		return nil, fmt.Errorf("bad hex big: %s", s)
	}
	return n, nil
}

func encodeBase64(b []byte) string {
	const tbl = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	out := make([]byte, 0, ((len(b)+2)/3)*4)
	for i := 0; i < len(b); i += 3 {
		end := i + 3
		if end > len(b) {
			end = len(b)
		}
		chunk := b[i:end]
		var n uint
		for j := 0; j < 3; j++ {
			n <<= 8
			if j < len(chunk) {
				n |= uint(chunk[j])
			}
		}
		out = append(out, tbl[(n>>18)&0x3f], tbl[(n>>12)&0x3f], tbl[(n>>6)&0x3f], tbl[n&0x3f])
	}
	pad := (3 - len(b)%3) % 3
	for i := 0; i < pad; i++ {
		out[len(out)-1-i] = '='
	}
	return string(out)
}