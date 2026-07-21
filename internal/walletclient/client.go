// Package walletclient provides a client to the Wallet Management service.
// To avoid a hard dependency on protoc/protoc-gen-go being installed at
// build time, the package defines a Client interface plus a hand-written
// HTTP/JSON implementation that talks to the REST surface exposed by
// wallet-management (POST /v1/wallets/{id}/funding-request and
// POST /v1/wallets/{id}/nonce/allocate). A Mock implementation is provided
// for unit tests.
//
// If a real gRPC client is later required, implement Client against the
// generated stubs — no caller changes are needed.
package walletclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// FundSenderRequest asks Wallet Management to fund the wallet with amount
// base units of the chain's native asset for gas. WalletID identifies the
// hot wallet in wallet-management; Addr is kept for logging/dedup.
type FundSenderRequest struct {
	WalletID string `json:"wallet_id"`
	ChainID  string `json:"chain_id"`
	Addr     string `json:"addr"`
	Amount   string `json:"amount"` // decimal string (wei/lamports/sats)
}

// FundSenderResponse carries the funding tx hash and a confirmation flag.
// When wallet-management accepts the funding request asynchronously it
// returns Ok=true with an empty FundingTx (the treasury funds the wallet
// out-of-band); the blockchain-gateway then polls the sender's balance
// via the chain adapter.
type FundSenderResponse struct {
	Ok        bool   `json:"ok"`
	FundingTx string `json:"funding_tx"`
	Error     string `json:"error,omitempty"`
}

// AllocateNonceResponse returns the next nonce to use for the wallet on
// the given chain.
type AllocateNonceResponse struct {
	Nonce uint64 `json:"nonce"`
}

// Client is the wallet-management surface used by the gateway.
type Client interface {
	FundSender(ctx context.Context, req FundSenderRequest) (*FundSenderResponse, error)
	AllocateNonce(ctx context.Context, walletID, chainID string) (*AllocateNonceResponse, error)
}

// HTTPClient implements Client over the wallet-management REST/JSON
// surface. It is the production default when gRPC codegen is unavailable.
type HTTPClient struct {
	baseURL string
	http    *http.Client
}

// NewHTTPClient returns an HTTPClient for the given base URL.
func NewHTTPClient(baseURL string, timeout time.Duration) *HTTPClient {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &HTTPClient{baseURL: baseURL, http: &http.Client{Timeout: timeout}}
}

// nativeAsset maps a chain id to its native asset symbol used by
// wallet-management's funding-request endpoint. Unknown chains fall back
// to the chain id itself (wallet-management stores the asset verbatim).
func nativeAsset(chainID string) string {
	switch chainID {
	case "ethereum", "polygon", "arbitrum", "base", "optimism":
		return "eth"
	case "bitcoin":
		return "btc"
	case "solana":
		return "sol"
	}
	return chainID
}

// fundingRequestBody is the JSON shape expected by wallet-management's
// POST /v1/wallets/{id}/funding-request endpoint.
type fundingRequestBody struct {
	Asset  string `json:"asset"`
	Amount string `json:"amount"`
	Reason string `json:"reason"`
}

// fundingResponseBody is the JSON shape returned by wallet-management's
// POST /v1/wallets/{id}/funding-request endpoint.
type fundingResponseBody struct {
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

// nonceRequestBody is the JSON shape expected by wallet-management's
// POST /v1/wallets/{id}/nonce/allocate endpoint.
type nonceRequestBody struct {
	Chain string `json:"chain"`
}

func (c *HTTPClient) FundSender(ctx context.Context, req FundSenderRequest) (*FundSenderResponse, error) {
	if req.WalletID == "" {
		return &FundSenderResponse{Ok: false, Error: "wallet_id is required"}, errors.New("fund sender: wallet_id is required")
	}
	url := c.baseURL + "/v1/wallets/" + req.WalletID + "/funding-request"
	body := fundingRequestBody{
		Asset:  nativeAsset(req.ChainID),
		Amount: req.Amount,
		Reason: "blockchain-gateway gas prepayment",
	}
	raw, err := doJSON[fundingResponseBody](ctx, c.http, url, body)
	if err != nil {
		return &FundSenderResponse{Ok: false, Error: err.Error()}, err
	}
	if raw.Error != "" {
		return &FundSenderResponse{Ok: false, Error: raw.Error}, fmt.Errorf("%s", raw.Error)
	}
	// wallet-management acknowledges the request asynchronously; the
	// funding tx hash is not known at this point (treasury settles out of
	// band). The prepayment manager polls the sender's on-chain balance
	// to confirm funds landed.
	return &FundSenderResponse{Ok: raw.Status == "requested" || raw.Status == "approved" || raw.Status == ""}, nil
}

func (c *HTTPClient) AllocateNonce(ctx context.Context, walletID, chainID string) (*AllocateNonceResponse, error) {
	if walletID == "" {
		return nil, errors.New("allocate nonce: wallet_id is required")
	}
	url := c.baseURL + "/v1/wallets/" + walletID + "/nonce/allocate"
	body := nonceRequestBody{Chain: chainID}
	resp, err := doJSON[AllocateNonceResponse](ctx, c.http, url, body)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func doJSON[T any](ctx context.Context, c *http.Client, url string, body any) (*T, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 400 {
		txt, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("wallet-mgmt http %d: %s", resp.StatusCode, string(txt))
	}
	var out T
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Mock is an in-memory Client for unit tests. It is safe for concurrent
// use.
type Mock struct {
	mu            sync.Mutex
	FundErr       error
	NonceErr      error
	FundingTx     string
	NextNonce     uint64
	FundRequests  []FundSenderRequest
	NonceRequests []struct{ WalletID, ChainID string }
	FailNthFund   int // 1-based; if >0, the Nth FundSender call fails once
	fundCallCount int
}

// NewMock returns a Mock that succeeds with the given funding tx hash and
// nonce.
func NewMock(fundingTx string, nextNonce uint64) *Mock {
	return &Mock{FundingTx: fundingTx, NextNonce: nextNonce}
}

func (m *Mock) FundSender(_ context.Context, req FundSenderRequest) (*FundSenderResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.FundRequests = append(m.FundRequests, req)
	m.fundCallCount++
	if m.FailNthFund > 0 && m.fundCallCount == m.FailNthFund {
		return &FundSenderResponse{Ok: false, Error: "transient"}, errors.New("transient fund error")
	}
	if m.FundErr != nil {
		return &FundSenderResponse{Ok: false, Error: m.FundErr.Error()}, m.FundErr
	}
	return &FundSenderResponse{Ok: true, FundingTx: m.FundingTx}, nil
}

func (m *Mock) AllocateNonce(_ context.Context, walletID, chainID string) (*AllocateNonceResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.NonceRequests = append(m.NonceRequests, struct{ WalletID, ChainID string }{walletID, chainID})
	if m.NonceErr != nil {
		return nil, m.NonceErr
	}
	n := m.NextNonce
	m.NextNonce++
	return &AllocateNonceResponse{Nonce: n}, nil
}

// FundCalls returns the number of FundSender calls observed.
func (m *Mock) FundCalls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.fundCallCount
}
