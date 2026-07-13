// Package walletclient provides a client to the Wallet Management service.
// To avoid a hard dependency on protoc/protoc-gen-go being installed at
// build time, the package defines a Client interface plus a hand-written
// HTTP/JSON implementation that talks to a small REST surface exposed by
// wallet-management. A Mock implementation is provided for unit tests.
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

// FundSenderRequest asks Wallet Management to fund addr with amount base
// units of the chain's native asset for gas.
type FundSenderRequest struct {
	ChainID string `json:"chain_id"`
	Addr    string `json:"addr"`
	Amount  string `json:"amount"` // decimal string (wei/lamports/sats)
}

// FundSenderResponse carries the funding tx hash and a confirmation flag.
type FundSenderResponse struct {
	Ok         bool   `json:"ok"`
	FundingTx  string `json:"funding_tx"`
	Error      string `json:"error,omitempty"`
}

// AllocateNonceResponse returns the next nonce to use for addr on chain.
type AllocateNonceResponse struct {
	Nonce uint64 `json:"nonce"`
}

// Client is the wallet-management surface used by the gateway.
type Client interface {
	FundSender(ctx context.Context, req FundSenderRequest) (*FundSenderResponse, error)
	AllocateNonce(ctx context.Context, chainID, addr string) (*AllocateNonceResponse, error)
}

// HTTPClient implements Client over a small REST/JSON surface. It is the
// production default when gRPC codegen is unavailable.
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

func (c *HTTPClient) FundSender(ctx context.Context, req FundSenderRequest) (*FundSenderResponse, error) {
	return doJSON[FundSenderResponse](ctx, c.http, c.baseURL+"/v1/fund-sender", req)
}

func (c *HTTPClient) AllocateNonce(ctx context.Context, chainID, addr string) (*AllocateNonceResponse, error) {
	return doJSON[AllocateNonceResponse](ctx, c.http, c.baseURL+"/v1/nonce/allocate", map[string]string{
		"chain_id": chainID,
		"addr":     addr,
	})
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
	defer resp.Body.Close()
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
	mu              sync.Mutex
	FundErr         error
	NonceErr        error
	FundingTx       string
	NextNonce       uint64
	FundRequests    []FundSenderRequest
	NonceRequests   []struct{ ChainID, Addr string }
	FailNthFund     int // 1-based; if >0, the Nth FundSender call fails once
	fundCallCount   int
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

func (m *Mock) AllocateNonce(_ context.Context, chainID, addr string) (*AllocateNonceResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.NonceRequests = append(m.NonceRequests, struct{ ChainID, Addr string }{chainID, addr})
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