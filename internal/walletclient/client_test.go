package walletclient

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestMockFundSenderSuccess(t *testing.T) {
	m := NewMock("0xfunding", 5)
	resp, err := m.FundSender(context.Background(), FundSenderRequest{ChainID: "ethereum", Addr: "0x1", Amount: "100"})
	if err != nil || !resp.Ok || resp.FundingTx != "0xfunding" {
		t.Fatalf("fund: %+v %v", resp, err)
	}
	if len(m.FundRequests) != 1 {
		t.Errorf("fund requests: %d", len(m.FundRequests))
	}
}

func TestMockAllocateNonceIncrements(t *testing.T) {
	m := NewMock("0xfunding", 0)
	r1, _ := m.AllocateNonce(context.Background(), "ethereum", "0x1")
	r2, _ := m.AllocateNonce(context.Background(), "ethereum", "0x1")
	if r1.Nonce != 0 || r2.Nonce != 1 {
		t.Errorf("nonces: %d %d", r1.Nonce, r2.Nonce)
	}
}

func TestMockFundErr(t *testing.T) {
	m := NewMock("0xfunding", 0)
	m.FundErr = errors.New("boom")
	_, err := m.FundSender(context.Background(), FundSenderRequest{ChainID: "ethereum"})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestMockFailNthFund(t *testing.T) {
	m := NewMock("0xfunding", 0)
	m.FailNthFund = 2
	_, err1 := m.FundSender(context.Background(), FundSenderRequest{ChainID: "ethereum"})
	_, err2 := m.FundSender(context.Background(), FundSenderRequest{ChainID: "ethereum"})
	if err1 != nil {
		t.Errorf("first call should succeed: %v", err1)
	}
	if err2 == nil {
		t.Fatal("second call should fail")
	}
	if m.FundCalls() != 2 {
		t.Errorf("calls: %d", m.FundCalls())
	}
}

func TestHTTPClientFundSender(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/fund-sender" {
			t.Errorf("path: %s", r.URL.Path)
		}
		var req FundSenderRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		_ = io.Discard
		_ = json.NewEncoder(w).Encode(FundSenderResponse{Ok: true, FundingTx: "0xfunding"})
	}))
	defer srv.Close()
	c := NewHTTPClient(srv.URL, 2*time.Second)
	resp, err := c.FundSender(context.Background(), FundSenderRequest{ChainID: "ethereum", Addr: "0x1", Amount: "100"})
	if err != nil || !resp.Ok || resp.FundingTx != "0xfunding" {
		t.Fatalf("fund: %+v %v", resp, err)
	}
}

func TestHTTPClientAllocateNonce(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/nonce/allocate" {
			t.Errorf("path: %s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(AllocateNonceResponse{Nonce: 42})
	}))
	defer srv.Close()
	c := NewHTTPClient(srv.URL, 2*time.Second)
	resp, err := c.AllocateNonce(context.Background(), "ethereum", "0x1")
	if err != nil || resp.Nonce != 42 {
		t.Fatalf("nonce: %+v %v", resp, err)
	}
}

func TestHTTPClientErrorStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	c := NewHTTPClient(srv.URL, 2*time.Second)
	_, err := c.FundSender(context.Background(), FundSenderRequest{})
	if err == nil {
		t.Fatal("expected error on 500")
	}
}

func TestHTTPClientTransportError(t *testing.T) {
	c := NewHTTPClient("http://127.0.0.1:0", 100*time.Millisecond)
	_, err := c.FundSender(context.Background(), FundSenderRequest{})
	if err == nil {
		t.Fatal("expected transport error")
	}
}

func TestHTTPClientMalformedJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("not json"))
	}))
	defer srv.Close()
	c := NewHTTPClient(srv.URL, 2*time.Second)
	_, err := c.FundSender(context.Background(), FundSenderRequest{})
	if err == nil {
		t.Fatal("expected decode error")
	}
}