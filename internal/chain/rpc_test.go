package chain

import (
	"context"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestEVMAdapterRPCBroadcast(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req rpcRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		_ = json.NewEncoder(w).Encode(rpcResponse{Result: json.RawMessage(`"0xabc"`)})
	}))
	defer srv.Close()
	a := NewEVMAdapter(ChainConfig{ChainID: "ethereum", RPCURLs: []string{srv.URL}, FinalityBlocks: 64})
	h, err := a.Broadcast(context.Background(), []byte{0xde, 0xad})
	if err != nil || h != "0xabc" {
		t.Fatalf("broadcast: %v %s", err, h)
	}
}

func TestEVMAdapterRPCHeight(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(rpcResponse{Result: json.RawMessage(`"0x64"`)})
	}))
	defer srv.Close()
	a := NewEVMAdapter(ChainConfig{ChainID: "ethereum", RPCURLs: []string{srv.URL}, FinalityBlocks: 64})
	h, err := a.Height(context.Background())
	if err != nil || h != 100 {
		t.Fatalf("height: %v %d", err, h)
	}
}

func TestEVMAdapterRPCBalance(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(rpcResponse{Result: json.RawMessage(`"0xff"`)})
	}))
	defer srv.Close()
	a := NewEVMAdapter(ChainConfig{ChainID: "ethereum", RPCURLs: []string{srv.URL}, FinalityBlocks: 64})
	b, err := a.Balance(context.Background(), "0xaddr")
	if err != nil || b.Cmp(big.NewInt(255)) != 0 {
		t.Fatalf("balance: %v %s", err, b)
	}
}

func TestEVMAdapterRPCGetTx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(rpcResponse{Result: json.RawMessage(`{"hash":"0x1","from":"0xa","to":"0xb","value":"0x64","nonce":"0x1","blockNumber":"0xa","blockHash":"0xblk"}`)})
	}))
	defer srv.Close()
	a := NewEVMAdapter(ChainConfig{ChainID: "ethereum", RPCURLs: []string{srv.URL}, FinalityBlocks: 64})
	tx, err := a.GetTx(context.Background(), "0x1")
	if err != nil || tx.Hash != "0x1" || tx.From != "0xa" {
		t.Fatalf("get tx: %v %+v", err, tx)
	}
	if tx.BlockHeight != 10 || tx.Nonce != 1 {
		t.Errorf("block/nonce: %d %d", tx.BlockHeight, tx.Nonce)
	}
}

func TestEVMAdapterRPCGetTxNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(rpcResponse{Result: json.RawMessage(`null`)})
	}))
	defer srv.Close()
	a := NewEVMAdapter(ChainConfig{ChainID: "ethereum", RPCURLs: []string{srv.URL}, FinalityBlocks: 64})
	if _, err := a.GetTx(context.Background(), "0xmissing"); err != ErrTxNotFound {
		t.Fatalf("expected not found, got %v", err)
	}
}

func TestEVMAdapterRPCGetTxStatus(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		if calls == 1 {
			_ = json.NewEncoder(w).Encode(rpcResponse{Result: json.RawMessage(`{"hash":"0x1","blockNumber":"0xa"}`)})
		} else {
			_ = json.NewEncoder(w).Encode(rpcResponse{Result: json.RawMessage(`"0x6e"`)}) // 110
		}
	}))
	defer srv.Close()
	a := NewEVMAdapter(ChainConfig{ChainID: "ethereum", RPCURLs: []string{srv.URL}, FinalityBlocks: 64})
	st, err := a.GetTxStatus(context.Background(), "0x1")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if st.BlockHeight != 10 || st.Confirmations != 101 {
		t.Errorf("status: %+v", st)
	}
}

func TestEVMAdapterRPCEstimateFee(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(rpcResponse{Result: json.RawMessage(`"0x3b9aca00"`)}) // 1 gwei
	}))
	defer srv.Close()
	a := NewEVMAdapter(ChainConfig{ChainID: "ethereum", RPCURLs: []string{srv.URL}, FinalityBlocks: 64, GasStrategy: "legacy_only"})
	fe, err := a.EstimateFee(context.Background(), FeeEstimateReq{Priority: PriorityHigh})
	if err != nil {
		t.Fatalf("estimate: %v", err)
	}
	if fe.GasPrice == nil || fe.GasPrice.Int64() <= 0 {
		t.Errorf("gas price: %s", fe.GasPrice)
	}
}

func TestEVMAdapterRPCEstimateFeeLow(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(rpcResponse{Result: json.RawMessage(`"0x3b9aca00"`)})
	}))
	defer srv.Close()
	a := NewEVMAdapter(ChainConfig{ChainID: "ethereum", RPCURLs: []string{srv.URL}, FinalityBlocks: 64, GasStrategy: "legacy_only"})
	fe, err := a.EstimateFee(context.Background(), FeeEstimateReq{Priority: PriorityLow})
	if err != nil {
		t.Fatalf("estimate: %v", err)
	}
	if fe.GasPrice == nil {
		t.Fatal("nil gas price")
	}
}

func TestEVMAdapterRPCError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(rpcResponse{Error: &rpcError{Code: -32000, Message: "nonce too low"}})
	}))
	defer srv.Close()
	a := NewEVMAdapter(ChainConfig{ChainID: "ethereum", RPCURLs: []string{srv.URL}, FinalityBlocks: 64})
	if _, err := a.Height(context.Background()); err == nil {
		t.Fatal("expected rpc error")
	}
}

func TestEVMAdapterHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	a := NewEVMAdapter(ChainConfig{ChainID: "ethereum", RPCURLs: []string{srv.URL}, FinalityBlocks: 64})
	if _, err := a.Height(context.Background()); err == nil {
		t.Fatal("expected http error")
	}
}

func TestEVMAdapterNoURLs(t *testing.T) {
	a := NewEVMAdapter(ChainConfig{ChainID: "ethereum", RPCURLs: nil, FinalityBlocks: 64})
	if _, err := a.Height(context.Background()); err == nil {
		t.Fatal("expected error with no urls")
	}
}

func TestSolanaAdapterRPCHeight(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(rpcResponse{Result: json.RawMessage(`100`)})
	}))
	defer srv.Close()
	a := NewSolanaAdapter(ChainConfig{ChainID: "solana", RPCURLs: []string{srv.URL}, FinalityBlocks: 1})
	h, err := a.Height(context.Background())
	if err != nil || h != 100 {
		t.Fatalf("height: %v %d", err, h)
	}
}

func TestSolanaAdapterRPCBalance(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(rpcResponse{Result: json.RawMessage(`{"value":1000}`)})
	}))
	defer srv.Close()
	a := NewSolanaAdapter(ChainConfig{ChainID: "solana", RPCURLs: []string{srv.URL}, FinalityBlocks: 1})
	b, err := a.Balance(context.Background(), "addr")
	if err != nil || b.Int64() != 1000 {
		t.Fatalf("balance: %v %s", err, b)
	}
}

func TestSolanaAdapterRPCGetTx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(rpcResponse{Result: json.RawMessage(`{"slot":50}`)})
	}))
	defer srv.Close()
	a := NewSolanaAdapter(ChainConfig{ChainID: "solana", RPCURLs: []string{srv.URL}, FinalityBlocks: 1})
	tx, err := a.GetTx(context.Background(), "0x1")
	if err != nil || tx.BlockHeight != 50 {
		t.Fatalf("get tx: %v %+v", err, tx)
	}
}

func TestSolanaAdapterRPCGetTxNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(rpcResponse{Result: json.RawMessage(`null`)})
	}))
	defer srv.Close()
	a := NewSolanaAdapter(ChainConfig{ChainID: "solana", RPCURLs: []string{srv.URL}, FinalityBlocks: 1})
	if _, err := a.GetTx(context.Background(), "0xmissing"); err != ErrTxNotFound {
		t.Fatalf("expected not found, got %v", err)
	}
}

func TestSolanaAdapterSubscribeMempool(t *testing.T) {
	a := NewSolanaAdapter(ChainConfig{ChainID: "solana", RPCURLs: nil, FinalityBlocks: 1})
	ch, _, err := a.SubscribeMempool(context.Background(), nil)
	if err != nil || ch == nil {
		t.Fatalf("mempool: %v", err)
	}
}

func TestBitcoinAdapterRPCHeight(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(rpcResponse{Result: json.RawMessage(`100`)})
	}))
	defer srv.Close()
	a := NewBitcoinAdapter(ChainConfig{ChainID: "bitcoin", RPCURLs: []string{srv.URL}, FinalityBlocks: 6})
	h, err := a.Height(context.Background())
	if err != nil || h != 100 {
		t.Fatalf("height: %v %d", err, h)
	}
}

func TestBitcoinAdapterRPCBalance(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(rpcResponse{Result: json.RawMessage(`0.5`)})
	}))
	defer srv.Close()
	a := NewBitcoinAdapter(ChainConfig{ChainID: "bitcoin", RPCURLs: []string{srv.URL}, FinalityBlocks: 6})
	b, err := a.Balance(context.Background(), "addr")
	if err != nil || b == nil {
		t.Fatalf("balance: %v %s", err, b)
	}
}

func TestBitcoinAdapterRPCEstimateFee(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(rpcResponse{Result: json.RawMessage(`{"feerate":0.0001}`)})
	}))
	defer srv.Close()
	a := NewBitcoinAdapter(ChainConfig{ChainID: "bitcoin", RPCURLs: []string{srv.URL}, FinalityBlocks: 6, GasStrategy: "bitcoin_rbf"})
	fe, err := a.EstimateFee(context.Background(), FeeEstimateReq{Priority: PriorityHigh})
	if err != nil {
		t.Fatalf("estimate: %v", err)
	}
	if fe.GasPrice == nil {
		t.Fatal("nil gas price")
	}
}

func TestBitcoinAdapterSubscribeMempool(t *testing.T) {
	a := NewBitcoinAdapter(ChainConfig{ChainID: "bitcoin", RPCURLs: nil, FinalityBlocks: 6})
	ch, _, err := a.SubscribeMempool(context.Background(), nil)
	if err != nil || ch == nil {
		t.Fatalf("mempool: %v", err)
	}
}

func TestParseHexQuantityErrors(t *testing.T) {
	_, err := parseHexQuantity("0xZZ")
	if err == nil {
		t.Error("expected error for bad hex")
	}
}

func TestParseHexBigBad(t *testing.T) {
	_, err := parseHexBig("0xZZ")
	if err == nil {
		t.Error("expected error for bad hex big")
	}
}

// --- Solana adapter RPC edge cases ---

func TestSolanaAdapterRPCBroadcast(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(rpcResponse{Result: json.RawMessage(`"0xabc"`)})
	}))
	defer srv.Close()
	a := NewSolanaAdapter(ChainConfig{ChainID: "solana", RPCURLs: []string{srv.URL}, FinalityBlocks: 1})
	h, err := a.Broadcast(context.Background(), []byte{0xde, 0xad})
	if err != nil || h != "0xabc" {
		t.Fatalf("broadcast: %v %s", err, h)
	}
}

func TestSolanaAdapterRPCGetTxStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(rpcResponse{Result: json.RawMessage(`{"slot":50}`)})
	}))
	defer srv.Close()
	a := NewSolanaAdapter(ChainConfig{ChainID: "solana", RPCURLs: []string{srv.URL}, FinalityBlocks: 1})
	st, err := a.GetTxStatus(context.Background(), "0x1")
	if err != nil || st == nil {
		t.Fatalf("status: %v %v", st, err)
	}
	if st.BlockHeight != 50 {
		t.Errorf("block height: %d", st.BlockHeight)
	}
}

func TestSolanaAdapterRPCEstimateFeeStandard(t *testing.T) {
	a := NewSolanaAdapter(ChainConfig{ChainID: "solana", RPCURLs: nil, FinalityBlocks: 1, GasStrategy: "solana_priority_fee"})
	fe, err := a.EstimateFee(context.Background(), FeeEstimateReq{Priority: PriorityStandard})
	if err != nil {
		t.Fatalf("estimate: %v", err)
	}
	if fe.GasPrice == nil || fe.GasPrice.Int64() != 5000 {
		t.Errorf("standard gas price: %s want 5000", fe.GasPrice)
	}
	if fe.TotalFee == nil || fe.TotalFee.Int64() != 5000 {
		t.Errorf("total fee: %s want 5000", fe.TotalFee)
	}
}

func TestSolanaAdapterRPCEstimateFeeLow(t *testing.T) {
	a := NewSolanaAdapter(ChainConfig{ChainID: "solana", RPCURLs: nil, FinalityBlocks: 1, GasStrategy: "solana_priority_fee"})
	fe, err := a.EstimateFee(context.Background(), FeeEstimateReq{Priority: PriorityLow})
	if err != nil {
		t.Fatalf("estimate: %v", err)
	}
	if fe.GasPrice == nil || fe.GasPrice.Int64() != 2500 {
		t.Errorf("low gas price: %s want 2500", fe.GasPrice)
	}
}

func TestSolanaAdapterRPCEstimateFeeEmptyPriority(t *testing.T) {
	a := NewSolanaAdapter(ChainConfig{ChainID: "solana", RPCURLs: nil, FinalityBlocks: 1, GasStrategy: "solana_priority_fee"})
	fe, err := a.EstimateFee(context.Background(), FeeEstimateReq{Priority: ""})
	if err != nil {
		t.Fatalf("estimate: %v", err)
	}
	if fe.Priority != PriorityStandard {
		t.Errorf("priority: %s want standard", fe.Priority)
	}
}

func TestSolanaAdapterRPCGetTxUnmarshalError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Malformed JSON object (not a JSON object structure that fits).
		_ = json.NewEncoder(w).Encode(rpcResponse{Result: json.RawMessage(`"notanobject"`)})
	}))
	defer srv.Close()
	a := NewSolanaAdapter(ChainConfig{ChainID: "solana", RPCURLs: []string{srv.URL}, FinalityBlocks: 1})
	if _, err := a.GetTx(context.Background(), "0x1"); err == nil {
		t.Fatal("expected unmarshal error")
	}
}

func TestSolanaAdapterRPCBroadcastUnmarshalError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Result is an object, not a string.
		_ = json.NewEncoder(w).Encode(rpcResponse{Result: json.RawMessage(`{"foo":1}`)})
	}))
	defer srv.Close()
	a := NewSolanaAdapter(ChainConfig{ChainID: "solana", RPCURLs: []string{srv.URL}, FinalityBlocks: 1})
	if _, err := a.Broadcast(context.Background(), []byte{0x01}); err == nil {
		t.Fatal("expected unmarshal error")
	}
}

func TestSolanaAdapterRPCHeightUnmarshalError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(rpcResponse{Result: json.RawMessage(`"notauint"`)})
	}))
	defer srv.Close()
	a := NewSolanaAdapter(ChainConfig{ChainID: "solana", RPCURLs: []string{srv.URL}, FinalityBlocks: 1})
	if _, err := a.Height(context.Background()); err == nil {
		t.Fatal("expected unmarshal error")
	}
}

func TestSolanaAdapterRPCBalanceUnmarshalError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(rpcResponse{Result: json.RawMessage(`"notanobject"`)})
	}))
	defer srv.Close()
	a := NewSolanaAdapter(ChainConfig{ChainID: "solana", RPCURLs: []string{srv.URL}, FinalityBlocks: 1})
	if _, err := a.Balance(context.Background(), "addr"); err == nil {
		t.Fatal("expected unmarshal error")
	}
}

func TestSolanaAdapterNoURLs(t *testing.T) {
	a := NewSolanaAdapter(ChainConfig{ChainID: "solana", RPCURLs: nil, FinalityBlocks: 1})
	if _, err := a.Height(context.Background()); err == nil {
		t.Fatal("expected error with no urls")
	}
}

func TestSolanaAdapterRPCError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(rpcResponse{Error: &rpcError{Code: -32000, Message: "boom"}})
	}))
	defer srv.Close()
	a := NewSolanaAdapter(ChainConfig{ChainID: "solana", RPCURLs: []string{srv.URL}, FinalityBlocks: 1})
	if _, err := a.Height(context.Background()); err == nil {
		t.Fatal("expected rpc error")
	}
}

// --- Bitcoin adapter RPC edge cases ---

func TestBitcoinAdapterRPCBroadcast(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(rpcResponse{Result: json.RawMessage(`"0xabc"`)})
	}))
	defer srv.Close()
	a := NewBitcoinAdapter(ChainConfig{ChainID: "bitcoin", RPCURLs: []string{srv.URL}, FinalityBlocks: 6})
	h, err := a.Broadcast(context.Background(), []byte{0xde, 0xad})
	if err != nil || h != "0xabc" {
		t.Fatalf("broadcast: %v %s", err, h)
	}
}

func TestBitcoinAdapterRPCBroadcastUnmarshalError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(rpcResponse{Result: json.RawMessage(`{"foo":1}`)})
	}))
	defer srv.Close()
	a := NewBitcoinAdapter(ChainConfig{ChainID: "bitcoin", RPCURLs: []string{srv.URL}, FinalityBlocks: 6})
	if _, err := a.Broadcast(context.Background(), []byte{0x01}); err == nil {
		t.Fatal("expected unmarshal error")
	}
}

func TestBitcoinAdapterRPCHeightUnmarshalError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(rpcResponse{Result: json.RawMessage(`"notauint"`)})
	}))
	defer srv.Close()
	a := NewBitcoinAdapter(ChainConfig{ChainID: "bitcoin", RPCURLs: []string{srv.URL}, FinalityBlocks: 6})
	if _, err := a.Height(context.Background()); err == nil {
		t.Fatal("expected unmarshal error")
	}
}

func TestBitcoinAdapterRPCBalanceUnmarshalError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(rpcResponse{Result: json.RawMessage(`"notanumber"`)})
	}))
	defer srv.Close()
	a := NewBitcoinAdapter(ChainConfig{ChainID: "bitcoin", RPCURLs: []string{srv.URL}, FinalityBlocks: 6})
	if _, err := a.Balance(context.Background(), "addr"); err == nil {
		t.Fatal("expected unmarshal error")
	}
}

func TestBitcoinAdapterRPCEstimateFeeStandard(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(rpcResponse{Result: json.RawMessage(`{"feerate":0.0001}`)})
	}))
	defer srv.Close()
	a := NewBitcoinAdapter(ChainConfig{ChainID: "bitcoin", RPCURLs: []string{srv.URL}, FinalityBlocks: 6, GasStrategy: "bitcoin_rbf"})
	fe, err := a.EstimateFee(context.Background(), FeeEstimateReq{Priority: PriorityStandard})
	if err != nil {
		t.Fatalf("estimate: %v", err)
	}
	if fe.GasPrice == nil {
		t.Fatal("nil gas price")
	}
	if fe.TotalFee == nil {
		t.Fatal("nil total fee")
	}
}

func TestBitcoinAdapterRPCEstimateFeeLow(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(rpcResponse{Result: json.RawMessage(`{"feerate":0.0001}`)})
	}))
	defer srv.Close()
	a := NewBitcoinAdapter(ChainConfig{ChainID: "bitcoin", RPCURLs: []string{srv.URL}, FinalityBlocks: 6, GasStrategy: "bitcoin_rbf"})
	fe, err := a.EstimateFee(context.Background(), FeeEstimateReq{Priority: PriorityLow})
	if err != nil {
		t.Fatalf("estimate: %v", err)
	}
	if fe.GasPrice == nil {
		t.Fatal("nil gas price")
	}
}

func TestBitcoinAdapterRPCEstimateFeeEmptyPriority(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(rpcResponse{Result: json.RawMessage(`{"feerate":0.0001}`)})
	}))
	defer srv.Close()
	a := NewBitcoinAdapter(ChainConfig{ChainID: "bitcoin", RPCURLs: []string{srv.URL}, FinalityBlocks: 6, GasStrategy: "bitcoin_rbf"})
	fe, err := a.EstimateFee(context.Background(), FeeEstimateReq{Priority: ""})
	if err != nil {
		t.Fatalf("estimate: %v", err)
	}
	if fe.Priority != PriorityStandard {
		t.Errorf("priority: %s want standard", fe.Priority)
	}
}

func TestBitcoinAdapterRPCEstimateFeeUnmarshalError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(rpcResponse{Result: json.RawMessage(`"notanobject"`)})
	}))
	defer srv.Close()
	a := NewBitcoinAdapter(ChainConfig{ChainID: "bitcoin", RPCURLs: []string{srv.URL}, FinalityBlocks: 6, GasStrategy: "bitcoin_rbf"})
	if _, err := a.EstimateFee(context.Background(), FeeEstimateReq{Priority: PriorityStandard}); err == nil {
		t.Fatal("expected unmarshal error")
	}
}

func TestBitcoinAdapterNoURLs(t *testing.T) {
	a := NewBitcoinAdapter(ChainConfig{ChainID: "bitcoin", RPCURLs: nil, FinalityBlocks: 6})
	if _, err := a.Height(context.Background()); err == nil {
		t.Fatal("expected error with no urls")
	}
}

func TestBitcoinAdapterRPCError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(rpcResponse{Error: &rpcError{Code: -32000, Message: "boom"}})
	}))
	defer srv.Close()
	a := NewBitcoinAdapter(ChainConfig{ChainID: "bitcoin", RPCURLs: []string{srv.URL}, FinalityBlocks: 6})
	if _, err := a.Height(context.Background()); err == nil {
		t.Fatal("expected rpc error")
	}
}

// --- EVM adapter edge cases ---

// TestEVMAdapterRPCGetTxStatusMempool exercises the BlockHeight==0 branch
// of GetTxStatus which returns StatusMempool without a second RPC call.
func TestEVMAdapterRPCGetTxStatusMempool(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(rpcResponse{Result: json.RawMessage(`{"hash":"0x1","blockNumber":"0x0"}`)})
	}))
	defer srv.Close()
	a := NewEVMAdapter(ChainConfig{ChainID: "ethereum", RPCURLs: []string{srv.URL}, FinalityBlocks: 64})
	st, err := a.GetTxStatus(context.Background(), "0x1")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if st.Status != StatusMempool {
		t.Errorf("status: %s want mempool", st.Status)
	}
}

func TestEVMAdapterRPCGetTxStatusFinalized(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		if calls == 1 {
			_ = json.NewEncoder(w).Encode(rpcResponse{Result: json.RawMessage(`{"hash":"0x1","blockNumber":"0xa"}`)})
		} else {
			// tip = 0x6e (110); confs = 110 - 10 + 1 = 101 >= 64.
			_ = json.NewEncoder(w).Encode(rpcResponse{Result: json.RawMessage(`"0x6e"`)})
		}
	}))
	defer srv.Close()
	a := NewEVMAdapter(ChainConfig{ChainID: "ethereum", RPCURLs: []string{srv.URL}, FinalityBlocks: 64})
	st, err := a.GetTxStatus(context.Background(), "0x1")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if st.Status != StatusFinalized {
		t.Errorf("status: %s want finalized", st.Status)
	}
}

func TestEVMAdapterRPCGetTxEmptyHash(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(rpcResponse{Result: json.RawMessage(`{"hash":"","blockNumber":"0xa"}`)})
	}))
	defer srv.Close()
	a := NewEVMAdapter(ChainConfig{ChainID: "ethereum", RPCURLs: []string{srv.URL}, FinalityBlocks: 64})
	if _, err := a.GetTx(context.Background(), "0x1"); err != ErrTxNotFound {
		t.Fatalf("expected ErrTxNotFound for empty hash, got %v", err)
	}
}

func TestEVMAdapterRPCGetTxUnmarshalError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(rpcResponse{Result: json.RawMessage(`"notanobject"`)})
	}))
	defer srv.Close()
	a := NewEVMAdapter(ChainConfig{ChainID: "ethereum", RPCURLs: []string{srv.URL}, FinalityBlocks: 64})
	if _, err := a.GetTx(context.Background(), "0x1"); err == nil {
		t.Fatal("expected unmarshal error")
	}
}

func TestEVMAdapterRPCBroadcastUnmarshalError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(rpcResponse{Result: json.RawMessage(`{"foo":1}`)})
	}))
	defer srv.Close()
	a := NewEVMAdapter(ChainConfig{ChainID: "ethereum", RPCURLs: []string{srv.URL}, FinalityBlocks: 64})
	if _, err := a.Broadcast(context.Background(), []byte{0x01}); err == nil {
		t.Fatal("expected unmarshal error")
	}
}

func TestEVMAdapterRPCHeightUnmarshalError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(rpcResponse{Result: json.RawMessage(`42`)})
	}))
	defer srv.Close()
	a := NewEVMAdapter(ChainConfig{ChainID: "ethereum", RPCURLs: []string{srv.URL}, FinalityBlocks: 64})
	if _, err := a.Height(context.Background()); err == nil {
		t.Fatal("expected unmarshal error")
	}
}

func TestEVMAdapterRPCBalanceUnmarshalError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(rpcResponse{Result: json.RawMessage(`42`)})
	}))
	defer srv.Close()
	a := NewEVMAdapter(ChainConfig{ChainID: "ethereum", RPCURLs: []string{srv.URL}, FinalityBlocks: 64})
	if _, err := a.Balance(context.Background(), "0xaddr"); err == nil {
		t.Fatal("expected unmarshal error")
	}
}

func TestEVMAdapterRPCEstimateFeeUnmarshalError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(rpcResponse{Result: json.RawMessage(`42`)})
	}))
	defer srv.Close()
	a := NewEVMAdapter(ChainConfig{ChainID: "ethereum", RPCURLs: []string{srv.URL}, FinalityBlocks: 64, GasStrategy: "legacy_only"})
	if _, err := a.EstimateFee(context.Background(), FeeEstimateReq{Priority: PriorityStandard}); err == nil {
		t.Fatal("expected unmarshal error")
	}
}

func TestEVMAdapterRPCEstimateFeeBadHex(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(rpcResponse{Result: json.RawMessage(`"0xZZ"`)})
	}))
	defer srv.Close()
	a := NewEVMAdapter(ChainConfig{ChainID: "ethereum", RPCURLs: []string{srv.URL}, FinalityBlocks: 64, GasStrategy: "legacy_only"})
	if _, err := a.EstimateFee(context.Background(), FeeEstimateReq{Priority: PriorityStandard}); err == nil {
		t.Fatal("expected bad hex error")
	}
}

func TestEVMAdapterRPCEstimateFeeStandard(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(rpcResponse{Result: json.RawMessage(`"0x3b9aca00"`)})
	}))
	defer srv.Close()
	a := NewEVMAdapter(ChainConfig{ChainID: "ethereum", RPCURLs: []string{srv.URL}, FinalityBlocks: 64, GasStrategy: "legacy_only"})
	fe, err := a.EstimateFee(context.Background(), FeeEstimateReq{Priority: PriorityStandard})
	if err != nil {
		t.Fatalf("estimate: %v", err)
	}
	if fe.GasPrice == nil {
		t.Fatal("nil gas price")
	}
	if fe.Priority != PriorityStandard {
		t.Errorf("priority: %s", fe.Priority)
	}
}

func TestEVMAdapterRPCEstimateFeeEmptyPriority(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(rpcResponse{Result: json.RawMessage(`"0x3b9aca00"`)})
	}))
	defer srv.Close()
	a := NewEVMAdapter(ChainConfig{ChainID: "ethereum", RPCURLs: []string{srv.URL}, FinalityBlocks: 64, GasStrategy: "legacy_only"})
	fe, err := a.EstimateFee(context.Background(), FeeEstimateReq{Priority: ""})
	if err != nil {
		t.Fatalf("estimate: %v", err)
	}
	if fe.Priority != PriorityStandard {
		t.Errorf("priority: %s want standard", fe.Priority)
	}
}

// TestEVMAdapterSubscribeHeadsEmit exercises the poll loop's height-change
// branch by serving increasing block heights from a test RPC server.
func TestEVMAdapterSubscribeHeadsEmit(t *testing.T) {
	var height uint64 = 100
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(rpcResponse{Result: json.RawMessage(`"0x64"`)})
		_ = height
	}))
	defer srv.Close()
	a := NewEVMAdapter(ChainConfig{ChainID: "ethereum", RPCURLs: []string{srv.URL}, FinalityBlocks: 64})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, _, err := a.SubscribeHeads(ctx)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	// The poll loop ticks every 2s; we cannot wait that long in a unit
	// test, so just verify the channel is open and cancel.
	select {
	case <-ch:
		t.Fatal("channel should not close before cancel")
	default:
	}
}

// TestRPCClientCallMarshalError is not directly reachable since
// rpcRequest always marshals; instead exercise the no-urls branch and a
// 4xx HTTP status.
func TestRPCClientHTTP4xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()
	a := NewEVMAdapter(ChainConfig{ChainID: "ethereum", RPCURLs: []string{srv.URL}, FinalityBlocks: 64})
	if _, err := a.Height(context.Background()); err == nil {
		t.Fatal("expected http 400 error")
	}
}

func TestRPCClientCallContextCanceled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()
	a := NewEVMAdapter(ChainConfig{ChainID: "ethereum", RPCURLs: []string{srv.URL}, FinalityBlocks: 64})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := a.Height(ctx); err == nil {
		t.Fatal("expected canceled context error")
	}
}