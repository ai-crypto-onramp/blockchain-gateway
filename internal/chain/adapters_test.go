package chain

import (
	"context"
	"math/big"
	"testing"
)

// TestEVMAdapterConstruction verifies the EVM adapter scaffold constructs and
// reports its config without making network calls.
func TestEVMAdapterConstruction(t *testing.T) {
	a := NewEVMAdapter(ChainConfig{ChainID: "ethereum", RPCURLs: []string{"http://localhost:8545"}, FinalityBlocks: 64, GasStrategy: "eip1559_dynamic"})
	if a.ChainID() != "ethereum" {
		t.Errorf("chain id: %s", a.ChainID())
	}
	if a.FinalityBlocks() != 64 {
		t.Errorf("finality: %d", a.FinalityBlocks())
	}
}

func TestEVMAdapterSubscribeHeadsPolls(t *testing.T) {
	a := NewEVMAdapter(ChainConfig{ChainID: "ethereum", RPCURLs: nil, FinalityBlocks: 64})
	// No RPC URLs: SubscribeHeads should still return a channel that
	// closes on context cancel (the poll loop swallows errors).
	ctx, cancel := context.WithCancel(context.Background())
	ch, _, err := a.SubscribeHeads(ctx)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	cancel()
	// Channel should close shortly after cancel.
	<-ch
}

func TestEVMAdapterSubscribeMempool(t *testing.T) {
	a := NewEVMAdapter(ChainConfig{ChainID: "ethereum", RPCURLs: nil, FinalityBlocks: 64})
	ch, _, err := a.SubscribeMempool(context.Background(), nil)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if ch == nil {
		t.Fatal("nil channel")
	}
}

func TestSolanaAdapterConstruction(t *testing.T) {
	a := NewSolanaAdapter(ChainConfig{ChainID: "solana", RPCURLs: []string{"http://localhost:8899"}, FinalityBlocks: 1, GasStrategy: "solana_priority_fee"})
	if a.ChainID() != "solana" || a.FinalityBlocks() != 1 {
		t.Errorf("solana: %s %d", a.ChainID(), a.FinalityBlocks())
	}
}

func TestSolanaAdapterEstimateFee(t *testing.T) {
	a := NewSolanaAdapter(ChainConfig{ChainID: "solana", RPCURLs: nil, FinalityBlocks: 1, GasStrategy: "solana_priority_fee"})
	fe, err := a.EstimateFee(context.Background(), FeeEstimateReq{Priority: PriorityHigh})
	if err != nil {
		t.Fatalf("estimate: %v", err)
	}
	if fe.GasPrice == nil || fe.GasPrice.Int64() <= 0 {
		t.Errorf("gas price: %s", fe.GasPrice)
	}
}

func TestSolanaAdapterSubscribeHeadsPolls(t *testing.T) {
	a := NewSolanaAdapter(ChainConfig{ChainID: "solana", RPCURLs: nil, FinalityBlocks: 1})
	ctx, cancel := context.WithCancel(context.Background())
	ch, _, err := a.SubscribeHeads(ctx)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	cancel()
	<-ch
}

func TestBitcoinAdapterConstruction(t *testing.T) {
	a := NewBitcoinAdapter(ChainConfig{ChainID: "bitcoin", RPCURLs: []string{"http://localhost:8332"}, FinalityBlocks: 6, GasStrategy: "bitcoin_rbf"})
	if a.ChainID() != "bitcoin" || a.FinalityBlocks() != 6 {
		t.Errorf("bitcoin: %s %d", a.ChainID(), a.FinalityBlocks())
	}
}

func TestBitcoinAdapterGetTx(t *testing.T) {
	a := NewBitcoinAdapter(ChainConfig{ChainID: "bitcoin", RPCURLs: nil, FinalityBlocks: 6})
	tx, err := a.GetTx(context.Background(), "abc")
	if err != nil || tx == nil {
		t.Fatalf("get tx: %v %v", tx, err)
	}
}

func TestBitcoinAdapterGetTxStatus(t *testing.T) {
	a := NewBitcoinAdapter(ChainConfig{ChainID: "bitcoin", RPCURLs: nil, FinalityBlocks: 6})
	st, err := a.GetTxStatus(context.Background(), "abc")
	if err != nil || st == nil {
		t.Fatalf("get tx status: %v %v", st, err)
	}
}

func TestBitcoinAdapterSubscribeHeadsPolls(t *testing.T) {
	a := NewBitcoinAdapter(ChainConfig{ChainID: "bitcoin", RPCURLs: nil, FinalityBlocks: 6})
	ctx, cancel := context.WithCancel(context.Background())
	ch, _, err := a.SubscribeHeads(ctx)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	cancel()
	<-ch
}

func TestStubAdapterSetHeightAndBalance(t *testing.T) {
	a := NewStubAdapter(StubAdapterOptions{ChainID: "stub", FinalityBlocks: 3}).(*stubAdapter)
	a.SetHeight(500)
	h, _ := a.Height(context.Background())
	if h != 500 {
		t.Errorf("height: %d", h)
	}
	a.SetBalance(big.NewInt(42))
	b, _ := a.Balance(context.Background(), "x")
	if b.Int64() != 42 {
		t.Errorf("balance: %s", b)
	}
}

func TestStubAdapterLastBroadcast(t *testing.T) {
	a := NewStubAdapter(StubAdapterOptions{ChainID: "stub", FinalityBlocks: 3}).(*stubAdapter)
	_, _ = a.Broadcast(context.Background(), []byte("payload"))
	if string(a.LastBroadcast()) != "payload" {
		t.Errorf("last broadcast: %s", a.LastBroadcast())
	}
}

func TestStubAdapterSeedTxAndGet(t *testing.T) {
	a := NewStubAdapter(StubAdapterOptions{ChainID: "stub", FinalityBlocks: 3}).(*stubAdapter)
	a.SeedTx(&Tx{ChainID: "stub", Hash: "0x1", From: "0xa", Status: StatusConfirmed}, &TxStatus{ChainID: "stub", TxHash: "0x1", Status: StatusConfirmed, Confirmations: 2})
	tx, err := a.GetTx(context.Background(), "0x1")
	if err != nil || tx.From != "0xa" {
		t.Fatalf("get tx: %v %+v", err, tx)
	}
	st, err := a.GetTxStatus(context.Background(), "0x1")
	if err != nil || st.Confirmations != 2 {
		t.Fatalf("get status: %v %+v", err, st)
	}
}