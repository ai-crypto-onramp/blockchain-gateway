package prepayment

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/ai-crypto-onramp/blockchain-gateway/internal/chain"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/walletclient"
)

func TestMemRedisSetNXAndGet(t *testing.T) {
	r := NewMemRedis()
	ctx := context.Background()
	ok, _ := r.SetNX(ctx, "k", "v", time.Minute)
	if !ok {
		t.Fatal("first SetNX should succeed")
	}
	ok, _ = r.SetNX(ctx, "k", "v2", time.Minute)
	if ok {
		t.Fatal("second SetNX should fail")
	}
	v, err := r.Get(ctx, "k")
	if err != nil || v != "v" {
		t.Fatalf("get: %v %s", err, v)
	}
	if _, err := r.Get(ctx, "missing"); err != ErrNoKey {
		t.Fatalf("missing key: %v", err)
	}
}

func TestMemRedisTTLExpiry(t *testing.T) {
	r := NewMemRedis()
	ctx := context.Background()
	_, _ = r.SetNX(ctx, "k", "v", 10*time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	ok, _ := r.SetNX(ctx, "k", "v2", time.Minute)
	if !ok {
		t.Fatal("expired key should be settable")
	}
}

func TestMemRedisIncr(t *testing.T) {
	r := NewMemRedis()
	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		v, _ := r.Incr(ctx, "counter")
		if v != int64(i) {
			t.Errorf("incr %d: got %d", i, v)
		}
	}
}

func TestCoordinatorLockAcquireRelease(t *testing.T) {
	r := NewMemRedis()
	c := NewCoordinator(r, time.Minute, time.Second)
	ctx := context.Background()
	release, err := c.AcquireLock(ctx, "ethereum", "0xabc")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	// Second acquire should time out quickly.
	_, err = c.AcquireLock(ctx, "ethereum", "0xabc")
	if err != ErrLockBusy {
		t.Fatalf("expected lock busy, got %v", err)
	}
	release()
	release2, err := c.AcquireLock(ctx, "ethereum", "0xabc")
	if err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	release2()
}

func TestCoordinatorNextNonceSeedsFromWallet(t *testing.T) {
	r := NewMemRedis()
	c := NewCoordinator(r, time.Minute, time.Second)
	ctx := context.Background()
	mock := walletclient.NewMock("0xfund", 42)
	n1, err := c.NextNonce(ctx, "ethereum", "0xabc", func(_ context.Context) (uint64, error) {
		resp, err := mock.AllocateNonce(ctx, "wallet-1", "ethereum")
		if err != nil {
			return 0, err
		}
		return resp.Nonce, nil
	})
	if err != nil || n1 != 42 {
		t.Fatalf("next nonce: %v %d", err, n1)
	}
	// Second call should use the cached counter (43), not the wallet.
	n2, _ := c.NextNonce(ctx, "ethereum", "0xabc", func(_ context.Context) (uint64, error) {
		t.Fatal("should not call wallet on second call")
		return 0, nil
	})
	if n2 != 43 {
		t.Errorf("second nonce: %d want 43", n2)
	}
}

func TestCoordinatorNonceConcurrent(t *testing.T) {
	r := NewMemRedis()
	c := NewCoordinator(r, time.Minute, time.Second)
	ctx := context.Background()
	mock := walletclient.NewMock("0xfund", 0)
	var wg sync.WaitGroup
	seen := make(map[uint64]bool)
	var mu sync.Mutex
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, err := c.AcquireLock(ctx, "ethereum", "0xabc")
			if err != nil {
				t.Errorf("acquire: %v", err)
				return
			}
			defer release()
			n, err := c.NextNonce(ctx, "ethereum", "0xabc", func(_ context.Context) (uint64, error) {
				resp, err := mock.AllocateNonce(ctx, "wallet-1", "ethereum")
				if err != nil {
					return 0, err
				}
				return resp.Nonce, nil
			})
			if err != nil {
				t.Errorf("next nonce: %v", err)
				return
			}
			mu.Lock()
			if seen[n] {
				t.Errorf("nonce reuse: %d", n)
			}
			seen[n] = true
			mu.Unlock()
		}()
	}
	wg.Wait()
	if len(seen) != 10 {
		t.Errorf("unique nonces: %d want 10", len(seen))
	}
}

func TestManagerInsufficientFunds(t *testing.T) {
	r := NewMemRedis()
	c := NewCoordinator(r, time.Minute, time.Second)
	mock := walletclient.NewMock("0xfunding", 0)
	mgr := NewManager(mock, c, time.Second)
	mgr.SetFundingPollInterval(10 * time.Millisecond)
	reg := chain.NewRegistry()
	reg.Register(chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "ethereum", FinalityBlocks: 64, Balance: big.NewInt(0)}))
	stub := reg.StubEmitter("ethereum")
	// Seed the funding tx as already confirmed so the funding-wait loop
	// returns immediately.
	stub.SeedTx(
		&chain.Tx{ChainID: "ethereum", Hash: "0xfunding", From: "0xsender", Status: chain.StatusConfirmed, BlockHeight: 1},
		&chain.TxStatus{ChainID: "ethereum", TxHash: "0xfunding", Status: chain.StatusConfirmed, Confirmations: 1, BlockHeight: 1},
	)
	adapter, _ := reg.Get("ethereum")
	res, err := mgr.EnsureFundsAndNonce(context.Background(), adapter, "wallet-1", "0xsender", big.NewInt(1_000_000_000))
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if !res.Funded || res.FundingTx != "0xfunding" {
		t.Errorf("funding: %+v", res)
	}
	if res.Nonce != 0 {
		t.Errorf("nonce: %d", res.Nonce)
	}
}

func TestManagerSufficientBalanceSkipsFunding(t *testing.T) {
	r := NewMemRedis()
	c := NewCoordinator(r, time.Minute, time.Second)
	mock := walletclient.NewMock("0xfunding", 5)
	mgr := NewManager(mock, c, time.Second)
	stub := chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "ethereum", FinalityBlocks: 64, Balance: big.NewInt(10_000_000_000)})
	res, err := mgr.EnsureFundsAndNonce(context.Background(), stub, "wallet-1", "0xsender", big.NewInt(1_000_000_000))
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if res.Funded {
		t.Error("should not fund when balance sufficient")
	}
	if res.Nonce != 5 {
		t.Errorf("nonce: %d want 5", res.Nonce)
	}
}

func TestManagerFundFailureShortCircuits(t *testing.T) {
	r := NewMemRedis()
	c := NewCoordinator(r, time.Minute, time.Second)
	mock := walletclient.NewMock("0xfunding", 0)
	mock.FundErr = errFailed
	mgr := NewManager(mock, c, time.Second)
	stub := chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "ethereum", FinalityBlocks: 64, Balance: big.NewInt(0)})
	_, err := mgr.EnsureFundsAndNonce(context.Background(), stub, "wallet-1", "0xsender", big.NewInt(1_000_000_000))
	if err == nil {
		t.Fatal("expected error on fund failure")
	}
}

var errFailed = newErr("fund failed")

type errStr string

func (e errStr) Error() string { return string(e) }

func newErr(s string) error { return errStr(s) }

// fundingStatusStub is a ChainAdapter wrapper that returns a
// configurable sequence of funding tx statuses so we can exercise the
// polling loop in waitForFundingConfirmation.
type fundingStatusStub struct {
	chain.ChainAdapter
	txHash   string
	statuses []chain.TxStatus
	calls    int
}

func (s *fundingStatusStub) GetTxStatus(_ context.Context, txHash string) (*chain.TxStatus, error) {
	if txHash != s.txHash {
		return nil, chain.ErrTxNotFound
	}
	if s.calls < len(s.statuses) {
		st := s.statuses[s.calls]
		s.calls++
		return &st, nil
	}
	// Once we've exhausted the scripted statuses, return the last one
	// forever (confirmed).
	if len(s.statuses) > 0 {
		st := s.statuses[len(s.statuses)-1]
		return &st, nil
	}
	return nil, chain.ErrTxNotFound
}

// TestManagerWaitsForFundingConfirmation asserts the manager polls the
// funding tx status until it reaches the required confirmation depth
// before allocating the nonce.
func TestManagerWaitsForFundingConfirmation(t *testing.T) {
	r := NewMemRedis()
	c := NewCoordinator(r, time.Minute, time.Second)
	mock := walletclient.NewMock("0xfunding", 11)
	mgr := NewManager(mock, c, 5*time.Second)
	mgr.SetFundingPollInterval(5 * time.Millisecond)
	mgr.SetFundingMinConfirms(1)
	reg := chain.NewRegistry()
	reg.Register(chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "ethereum", FinalityBlocks: 64, Balance: big.NewInt(0)}))
	adapter, _ := reg.Get("ethereum")
	stub := &fundingStatusStub{
		ChainAdapter: adapter,
		txHash:       "0xfunding",
		// First poll: pending (not enough confirmations). Second poll:
		// confirmed.
		statuses: []chain.TxStatus{
			{ChainID: "ethereum", TxHash: "0xfunding", Status: chain.StatusMempool, Confirmations: 0},
			{ChainID: "ethereum", TxHash: "0xfunding", Status: chain.StatusConfirmed, Confirmations: 1, BlockHeight: 1},
		},
	}
	res, err := mgr.EnsureFundsAndNonce(context.Background(), stub, "wallet-1", "0xsender", big.NewInt(1_000_000_000))
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if !res.Funded || res.FundingTx != "0xfunding" {
		t.Errorf("funding: %+v", res)
	}
	if res.Nonce != 11 {
		t.Errorf("nonce: %d want 11", res.Nonce)
	}
	if stub.calls < 2 {
		t.Errorf("expected at least 2 status polls, got %d", stub.calls)
	}
}

// TestManagerFundingTimeout asserts the manager returns ErrFundingTimeout
// when the funding tx never confirms within fundingTimeout.
func TestManagerFundingTimeout(t *testing.T) {
	r := NewMemRedis()
	c := NewCoordinator(r, time.Minute, time.Second)
	mock := walletclient.NewMock("0xfunding", 11)
	mgr := NewManager(mock, c, 50*time.Millisecond)
	mgr.SetFundingPollInterval(5 * time.Millisecond)
	reg := chain.NewRegistry()
	reg.Register(chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "ethereum", FinalityBlocks: 64, Balance: big.NewInt(0)}))
	adapter, _ := reg.Get("ethereum")
	stub := &fundingStatusStub{
		ChainAdapter: adapter,
		txHash:       "0xfunding",
		// Always pending — never confirms.
		statuses: []chain.TxStatus{
			{ChainID: "ethereum", TxHash: "0xfunding", Status: chain.StatusMempool, Confirmations: 0},
		},
	}
	_, err := mgr.EnsureFundsAndNonce(context.Background(), stub, "wallet-1", "0xsender", big.NewInt(1_000_000_000))
	if err == nil {
		t.Fatal("expected funding timeout error")
	}
	if !errors.Is(err, ErrFundingTimeout) {
		t.Errorf("expected ErrFundingTimeout, got %v", err)
	}
}
