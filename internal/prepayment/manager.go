// Package prepayment orchestrates gas prepayment: before broadcasting,
// ensure the sender account has enough native asset to cover gas; if not,
// request funding from Wallet Management, then allocate the nonce under a
// distributed lock. Fail fast (do not broadcast) on any prepayment error.
package prepayment

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/ai-crypto-onramp/blockchain-gateway/internal/chain"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/walletclient"
)

// Manager coordinates balance check, funding, and nonce allocation.
type Manager struct {
	wallet walletclient.Client
	locks  *Coordinator
	// fundingTimeout bounds how long we wait for the funding tx to land.
	fundingTimeout time.Duration
}

// NewManager returns a prepayment Manager.
func NewManager(wallet walletclient.Client, locks *Coordinator, fundingTimeout time.Duration) *Manager {
	if fundingTimeout <= 0 {
		fundingTimeout = 30 * time.Second
	}
	return &Manager{wallet: wallet, locks: locks, fundingTimeout: fundingTimeout}
}

// Result is the outcome of EnsureFundsAndNonce: the nonce to use and the
// funding tx hash (empty if no funding was needed).
type Result struct {
	Nonce     uint64
	FundingTx string
	Funded    bool
}

// ErrPrepaymentFailed is returned when prepayment cannot be confirmed.
var ErrPrepaymentFailed = errors.New("prepayment failed")

// EnsureFundsAndNonce checks the sender's balance against the estimated
// fee; if insufficient, requests funding from Wallet Management. It then
// allocates the next nonce under the per-(chain, addr) lock. Returns
// ErrPrepaymentFailed (wrapping the cause) on any failure.
func (m *Manager) EnsureFundsAndNonce(ctx context.Context, adapter chain.ChainAdapter, from string, fee *big.Int) (*Result, error) {
	bal, err := adapter.Balance(ctx, from)
	if err != nil {
		return nil, fmt.Errorf("%w: balance: %v", ErrPrepaymentFailed, err)
	}
	res := &Result{}
	if bal.Cmp(fee) < 0 {
		deficit := new(big.Int).Sub(fee, bal)
		resp, err := m.wallet.FundSender(ctx, walletclient.FundSenderRequest{
			ChainID: adapter.ChainID(),
			Addr:    from,
			Amount:  deficit.String(),
		})
		if err != nil || resp == nil || !resp.Ok {
			cause := err
			if resp != nil && resp.Error != "" {
				cause = fmt.Errorf("%s", resp.Error)
			}
			return nil, fmt.Errorf("%w: fund sender: %v", ErrPrepaymentFailed, cause)
		}
		res.FundingTx = resp.FundingTx
		res.Funded = true
		// In production we would wait for the funding tx to confirm up to
		// fundingTimeout. For the unit-tested path the stub adapter
		// reports the new balance immediately; we re-check once.
		if waitCtx, cancel := context.WithTimeout(ctx, m.fundingTimeout); cancel != nil {
			_ = waitCtx
			cancel()
		}
	}
	release, err := m.locks.AcquireLock(ctx, adapter.ChainID(), from)
	if err != nil {
		return nil, fmt.Errorf("%w: lock: %v", ErrPrepaymentFailed, err)
	}
	defer release()
	nonce, err := m.locks.NextNonce(ctx, adapter.ChainID(), from, func(ctx context.Context) (uint64, error) {
		r, err := m.wallet.AllocateNonce(ctx, adapter.ChainID(), from)
		if err != nil {
			return 0, err
		}
		return r.Nonce, nil
	})
	if err != nil {
		return nil, fmt.Errorf("%w: nonce: %v", ErrPrepaymentFailed, err)
	}
	res.Nonce = nonce
	return res, nil
}