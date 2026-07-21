// Package prepayment orchestrates gas prepayment: before broadcasting,
// ensure the sender account has enough native asset to cover gas; if not,
// request funding from Wallet Management, wait for the funding tx to be
// confirmed on-chain, then allocate the nonce under a distributed lock.
// Fail fast (do not broadcast) on any prepayment error.
package prepayment

import (
	"context"
	"errors"
	"fmt"
	"log"
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
	// pollInterval is the polling interval for funding confirmation.
	pollInterval time.Duration
	// minConfirms is the number of confirmations required for the funding
	// tx before broadcast proceeds.
	minConfirms uint64
}

// NewManager returns a prepayment Manager.
func NewManager(wallet walletclient.Client, locks *Coordinator, fundingTimeout time.Duration) *Manager {
	return &Manager{
		wallet:         wallet,
		locks:          locks,
		fundingTimeout: fundingTimeout,
		pollInterval:   2 * time.Second,
		minConfirms:    1,
	}
}

// SetFundingPollInterval overrides the funding confirmation poll interval.
// Must be > 0.
func (m *Manager) SetFundingPollInterval(d time.Duration) {
	if d > 0 {
		m.pollInterval = d
	}
}

// SetFundingMinConfirms overrides the minimum confirmations required for
// the funding tx before broadcast proceeds.
func (m *Manager) SetFundingMinConfirms(n uint64) {
	if n > 0 {
		m.minConfirms = n
	}
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

// ErrFundingTimeout is returned when the funding tx does not reach the
// required confirmation depth within fundingTimeout.
var ErrFundingTimeout = errors.New("funding tx not confirmed in time")

// EnsureFundsAndNonce checks the sender's balance against the estimated
// fee; if insufficient, requests funding from Wallet Management and waits
// for the funding tx to confirm before allocating the nonce. walletID is
// the wallet-management wallet id owning the sender address; it is
// required for the funding and nonce-allocation REST calls. Returns
// ErrPrepaymentFailed (wrapping the cause) on any failure.
func (m *Manager) EnsureFundsAndNonce(ctx context.Context, adapter chain.ChainAdapter, walletID, from string, fee *big.Int) (*Result, error) {
	bal, err := adapter.Balance(ctx, from)
	if err != nil {
		return nil, fmt.Errorf("%w: balance: %v", ErrPrepaymentFailed, err)
	}
	res := &Result{}
	if bal.Cmp(fee) < 0 {
		deficit := new(big.Int).Sub(fee, bal)
		resp, err := m.wallet.FundSender(ctx, walletclient.FundSenderRequest{
			WalletID: walletID,
			ChainID:  adapter.ChainID(),
			Addr:     from,
			Amount:   deficit.String(),
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
		if res.FundingTx != "" {
			if err := m.waitForFundingConfirmation(ctx, adapter, res.FundingTx); err != nil {
				return nil, fmt.Errorf("%w: %w", ErrPrepaymentFailed, err)
			}
		} else {
			if err := m.waitForBalanceConfirmation(ctx, adapter, from, bal, deficit); err != nil {
				return nil, fmt.Errorf("%w: %w", ErrPrepaymentFailed, err)
			}
		}
	}
	release, err := m.locks.AcquireLock(ctx, adapter.ChainID(), from)
	if err != nil {
		return nil, fmt.Errorf("%w: lock: %v", ErrPrepaymentFailed, err)
	}
	defer release()
	nonce, err := m.locks.NextNonce(ctx, adapter.ChainID(), from, func(ctx context.Context) (uint64, error) {
		r, err := m.wallet.AllocateNonce(ctx, walletID, adapter.ChainID())
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

// waitForFundingConfirmation polls the chain adapter for the funding tx
// status until it reaches minConfirms confirmations or the funding
// timeout expires. Returns ErrFundingTimeout on timeout.
func (m *Manager) waitForFundingConfirmation(ctx context.Context, adapter chain.ChainAdapter, fundingTxHash string) error {
	waitCtx, cancel := context.WithTimeout(ctx, m.fundingTimeout)
	defer cancel()
	ticker := time.NewTicker(m.pollInterval)
	defer ticker.Stop()
	for {
		status, err := adapter.GetTxStatus(waitCtx, fundingTxHash)
		if err == nil && status != nil && status.Confirmations >= m.minConfirms &&
			(status.Status == chain.StatusConfirmed || status.Status == chain.StatusFinalized) {
			return nil
		}
		if err != nil && err != chain.ErrTxNotFound {
			log.Printf("prepayment: funding tx status poll for %s: %v", fundingTxHash, err)
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("%w: %s after %s", ErrFundingTimeout, fundingTxHash, m.fundingTimeout)
		case <-ticker.C:
		}
	}
}

// waitForBalanceConfirmation polls the sender's on-chain balance until it
// increases by at least the deficit, or the funding timeout expires.
// Used when wallet-management funds asynchronously and the funding tx
// hash is not known to the gateway.
func (m *Manager) waitForBalanceConfirmation(ctx context.Context, adapter chain.ChainAdapter, from string, prevBalance, deficit *big.Int) error {
	target := new(big.Int).Add(prevBalance, deficit)
	waitCtx, cancel := context.WithTimeout(ctx, m.fundingTimeout)
	defer cancel()
	ticker := time.NewTicker(m.pollInterval)
	defer ticker.Stop()
	for {
		bal, err := adapter.Balance(waitCtx, from)
		if err == nil && bal != nil && bal.Cmp(target) >= 0 {
			return nil
		}
		if err != nil {
			log.Printf("prepayment: balance poll for %s: %v", from, err)
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("%w: balance for %s not funded after %s", ErrFundingTimeout, from, m.fundingTimeout)
		case <-ticker.C:
		}
	}
}
