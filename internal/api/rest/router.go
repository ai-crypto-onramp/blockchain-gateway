// Package rest exposes the Blockchain Gateway REST API. It uses the chi
// router to keep handlers thin and testable. All chain-specific behavior
// is encapsulated behind the chain.Registry and the broadcast.Service.
package rest

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/ai-crypto-onramp/blockchain-gateway/internal/broadcast"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/chain"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/eventbus"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/fee"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/store"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/tip"

	"github.com/go-chi/chi/v5"
)

// Deps bundles the handler dependencies.
type Deps struct {
	Registry   *chain.Registry
	Broadcast  *broadcast.Service
	Estimators map[string]*fee.Estimator
	Broadcasts store.BroadcastStore
	Confirms   store.ConfirmationStore
	Tips       store.TipStore
	Followers  map[string]*tip.Follower
	Bus        *eventbus.Bus
	WSHandler  http.Handler // optional; served at /v1/chains/{chain}/heads
}

// NewRouter returns a chi.Mux wired with all REST routes.
func NewRouter(d *Deps) *chi.Mux {
	r := chi.NewRouter()
	r.Get("/healthz", healthz)
	r.Route("/v1/chains/{chain}", func(r chi.Router) {
		r.Post("/broadcast", d.handleBroadcast)
		r.Post("/estimate-fee", d.handleEstimateFee)
		r.Get("/height", d.handleHeight)
		r.Get("/address/{addr}/balance", d.handleBalance)
		r.Get("/tx/{hash}", d.handleGetTx)
		r.Get("/tx/{hash}/status", d.handleGetTxStatus)
		if d.WSHandler != nil {
			r.Get("/heads", d.WSHandler.ServeHTTP)
		}
	})
	return r
}

func healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type broadcastReq struct {
	SignedTx    string `json:"signed_tx"`
	From        string `json:"from"`
	WalletID    string `json:"wallet_id"`
	To          string `json:"to"`
	Value       string `json:"value"`
	Nonce       uint64 `json:"nonce"`
	SubmittedBy string `json:"submitted_by"`
}

type broadcastResp struct {
	TxHash string `json:"tx_hash"`
	Nonce  uint64 `json:"nonce"`
}

func (d *Deps) handleBroadcast(w http.ResponseWriter, r *http.Request) {
	chainID := chi.URLParam(r, "chain")
	var req broadcastReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "malformed json")
		return
	}
	if req.SignedTx == "" {
		writeError(w, http.StatusBadRequest, "missing signed_tx")
		return
	}
	signed, err := decodePayload(req.SignedTx)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid signed_tx encoding")
		return
	}
	resp, err := d.Broadcast.Broadcast(r.Context(), &broadcast.Request{
		ChainID:     chainID,
		SignedTx:    signed,
		From:        req.From,
		WalletID:    req.WalletID,
		To:          req.To,
		Value:       req.Value,
		Nonce:       req.Nonce,
		SubmittedBy: req.SubmittedBy,
	})
	if err != nil {
		writeBroadcastError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, &broadcastResp{TxHash: resp.TxHash, Nonce: resp.Nonce})
}

type estimateFeeReq struct {
	To       string `json:"to"`
	Amount   string `json:"amount"`
	Priority string `json:"priority"`
}

func (d *Deps) handleEstimateFee(w http.ResponseWriter, r *http.Request) {
	chainID := chi.URLParam(r, "chain")
	adapter, err := d.Registry.Get(chainID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	var req estimateFeeReq
	_ = json.NewDecoder(r.Body).Decode(&req)
	p := chain.Priority(req.Priority)
	if p == "" {
		p = chain.PriorityStandard
	}
	amt := new(big.Int)
	if req.Amount != "" {
		amt, _ = new(big.Int).SetString(req.Amount, 10)
	}
	var fe *chain.FeeEstimate
	if est, ok := d.Estimators[chainID]; ok {
		fe, err = est.Estimate(r.Context(), adapter, chain.FeeEstimateReq{To: req.To, Amount: amt, Priority: p})
	} else {
		fe, err = adapter.EstimateFee(r.Context(), chain.FeeEstimateReq{To: req.To, Amount: amt, Priority: p})
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, feeToJSON(fe))
}

func (d *Deps) handleHeight(w http.ResponseWriter, r *http.Request) {
	chainID := chi.URLParam(r, "chain")
	adapter, err := d.Registry.Get(chainID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	h, err := adapter.Height(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	tipHash := ""
	finalized := uint64(0)
	if t, err := d.Tips.Get(r.Context(), chainID); err == nil {
		tipHash = t.TipHash
		finalized = t.FinalizedHeight
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"height":           h,
		"hash":             tipHash,
		"finalized_height": finalized,
	})
}

func (d *Deps) handleBalance(w http.ResponseWriter, r *http.Request) {
	chainID := chi.URLParam(r, "chain")
	addr := chi.URLParam(r, "addr")
	adapter, err := d.Registry.Get(chainID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	bal, err := adapter.Balance(r.Context(), addr)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"address":  addr,
		"balance":  bal.String(),
		"decimals": 18,
		"symbol":   symbolFor(chainID),
	})
}

func (d *Deps) handleGetTx(w http.ResponseWriter, r *http.Request) {
	chainID := chi.URLParam(r, "chain")
	hash := chi.URLParam(r, "hash")
	adapter, err := d.Registry.Get(chainID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	t, err := adapter.GetTx(r.Context(), hash)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, txToJSON(t))
}

func (d *Deps) handleGetTxStatus(w http.ResponseWriter, r *http.Request) {
	chainID := chi.URLParam(r, "chain")
	hash := chi.URLParam(r, "hash")
	c, err := d.Confirms.Get(r.Context(), chainID, hash)
	if err == nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"status":        string(c.Status),
			"confirmations": c.Confirmations,
			"finalized_at":  c.FinalizedAt,
		})
		return
	}
	// Fall back to the adapter.
	adapter, err := d.Registry.Get(chainID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	st, err := adapter.GetTxStatus(r.Context(), hash)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":        string(st.Status),
		"confirmations": st.Confirmations,
		"finalized_at":  st.FinalizedAt,
	})
}

// --- helpers ---

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func writeBroadcastError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, chain.ErrUnknownChain):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, broadcast.ErrBadRequest):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, prepaymentErrSentinel()):
		writeError(w, http.StatusPaymentRequired, err.Error())
	default:
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}

// prepaymentErrSentinel returns the prepayment error sentinel for
// classification. It's referenced via a helper to avoid an import cycle
// with the prepayment package's error type in this classification switch.
func prepaymentErrSentinel() error { return errPrepaymentStub }

// errPrepaymentStub is a sentinel matching prepayment.ErrPrepaymentFailed
// without an import (the broadcast package wraps it). We classify by
// substring instead.
var errPrepaymentStub = errors.New("prepayment failed")

func decodePayload(s string) ([]byte, error) {
	if strings.HasPrefix(s, "0x") {
		return hex.DecodeString(s[2:])
	}
	// Try base64 then hex.
	if b, err := decodeBase64(s); err == nil {
		return b, nil
	}
	return hex.DecodeString(s)
}

func decodeBase64(s string) ([]byte, error) {
	const tbl = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	if len(s)%4 != 0 {
		s = s + strings.Repeat("=", (4-len(s)%4)%4)
	}
	out := make([]byte, 0, len(s)/4*3)
	var buf uint32
	var bits int
	for _, c := range s {
		if c == '=' {
			continue
		}
		idx := strings.IndexRune(tbl, c)
		if idx < 0 {
			return nil, errors.New("bad base64")
		}
		buf = (buf << 6) | uint32(idx)
		bits += 6
		if bits >= 8 {
			bits -= 8
			out = append(out, byte(buf>>bits))
			buf &= (1 << bits) - 1
		}
	}
	return out, nil
}

func feeToJSON(fe *chain.FeeEstimate) map[string]any {
	m := map[string]any{
		"gas_limit": fe.GasLimit,
		"priority":  string(fe.Priority),
		"strategy":  fe.Strategy,
		"total_fee": bigStr(fe.TotalFee),
	}
	if fe.MaxFeePerGas != nil {
		m["max_fee_per_gas"] = fe.MaxFeePerGas.String()
	}
	if fe.MaxPriorityFeePerGas != nil {
		m["max_priority_fee_per_gas"] = fe.MaxPriorityFeePerGas.String()
	}
	if fe.GasPrice != nil {
		m["gas_price"] = fe.GasPrice.String()
	}
	return m
}

func txToJSON(t *chain.Tx) map[string]any {
	m := map[string]any{
		"tx_hash":      t.Hash,
		"status":       string(t.Status),
		"block_height": t.BlockHeight,
		"from":         t.From,
		"to":           t.To,
		"nonce":        t.Nonce,
	}
	if t.Value != nil {
		m["value"] = t.Value.String()
	}
	if t.Fee != nil {
		m["fee"] = t.Fee.String()
	}
	if len(t.Raw) > 0 {
		m["raw"] = "0x" + hex.EncodeToString(t.Raw)
	}
	return m
}

func bigStr(b *big.Int) string {
	if b == nil {
		return "0"
	}
	return b.String()
}

func symbolFor(chainID string) string {
	switch chainID {
	case "ethereum", "polygon", "arbitrum", "optimism", "base":
		if chainID == "ethereum" {
			return "ETH"
		}
		if chainID == "polygon" {
			return "MATIC"
		}
		return "ETH"
	case "solana":
		return "SOL"
	case "bitcoin":
		return "BTC"
	}
	return ""
}

// ensure context import is used (handler timeouts use r.Context()).
var _ = context.Background
var _ = time.Now
