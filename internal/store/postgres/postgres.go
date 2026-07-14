// Package postgres implements the store interfaces against a real
// PostgreSQL database using database/sql + lib/pq. It is NOT used by unit
// tests (those use internal/store/memstore); it is exercised by the
// integration suite via docker-compose. A noop mode keeps the build clean
// when the database driver is absent.
package postgres

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"math/big"
	"time"

	_ "github.com/lib/pq"

	"github.com/ai-crypto-onramp/blockchain-gateway/internal/chain"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/store"
)

//go:embed migrations/001_init.sql
var initSQL string

// DB wraps a *sql.DB and exposes the store implementations.
type DB struct {
	sql           *sql.DB
	broadcast     *BroadcastStore
	confirmation  *ConfirmationStore
	tip           *TipStore
	fee           *FeeStore
	reorg         *ReorgStore
	outbox        *OutboxStore
}

// Open connects to dsn, pings, runs migrations, and returns a wired DB.
func Open(dsn string) (*DB, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if _, err := db.Exec(initSQL); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	d := &DB{sql: db}
	d.broadcast = &BroadcastStore{db: db}
	d.confirmation = &ConfirmationStore{db: db}
	d.tip = &TipStore{db: db}
	d.fee = &FeeStore{db: db}
	d.reorg = &ReorgStore{db: db}
	d.outbox = &OutboxStore{db: db}
	return d, nil
}

// Close releases the database connection.
func (d *DB) Close() error { return d.sql.Close() }

// Broadcast returns the BroadcastStore.
func (d *DB) Broadcast() store.BroadcastStore { return d.broadcast }

// Confirmation returns the ConfirmationStore.
func (d *DB) Confirmation() store.ConfirmationStore { return d.confirmation }

// Tip returns the TipStore.
func (d *DB) Tip() store.TipStore { return d.tip }

// Fee returns the FeeStore.
func (d *DB) Fee() store.FeeStore { return d.fee }

// Reorg returns the ReorgStore.
func (d *DB) Reorg() store.ReorgStore { return d.reorg }

// Outbox returns the OutboxStore.
func (d *DB) Outbox() store.OutboxStore { return d.outbox }

// --- BroadcastStore ---

type BroadcastStore struct{ db *sql.DB }

func (s *BroadcastStore) Insert(ctx context.Context, b *store.Broadcast) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO broadcasts (chain_id, tx_hash, signed_tx, from_addr, to_addr, value, nonce, submitted_at, submitted_by)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT (chain_id, tx_hash) DO NOTHING`,
		b.ChainID, b.TxHash, b.SignedTx, b.FromAddr, b.ToAddr, b.Value.String(), b.Nonce, b.SubmittedAt, b.SubmittedBy)
	return err
}

func (s *BroadcastStore) GetByTxHash(ctx context.Context, chainID, txHash string) (*store.Broadcast, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT chain_id, tx_hash, signed_tx, from_addr, to_addr, value, nonce, submitted_at, submitted_by
		 FROM broadcasts WHERE chain_id=$1 AND tx_hash=$2`, chainID, txHash)
	var b store.Broadcast
	var valStr, submittedBy string
	var submittedAt time.Time
	if err := row.Scan(&b.ChainID, &b.TxHash, &b.SignedTx, &b.FromAddr, &b.ToAddr, &valStr, &b.Nonce, &submittedAt, &submittedBy); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, &store.ErrNotFound{Chain: chainID, Key: txHash}
		}
		return nil, err
	}
	b.Value, _ = new(big.Int).SetString(valStr, 10)
	b.SubmittedAt = submittedAt
	b.SubmittedBy = submittedBy
	return &b, nil
}

func (s *BroadcastStore) Exists(ctx context.Context, chainID, txHash string) (bool, error) {
	var exists bool
	err := s.db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM broadcasts WHERE chain_id=$1 AND tx_hash=$2)`, chainID, txHash).Scan(&exists)
	return exists, err
}

// --- ConfirmationStore ---

type ConfirmationStore struct{ db *sql.DB }

func (s *ConfirmationStore) Upsert(ctx context.Context, c *store.Confirmation) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO tx_confirmations (chain_id, tx_hash, status, block_height, block_hash, confirmations, first_seen_at, confirmed_at, finalized_at)
		 VALUES ($1,$2,$3,$4,$5,$6,COALESCE($7,now()),$8,$9)
		 ON CONFLICT (chain_id, tx_hash) DO UPDATE SET
		   status=EXCLUDED.status, block_height=EXCLUDED.block_height, block_hash=EXCLUDED.block_hash,
		   confirmations=EXCLUDED.confirmations, confirmed_at=EXCLUDED.confirmed_at, finalized_at=EXCLUDED.finalized_at`,
		c.ChainID, c.TxHash, string(c.Status), c.BlockHeight, c.BlockHash, c.Confirmations, c.FirstSeenAt, c.ConfirmedAt, c.FinalizedAt)
	return err
}

func (s *ConfirmationStore) Get(ctx context.Context, chainID, txHash string) (*store.Confirmation, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT chain_id, tx_hash, status, block_height, block_hash, confirmations, first_seen_at, confirmed_at, finalized_at
		 FROM tx_confirmations WHERE chain_id=$1 AND tx_hash=$2`, chainID, txHash)
	var c store.Confirmation
	var status string
	if err := row.Scan(&c.ChainID, &c.TxHash, &status, &c.BlockHeight, &c.BlockHash, &c.Confirmations, &c.FirstSeenAt, &c.ConfirmedAt, &c.FinalizedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, &store.ErrNotFound{Chain: chainID, Key: txHash}
		}
		return nil, err
	}
	c.Status = chain.Status(status)
	return &c, nil
}

func (s *ConfirmationStore) ListByStatus(ctx context.Context, chainID string, status chain.Status) ([]*store.Confirmation, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT chain_id, tx_hash, status, block_height, block_hash, confirmations, first_seen_at, confirmed_at, finalized_at
		 FROM tx_confirmations WHERE chain_id=$1 AND status=$2`, chainID, string(status))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []*store.Confirmation
	for rows.Next() {
		var c store.Confirmation
		var st string
		if err := rows.Scan(&c.ChainID, &c.TxHash, &st, &c.BlockHeight, &c.BlockHash, &c.Confirmations, &c.FirstSeenAt, &c.ConfirmedAt, &c.FinalizedAt); err != nil {
			return nil, err
		}
		c.Status = chain.Status(st)
		out = append(out, &c)
	}
	return out, rows.Err()
}

func (s *ConfirmationStore) ListAboveHeight(ctx context.Context, chainID string, height uint64) ([]*store.Confirmation, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT chain_id, tx_hash, status, block_height, block_hash, confirmations, first_seen_at, confirmed_at, finalized_at
		 FROM tx_confirmations WHERE chain_id=$1 AND block_height > $2`, chainID, height)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []*store.Confirmation
	for rows.Next() {
		var c store.Confirmation
		var st string
		if err := rows.Scan(&c.ChainID, &c.TxHash, &st, &c.BlockHeight, &c.BlockHash, &c.Confirmations, &c.FirstSeenAt, &c.ConfirmedAt, &c.FinalizedAt); err != nil {
			return nil, err
		}
		c.Status = chain.Status(st)
		out = append(out, &c)
	}
	return out, rows.Err()
}

func (s *ConfirmationStore) Transition(ctx context.Context, chainID, txHash string, from, to chain.Status, mutator func(*store.Confirmation)) (*store.Confirmation, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = tx.Rollback() }()
	var c store.Confirmation
	var st string
	row := tx.QueryRowContext(ctx,
		`SELECT chain_id, tx_hash, status, block_height, block_hash, confirmations, first_seen_at, confirmed_at, finalized_at
		 FROM tx_confirmations WHERE chain_id=$1 AND tx_hash=$2 FOR UPDATE`, chainID, txHash)
	if err := row.Scan(&c.ChainID, &c.TxHash, &st, &c.BlockHeight, &c.BlockHash, &c.Confirmations, &c.FirstSeenAt, &c.ConfirmedAt, &c.FinalizedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, &store.ErrNotFound{Chain: chainID, Key: txHash}
		}
		return nil, false, err
	}
	c.Status = chain.Status(st)
	if c.Status != from {
		return nil, false, nil
	}
	if !from.CanTransitionTo(to) {
		return nil, false, fmt.Errorf("invalid transition %s -> %s", from, to)
	}
	c.Status = to
	if mutator != nil {
		mutator(&c)
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE tx_confirmations SET status=$3, block_height=$4, block_hash=$5, confirmations=$6, confirmed_at=$7, finalized_at=$8
		 WHERE chain_id=$1 AND tx_hash=$2`, chainID, txHash, string(c.Status), c.BlockHeight, c.BlockHash, c.Confirmations, c.ConfirmedAt, c.FinalizedAt); err != nil {
		return nil, false, err
	}
	if err := tx.Commit(); err != nil {
		return nil, false, err
	}
	return &c, true, nil
}

// --- TipStore ---

type TipStore struct{ db *sql.DB }

func (s *TipStore) Upsert(ctx context.Context, t *store.Tip) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO chain_tips (chain_id, tip_height, tip_hash, finalized_height, updated_at)
		 VALUES ($1,$2,$3,$4,COALESCE($5,now()))
		 ON CONFLICT (chain_id) DO UPDATE SET tip_height=EXCLUDED.tip_height, tip_hash=EXCLUDED.tip_hash,
		   finalized_height=EXCLUDED.finalized_height, updated_at=EXCLUDED.updated_at`,
		t.ChainID, t.TipHeight, t.TipHash, t.FinalizedHeight, t.UpdatedAt)
	return err
}

func (s *TipStore) Get(ctx context.Context, chainID string) (*store.Tip, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT chain_id, tip_height, tip_hash, finalized_height, updated_at FROM chain_tips WHERE chain_id=$1`, chainID)
	var t store.Tip
	if err := row.Scan(&t.ChainID, &t.TipHeight, &t.TipHash, &t.FinalizedHeight, &t.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, &store.ErrNotFound{Chain: chainID, Key: "tip"}
		}
		return nil, err
	}
	return &t, nil
}

// --- FeeStore ---

type FeeStore struct{ db *sql.DB }

func (s *FeeStore) Insert(ctx context.Context, r *store.FeeEstimateRow) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO fee_estimates (chain_id, priority, gas_limit, max_fee_per_gas, max_priority_fee_per_gas, gas_price, total_fee, sample_count, strategy, computed_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,COALESCE($10,now()))`,
		r.ChainID, string(r.Priority), r.GasLimit, bigStr(r.MaxFeePerGas), bigStr(r.MaxPriorityFeePerGas), bigStr(r.GasPrice), bigStr(r.TotalFee), r.SampleCount, r.Strategy, r.ComputedAt)
	return err
}

func (s *FeeStore) Latest(ctx context.Context, chainID string, p chain.Priority) (*store.FeeEstimateRow, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT chain_id, priority, gas_limit, max_fee_per_gas, max_priority_fee_per_gas, gas_price, total_fee, sample_count, strategy, computed_at
		 FROM fee_estimates WHERE chain_id=$1 AND priority=$2 ORDER BY computed_at DESC LIMIT 1`, chainID, string(p))
	var r store.FeeEstimateRow
	var mfp, mpf, gp, tf, prio string
	if err := row.Scan(&r.ChainID, &prio, &r.GasLimit, &mfp, &mpf, &gp, &tf, &r.SampleCount, &r.Strategy, &r.ComputedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, &store.ErrNotFound{Chain: chainID, Key: string(p)}
		}
		return nil, err
	}
	r.Priority = chain.Priority(prio)
	r.MaxFeePerGas, _ = new(big.Int).SetString(mfp, 10)
	r.MaxPriorityFeePerGas, _ = new(big.Int).SetString(mpf, 10)
	r.GasPrice, _ = new(big.Int).SetString(gp, 10)
	r.TotalFee, _ = new(big.Int).SetString(tf, 10)
	return &r, nil
}

// --- ReorgStore ---

type ReorgStore struct{ db *sql.DB }

func (s *ReorgStore) Append(ctx context.Context, e *store.ReorgEvent) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO reorg_events (chain_id, detected_at, old_tip_hash, new_tip_hash, common_ancestor_height, affected_tx_hashes)
		 VALUES ($1,COALESCE($2,now()),$3,$4,$5,$6)`,
		e.ChainID, e.DetectedAt, e.OldTipHash, e.NewTipHash, e.CommonAncestorHeight, pqArray(e.AffectedTxHashes))
	return err
}

func (s *ReorgStore) List(ctx context.Context, chainID string) ([]*store.ReorgEvent, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT chain_id, detected_at, old_tip_hash, new_tip_hash, common_ancestor_height, affected_tx_hashes
		 FROM reorg_events WHERE chain_id=$1 ORDER BY detected_at`, chainID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []*store.ReorgEvent
	for rows.Next() {
		var e store.ReorgEvent
		var arr sql.NullString
		if err := rows.Scan(&e.ChainID, &e.DetectedAt, &e.OldTipHash, &e.NewTipHash, &e.CommonAncestorHeight, &arr); err != nil {
			return nil, err
		}
		e.AffectedTxHashes = parseArray(arr.String)
		out = append(out, &e)
	}
	return out, rows.Err()
}

// --- OutboxStore ---

type OutboxStore struct{ db *sql.DB }

func (s *OutboxStore) Append(ctx context.Context, e *store.OutboxEntry) (bool, error) {
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO outbox (chain_id, tx_hash, status, block_height, event_type, payload, created_at)
		 VALUES ($1,$2,$3,$4,$5,$6,COALESCE($7,now())) ON CONFLICT (chain_id, tx_hash, status, block_height) DO NOTHING`,
		e.ChainID, e.TxHash, string(e.Status), e.BlockHeight, e.EventType, e.Payload, e.CreatedAt)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

func (s *OutboxStore) ListPending(ctx context.Context, limit int) ([]*store.OutboxEntry, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, chain_id, tx_hash, status, block_height, event_type, payload, created_at, emitted_at
		 FROM outbox WHERE emitted_at IS NULL ORDER BY id ASC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []*store.OutboxEntry
	for rows.Next() {
		var e store.OutboxEntry
		var st string
		var emitted sql.NullTime
		if err := rows.Scan(&e.ID, &e.ChainID, &e.TxHash, &st, &e.BlockHeight, &e.EventType, &e.Payload, &e.CreatedAt, &emitted); err != nil {
			return nil, err
		}
		e.Status = chain.Status(st)
		e.EmittedAt = emitted.Time
		out = append(out, &e)
	}
	return out, rows.Err()
}

func (s *OutboxStore) MarkEmitted(ctx context.Context, id int64) error {
	res, err := s.db.ExecContext(ctx, `UPDATE outbox SET emitted_at=now() WHERE id=$1`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return &store.ErrNotFound{Chain: "outbox", Key: fmt.Sprintf("%d", id)}
	}
	return nil
}

// --- helpers ---

func bigStr(b *big.Int) string {
	if b == nil {
		return "0"
	}
	return b.String()
}

// pqArray renders a Go string slice as a Postgres TEXT[] literal.
func pqArray(ss []string) string {
	out := "{"
	for i, s := range ss {
		if i > 0 {
			out += ","
		}
		out += s
	}
	out += "}"
	return out
}

// parseArray parses a Postgres TEXT[] literal back into a Go slice. It is
// intentionally tolerant: it strips braces and splits on commas.
func parseArray(s string) []string {
	s = trimBraces(s)
	if s == "" {
		return nil
	}
	var out []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	return out
}

func trimBraces(s string) string {
	for len(s) > 0 && s[0] == '{' {
		s = s[1:]
	}
	for len(s) > 0 && s[len(s)-1] == '}' {
		s = s[:len(s)-1]
	}
	return s
}