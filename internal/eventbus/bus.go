// Package eventbus publishes tx lifecycle events to Notification,
// Reconciliation, and the Audit Event Log via an at-least-once outbox
// deduped by (chain, tx_hash, status, block_height). When the bus is
// unavailable, events fall back to a synchronous POST to
// AUDIT_EVENT_LOG_URL with retry/backoff.
package eventbus

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

	"github.com/ai-crypto-onramp/blockchain-gateway/internal/chain"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/store"
)

// Event is the canonical event schema emitted by the gateway.
type Event struct {
	Type          string       `json:"type"`
	ChainID       string       `json:"chain_id"`
	TxHash        string       `json:"tx_hash"`
	From          string       `json:"from,omitempty"`
	To            string       `json:"to,omitempty"`
	Value         string       `json:"value,omitempty"`
	Fee           string       `json:"fee,omitempty"`
	Status        chain.Status `json:"status"`
	BlockHeight   uint64       `json:"block_height"`
	BlockHash     string       `json:"block_hash,omitempty"`
	Confirmations uint64       `json:"confirmations"`
	FinalizedAt   time.Time    `json:"finalized_at,omitempty"`
	EmittedAt     time.Time    `json:"emitted_at"`
}

// Publisher is the bus surface used by the gateway.
type Publisher interface {
	Publish(ctx context.Context, evt Event) error
}

// Bus is the at-least-once event bus with an outbox for dedup. It is safe
// for concurrent use.
type Bus struct {
	outbox     store.OutboxStore
	bus        Publisher
	auditURL   string
	httpClient *http.Client
	mu         sync.Mutex
	deduped    int64
	emitted    int64
	failed     int64
}

// NewBus returns a Bus. bus may be nil (audit fallback only). auditURL is
// the synchronous fallback endpoint.
func NewBus(outbox store.OutboxStore, bus Publisher, auditURL string) *Bus {
	return &Bus{
		outbox:     outbox,
		bus:        bus,
		auditURL:   auditURL,
		httpClient: &http.Client{Timeout: 5 * time.Second},
	}
}

// Emit appends an event to the outbox (deduped) and attempts to publish
// it. Returns true if the event was newly inserted into the outbox.
func (b *Bus) Emit(ctx context.Context, e Event) error {
	if e.EmittedAt.IsZero() {
		e.EmittedAt = time.Now()
	}
	payload, err := json.Marshal(e)
	if err != nil {
		return err
	}
	inserted, err := b.outbox.Append(ctx, &store.OutboxEntry{
		ChainID:     e.ChainID,
		TxHash:      e.TxHash,
		Status:      e.Status,
		BlockHeight: e.BlockHeight,
		EventType:   e.Type,
		Payload:     payload,
	})
	if err != nil {
		b.markFailed()
		return err
	}
	if !inserted {
		b.markDeduped()
		return nil
	}
	if err := b.publish(ctx, e); err != nil {
		b.markFailed()
		return err
	}
	b.markEmitted()
	return nil
}

func (b *Bus) publish(ctx context.Context, e Event) error {
	if b.bus != nil {
		if err := b.bus.Publish(ctx, e); err == nil {
			return nil
		}
	}
	// Fallback: synchronous POST to AUDIT_EVENT_LOG_URL with retry/backoff.
	if b.auditURL == "" {
		return nil
	}
	body, _ := json.Marshal(e)
	backoff := 200 * time.Millisecond
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.auditURL, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := b.httpClient.Do(req)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode < 400 {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
	}
	return errors.New("audit fallback exhausted")
}

func (b *Bus) markDeduped()  { b.mu.Lock(); b.deduped++; b.mu.Unlock() }
func (b *Bus) markEmitted()  { b.mu.Lock(); b.emitted++; b.mu.Unlock() }
func (b *Bus) markFailed()   { b.mu.Lock(); b.failed++; b.mu.Unlock() }

// Stats returns dedup/emitted/failed counters (test/metrics helper).
func (b *Bus) Stats() (emitted, deduped, failed int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.emitted, b.deduped, b.failed
}

// --- inline Publisher implementations ---

// NopPublisher is a Publisher that always succeeds (test/noop default).
type NopPublisher struct{}

// Publish returns nil.
func (NopPublisher) Publish(_ context.Context, _ Event) error { return nil }

// HTTPPublisher POSTs events to a broker URL (e.g. NATS REST proxy).
type HTTPPublisher struct {
	URL    string
	Client *http.Client
}

// NewHTTPPublisher returns an HTTPPublisher.
func NewHTTPPublisher(url string) *HTTPPublisher {
	return &HTTPPublisher{URL: url, Client: &http.Client{Timeout: 5 * time.Second}}
}

// Publish POSTs the event JSON to the broker.
func (p *HTTPPublisher) Publish(ctx context.Context, e Event) error {
	body, err := json.Marshal(e)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.Client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("event bus http %d", resp.StatusCode)
	}
	return nil
}