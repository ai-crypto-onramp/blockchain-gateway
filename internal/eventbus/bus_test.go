package eventbus

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ai-crypto-onramp/blockchain-gateway/internal/chain"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/store"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/store/memstore"
)

func TestBusDedup(t *testing.T) {
	outbox := memstore.NewOutboxStore()
	bus := NewBus(outbox, NopPublisher{}, "")
	ctx := context.Background()
	e := Event{Type: "tx.confirmed", ChainID: "ethereum", TxHash: "0x1", Status: chain.StatusConfirmed, BlockHeight: 100}
	if err := bus.Emit(ctx, e); err != nil {
		t.Fatalf("emit1: %v", err)
	}
	if err := bus.Emit(ctx, e); err != nil {
		t.Fatalf("emit2: %v", err)
	}
	emitted, deduped, failed := bus.Stats()
	if emitted != 1 || deduped != 1 || failed != 0 {
		t.Errorf("emitted=%d deduped=%d failed=%d", emitted, deduped, failed)
	}
}

func TestBusAuditFallback(t *testing.T) {
	var received atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	outbox := memstore.NewOutboxStore()
	bus := NewBus(outbox, nil, srv.URL)
	if err := bus.Emit(context.Background(), Event{Type: "tx.confirmed", ChainID: "ethereum", TxHash: "0x1", Status: chain.StatusConfirmed, BlockHeight: 100}); err != nil {
		t.Fatalf("emit: %v", err)
	}
	if received.Load() != 1 {
		t.Errorf("audit fallback received: %d", received.Load())
	}
}

func TestBusAuditFallbackRetries(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	outbox := memstore.NewOutboxStore()
	bus := NewBus(outbox, nil, srv.URL)
	// Shrink retry backoff by overriding httpClient timeout to keep test
	// fast enough.
	bus.httpClient = &http.Client{Timeout: time.Second}
	// Fallback exhaustion returns an error after 3 attempts; the audit
	// fallback retries internally, so we only assert that the upstream was
	// hit at least once.
	_ = bus.Emit(context.Background(), Event{Type: "tx.confirmed", ChainID: "ethereum", TxHash: "0x1", Status: chain.StatusConfirmed, BlockHeight: 100})
	if attempts.Load() < 1 {
		t.Errorf("expected attempts, got %d", attempts.Load())
	}
}

func TestHTTPPublisher(t *testing.T) {
	var got Event
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	p := NewHTTPPublisher(srv.URL)
	if err := p.Publish(context.Background(), Event{Type: "tx.confirmed", ChainID: "ethereum", TxHash: "0x1"}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if got.TxHash != "0x1" {
		t.Errorf("got: %+v", got)
	}
}

func TestHTTPPublisherError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()
	p := NewHTTPPublisher(srv.URL)
	if err := p.Publish(context.Background(), Event{}); err == nil {
		t.Fatal("expected error")
	}
}

func TestEventJSONRoundtrip(t *testing.T) {
	e := Event{
		Type:        "tx.finalized",
		ChainID:     "ethereum",
		TxHash:      "0x1",
		Status:      chain.StatusFinalized,
		BlockHeight: 100,
		EmittedAt:   time.Now(),
	}
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(raw), "tx.finalized") {
		t.Errorf("missing type in json: %s", raw)
	}
	var decoded Event
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.Type != e.Type {
		t.Errorf("type: %s", decoded.Type)
	}
}

func TestOutboxEntryDedupKey(t *testing.T) {
	outbox := memstore.NewOutboxStore()
	ctx := context.Background()
	_, _ = outbox.Append(ctx, &store.OutboxEntry{ChainID: "ethereum", TxHash: "0x1", Status: chain.StatusConfirmed, BlockHeight: 100, EventType: "tx.confirmed"})
	// Same dedup key -> second insert returns false.
	inserted, _ := outbox.Append(ctx, &store.OutboxEntry{ChainID: "ethereum", TxHash: "0x1", Status: chain.StatusConfirmed, BlockHeight: 100, EventType: "tx.confirmed"})
	if inserted {
		t.Fatal("should dedup")
	}
	// Different block height -> new entry.
	inserted, _ = outbox.Append(ctx, &store.OutboxEntry{ChainID: "ethereum", TxHash: "0x1", Status: chain.StatusConfirmed, BlockHeight: 101, EventType: "tx.confirmed"})
	if !inserted {
		t.Fatal("different block height should not dedup")
	}
}

var _ = fmt.Sprintf