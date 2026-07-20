package eventbus

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
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

func TestNewPublisherFromURLSelectsByScheme(t *testing.T) {
	p, err := NewPublisherFromURL("")
	if err != nil {
		t.Fatalf("empty: %v", err)
	}
	if _, ok := p.(NopPublisher); !ok {
		t.Fatalf("expected NopPublisher, got %T", p)
	}

	p, err = NewPublisherFromURL("kafka://broker:9092,broker2:9092?topic=blockchain.events.v1")
	if err != nil {
		t.Fatalf("kafka: %v", err)
	}
	kp, ok := p.(*KafkaPublisher)
	if !ok {
		t.Fatalf("expected *KafkaPublisher, got %T", p)
	}
	if kp.writer == nil {
		t.Fatal("expected non-nil writer")
	}
	if kp.writer.Topic != "blockchain.events.v1" {
		t.Fatalf("expected topic blockchain.events.v1, got %q", kp.writer.Topic)
	}
	_ = kp.Close()

	p, err = NewPublisherFromURL("http://example.com/events")
	if err != nil {
		t.Fatalf("http: %v", err)
	}
	if _, ok := p.(*HTTPPublisher); !ok {
		t.Fatalf("expected *HTTPPublisher, got %T", p)
	}

	if _, err := NewPublisherFromURL("foobar://x"); err == nil {
		t.Fatal("expected error for unknown scheme")
	}
}

func TestKafkaPublisherNoBrokers(t *testing.T) {
	if _, err := NewKafkaPublisher(nil, ""); err == nil {
		t.Fatal("expected error for empty brokers")
	}
	if _, err := NewKafkaPublisherFromURL("kafka://"); err == nil {
		t.Fatal("expected error for empty brokers")
	}
}

func TestKafkaPublisherNilWriterPublish(t *testing.T) {
	var p *KafkaPublisher
	if err := p.Publish(context.Background(), Event{TxHash: "0x1"}); err == nil {
		t.Fatal("expected error on nil publisher")
	}
	p = &KafkaPublisher{}
	if err := p.Publish(context.Background(), Event{TxHash: "0x1"}); err == nil {
		t.Fatal("expected error on nil writer")
	}
	if err := p.Close(); err != nil {
		t.Fatalf("close should be no-op on nil writer: %v", err)
	}
}