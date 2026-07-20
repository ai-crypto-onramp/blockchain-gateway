// Package eventbus publishes tx lifecycle events to Notification and
// Reconciliation via an at-least-once outbox deduped by
// (chain, tx_hash, status, block_height). Audit emission is handled by
// the app layer's kafkaAuditSink (publishing the canonical audit.v1
// envelope) — the bus no longer posts to AUDIT_EVENT_LOG_URL.
package eventbus

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ai-crypto-onramp/blockchain-gateway/internal/chain"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/store"
	"github.com/segmentio/kafka-go"
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
	outbox  store.OutboxStore
	bus     Publisher
	mu      sync.Mutex
	deduped int64
	emitted int64
	failed  int64
}

// NewBus returns a Bus. bus may be nil (no external publisher).
func NewBus(outbox store.OutboxStore, bus Publisher, _ string) *Bus {
	return &Bus{
		outbox: outbox,
		bus:    bus,
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
	return nil
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

// HTTPPublisher POSTs events to a broker URL (e.g. REST proxy).
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

// NewPublisherFromURL selects a Publisher based on the URL scheme:
//   - ""                       -> NopPublisher
//   - "kafka://host:9092[?topic=t]" -> KafkaPublisher
//   - "http://" or "https://"  -> HTTPPublisher
//
// Any other scheme returns an error.
func NewPublisherFromURL(url string) (Publisher, error) {
	switch {
	case url == "":
		return NopPublisher{}, nil
	case strings.HasPrefix(url, "kafka://"):
		return NewKafkaPublisherFromURL(url)
	case strings.HasPrefix(url, "http://") || strings.HasPrefix(url, "https://"):
		return NewHTTPPublisher(url), nil
	default:
		return nil, fmt.Errorf("eventbus: unknown scheme in %q (use kafka:// or http://)", url)
	}
}

// KafkaPublisher publishes blockchain lifecycle events to a Kafka topic.
// It implements Publisher.
type KafkaPublisher struct {
	writer *kafka.Writer
}

// NewKafkaPublisher returns a KafkaPublisher targeting the given brokers and
// topic. Events are keyed by tx_hash so consumers receive per-tx ordering.
func NewKafkaPublisher(brokers []string, topic string) (*KafkaPublisher, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("eventbus kafka: no brokers provided")
	}
	if topic == "" {
		topic = "blockchain.events.v1"
	}
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafka.RequireAll,
	}
	return &KafkaPublisher{writer: w}, nil
}

// NewKafkaPublisherFromURL parses a "kafka://host:9092[,host2][?topic=t]" URL
// and returns a KafkaPublisher.
func NewKafkaPublisherFromURL(url string) (*KafkaPublisher, error) {
	rest := strings.TrimPrefix(url, "kafka://")
	topic := ""
	if i := strings.Index(rest, "?"); i >= 0 {
		q := rest[i+1:]
		rest = rest[:i]
		for _, kv := range strings.Split(q, "&") {
			if strings.HasPrefix(kv, "topic=") {
				topic = strings.TrimPrefix(kv, "topic=")
			}
		}
	}
	brokers := strings.Split(rest, ",")
	clean := brokers[:0]
	for _, b := range brokers {
		b = strings.TrimSpace(b)
		if b != "" {
			clean = append(clean, b)
		}
	}
	return NewKafkaPublisher(clean, topic)
}

// Publish writes the encoded event to Kafka, keyed by tx_hash.
func (p *KafkaPublisher) Publish(ctx context.Context, e Event) error {
	if p == nil || p.writer == nil {
		return fmt.Errorf("eventbus kafka: not connected")
	}
	body, err := json.Marshal(e)
	if err != nil {
		return err
	}
	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(e.TxHash),
		Value: body,
	})
}

// Close flushes and closes the underlying writer.
func (p *KafkaPublisher) Close() error {
	if p == nil || p.writer == nil {
		return nil
	}
	return p.writer.Close()
}