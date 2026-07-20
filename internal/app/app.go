// Package app wires the gateway's subsystems into a single runnable HTTP
// server. It is the composition root: it loads config, builds the adapter
// registry, opens stores (in-memory by default, Postgres when DB_URL is
// set), constructs the broadcast / fee / confirmation / reorg / mempool /
// tip / provider / event-bus components, and exposes the REST + WS
// handlers.
package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ai-crypto-onramp/blockchain-gateway/internal/api/rest"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/api/ws"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/audit"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/broadcast"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/chain"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/confirmation"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/eventbus"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/fee"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/mempool"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/prepayment"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/reorg"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/store"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/store/memstore"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/store/postgres"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/tip"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/walletclient"
	"github.com/segmentio/kafka-go"

)

// Config is the top-level app configuration loaded from env.
type Config struct {
	Port             string
	WalletMgmtURL    string
	EventBusURL      string
	BroadcastTimeout time.Duration
	BroadcastRetryMax int
	ConfirmationPoll time.Duration
	FeeRefresh       time.Duration
}

// LoadConfig reads configuration from the environment.
func LoadConfig() Config {
	cfg := Config{
		Port:              envOr("PORT", "8080"),
		WalletMgmtURL:     os.Getenv("WALLET_MGMT_URL"),
		EventBusURL:       os.Getenv("EVENT_BUS_URL"),
		BroadcastTimeout:  envDur("BROADCAST_TIMEOUT", 10*time.Second),
		BroadcastRetryMax: envInt("BROADCAST_RETRY_MAX", 3),
		ConfirmationPoll:  envDur("CONFIRMATION_POLL_INTERVAL", 2*time.Second),
		FeeRefresh:        envDur("FEE_ESTIMATE_REFRESH", 15*time.Second),
	}
	return cfg
}

// Server bundles the wired gateway. Use Run to start it.
type Server struct {
	cfg       Config
	registry  *chain.Registry
	http      *http.Server
	followers map[string]*tip.Follower
	cancelMu  sync.Mutex
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	bus       *eventbus.Bus
}

// Build constructs the server from env. It prefers Postgres when DB_URL is
// set; otherwise it uses the in-memory stores, but only in DEV_MODE=1. In
// production DB_URL is required (fatal-on-missing).
func Build(cfg Config) (*Server, error) {
	devMode := os.Getenv("DEV_MODE") == "1"
	if devMode {
		log.Printf("DEV_MODE=1: stub/stand-in clients in use — NOT FOR PRODUCTION")
	}
	loader := chain.NewConfigLoader()
	cfgs, err := loader.Load()
	if err != nil {
		if !devMode {
			return nil, fmt.Errorf("CHAINS_SUPPORTED config load failed and DEV_MODE!=1: %w", err)
		}
		// Fall back to a stub-only registry so the binary still boots in
		// tests without CHAINS_SUPPORTED.
		cfgs = nil
	}
	registry := chain.NewRegistry()
	for _, c := range cfgs {
		registry.Register(buildAdapter(c))
	}
	if len(registry.Chains()) == 0 {
		if !devMode {
			return nil, fmt.Errorf("no chain adapters registered and DEV_MODE!=1; refusing to start with stub-only registry in production mode")
		}
		// Always register a stub so /healthz and basic routes work.
		registry.Register(chain.NewStubAdapter(chain.StubAdapterOptions{ChainID: "stub", FinalityBlocks: 3}))
	}

	var (
		broadcastStore    store.BroadcastStore
		confirmationStore store.ConfirmationStore
		tipStore          store.TipStore
		_                 store.FeeStore
		reorgStore        store.ReorgStore
		outboxStore       store.OutboxStore
	)
	if dsn := os.Getenv("DB_URL"); dsn != "" {
		pg, err := postgres.Open(dsn)
		if err != nil {
			return nil, fmt.Errorf("open postgres: %w", err)
		}
		broadcastStore = pg.Broadcast()
		confirmationStore = pg.Confirmation()
		tipStore = pg.Tip()
		_ = pg.Fee()
		reorgStore = pg.Reorg()
		outboxStore = pg.Outbox()
	} else {
		if !devMode {
			return nil, fmt.Errorf("DB_URL not set and DEV_MODE!=1; refusing to start in production mode")
		}
		mem := memstore.NewAll()
		broadcastStore = mem.Broadcast
		confirmationStore = mem.Confirmation
		tipStore = mem.Tip
		_ = mem.Fee
		reorgStore = mem.Reorg
		outboxStore = mem.Outbox
	}
	bus := eventbus.NewBus(outboxStore, eventbus.NopPublisher{}, "")
	// When EVENT_BUS_URL is set (kafka://...), wire a real Kafka publisher
	// instead of the Nop default.
	if cfg.EventBusURL != "" {
		if pub, err := eventbus.NewPublisherFromURL(cfg.EventBusURL); err == nil {
			bus = eventbus.NewBus(outboxStore, pub, "")
		} else {
			log.Printf("app: event bus init failed, using nop publisher: %v", err)
		}
	}
	auditLog := audit.New(bus, 1024)

	auditSink, err := newAuditSinkFromEnv(devMode)
	if err != nil {
		return nil, err
	}

	// Wire emitters: confirmation -> bus, reorg -> bus, mempool -> bus.
	emitter := &busEmitter{bus: bus, audit: auditLog, auditSink: auditSink}

	// Wallet client + prepayment manager.
	var wallet walletclient.Client
	if cfg.WalletMgmtURL != "" {
		wallet = walletclient.NewHTTPClient(cfg.WalletMgmtURL, 5*time.Second)
	} else {
		if !devMode {
			return nil, fmt.Errorf("WALLET_MGMT_URL not set and DEV_MODE!=1; refusing to start in production mode")
		}
		wallet = walletclient.NewMock("0xfunding", 0)
	}
	locks := prepayment.NewCoordinator(prepayment.NewMemRedis(), 10*time.Second, 5*time.Second)
	prepay := prepayment.NewManager(wallet, locks, 30*time.Second)

	// Confirmation tracker.
	confirmer := confirmation.NewWorkerPool(4, confirmationStore, lookupAdapter(registry), emitter)

	// Mempool watcher.
	watcher := mempool.NewWatcher(emitter, 5*time.Minute)

	// Broadcast service.
	svc := broadcast.NewService(registry, broadcastStore, confirmationStore, prepay, watcher, bus, confirmer, broadcast.Options{
		Timeout:  cfg.BroadcastTimeout,
		RetryMax: cfg.BroadcastRetryMax,
	})

	// Fee estimators per chain.
	estimators := make(map[string]*fee.Estimator)
	for _, c := range cfgs {
		estimators[c.ChainID] = fee.NewEstimator(c.ChainID, c.GasStrategy, c.FinalityBlocks)
	}

	// Tip followers per chain.
	followers := make(map[string]*tip.Follower)
	for _, c := range cfgs {
		adapter, err := registry.Get(c.ChainID)
		if err != nil {
			continue
		}
		f := tip.NewFollower(adapter, tipStore, cfg.ConfirmationPoll)
		f.SetConfirmer(confirmer)
		f.SetDetector(&detectorAdapter{det: reorg.NewDetector(tipStore, reorgStore, confirmationStore, emitter)})
		followers[c.ChainID] = f
	}
	// Stub follower so WS /v1/chains/stub/heads works in tests.
	if _, ok := followers["stub"]; !ok {
		adapter, _ := registry.Get("stub")
		if adapter != nil {
			f := tip.NewFollower(adapter, tipStore, cfg.ConfirmationPoll)
			f.SetConfirmer(confirmer)
			followers["stub"] = f
		}
	}

	deps := &rest.Deps{
		Registry:   registry,
		Broadcast:  svc,
		Estimators: estimators,
		Broadcasts: broadcastStore,
		Confirms:   confirmationStore,
		Tips:       tipStore,
		Followers:  followers,
		Bus:        bus,
		WSHandler:  ws.NewHandler(followers),
	}
	router := rest.NewRouter(deps)

	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return &Server{
		cfg:       cfg,
		registry:  registry,
		http:      srv,
		followers: followers,
		bus:       bus,
	}, nil
}

// Run starts the HTTP server and the tip-followers / confirmation workers
// and blocks until the process receives SIGINT/SIGTERM.
func (s *Server) Run() error {
	ctx, cancel := context.WithCancel(context.Background())
	s.cancelMu.Lock()
	s.cancel = cancel
	s.cancelMu.Unlock()
	s.startFollowers(ctx)
	log.Printf("blockchain-gateway listening on :%s (chains=%v)", s.cfg.Port, s.registry.Chains())
	errCh := make(chan error, 1)
	go func() { errCh <- s.http.ListenAndServe() }()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-errCh:
		return err
	case <-sig:
		return s.Shutdown()
	}
}

// StartFollowers begins the tip followers in the background. It is
// intended for tests that serve HTTP via httptest.NewServer. The caller
// should call Shutdown when done. If ctx is nil, context.Background() is
// used.
func (s *Server) StartFollowers(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	s.startFollowers(ctx)
}

func (s *Server) startFollowers(ctx context.Context) {
	for chainID, f := range s.followers {
		s.wg.Add(1)
		go func(id string, follower *tip.Follower) {
			defer s.wg.Done()
			if err := follower.Run(ctx); err != nil {
				log.Printf("tip follower %s: %v", id, err)
			}
		}(chainID, f)
	}
}

// Shutdown gracefully stops the server.
func (s *Server) Shutdown() error {
	s.cancelMu.Lock()
	if s.cancel != nil {
		s.cancel()
	}
	s.cancelMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if s.http != nil {
		if err := s.http.Shutdown(ctx); err != nil {
			return err
		}
	}
	s.wg.Wait()
	return nil
}

// Registry returns the wired adapter registry (test helper).
func (s *Server) Registry() *chain.Registry { return s.registry }

// HTTPHandler returns the wired HTTP handler (test helper).
func (s *Server) HTTPHandler() http.Handler { return s.http.Handler }

func buildAdapter(c chain.ChainConfig) chain.ChainAdapter {
	switch {
	case isEVM(c.ChainID):
		return chain.NewEVMAdapter(c)
	case c.ChainID == "solana":
		return chain.NewSolanaAdapter(c)
	case c.ChainID == "bitcoin":
		return chain.NewBitcoinAdapter(c)
	}
	return chain.NewStubAdapter(chain.StubAdapterOptions{
		ChainID:        c.ChainID,
		FinalityBlocks: c.FinalityBlocks,
	})
}

func isEVM(id string) bool {
	switch id {
	case "ethereum", "polygon", "arbitrum", "optimism", "base":
		return true
	}
	return false
}

func lookupAdapter(reg *chain.Registry) chain.ChainAdapter {
	ids := reg.Chains()
	if len(ids) == 0 {
		return nil
	}
	a, _ := reg.Get(ids[0])
	return a
}

// busEmitter adapts eventbus.Event into the confirmation/mempool/reorg
// Emitter interfaces. It implements all three Emit overloads.
type busEmitter struct {
	bus       *eventbus.Bus
	audit     *audit.Log
	auditSink *kafkaAuditSink
}

func (e *busEmitter) emitCommon(ctx context.Context, typ, chainID, txHash string, status chain.Status, blockHeight, confirmations uint64, finalizedAt time.Time) error {
	evt := eventbus.Event{
		Type:          typ,
		ChainID:       chainID,
		TxHash:        txHash,
		Status:        status,
		BlockHeight:   blockHeight,
		Confirmations: confirmations,
		FinalizedAt:   finalizedAt,
		EmittedAt:     time.Now(),
	}
	if err := e.bus.Emit(ctx, evt); err != nil {
		return err
	}
	if e.auditSink != nil {
		_ = e.auditSink.publish(ctx, evt)
	}
	return nil
}

// Emit satisfies confirmation.Emitter.
func (e *busEmitter) Emit(ctx context.Context, ev confirmation.Event) error {
	return e.emitCommon(ctx, ev.Type, ev.ChainID, ev.TxHash, ev.Status, ev.BlockHeight, ev.Confirmations, ev.FinalizedAt)
}

// EmitMempool satisfies mempool.Emitter.
func (e *busEmitter) EmitMempool(ctx context.Context, ev mempool.Event) error {
	return e.emitCommon(ctx, ev.Type, ev.ChainID, ev.TxHash, ev.Status, 0, 0, time.Time{})
}

// EmitReorg satisfies reorg.Emitter.
func (e *busEmitter) EmitReorg(ctx context.Context, ev reorg.Event) error {
	// Reorg events affect multiple txs; emit one per affected tx for the
	// outbox dedup shape (chain, tx_hash, status, block_height).
	for _, h := range ev.Affected {
		if err := e.emitCommon(ctx, ev.Type, ev.ChainID, h, chain.StatusReorgedOut, ev.CommonAncestor, 0, time.Time{}); err != nil {
			return err
		}
	}
	return nil
}

// detectorAdapter bridges tip.Detector to reorg.Detector.
type detectorAdapter struct {
	det *reorg.Detector
}

func (d *detectorAdapter) OnHead(ctx context.Context, h chain.Head) (interface{}, error) {
	return d.det.OnHead(ctx, h)
}

// --- helpers ---

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envDur(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil {
			return n
		}
	}
	return def
}

// unused symbol guards.
var _ = strings.Split
var _ = big.NewInt
var _ = log.Printf

// kafkaAuditSink publishes the canonical audit.v1 envelope (see
// .github/contracts/asyncapi/audit/v1/asyncapi.yaml) to the `audit.v1` topic for
// every blockchain lifecycle event. The blockchain Event itself is carried
// as the `payload` field.
type kafkaAuditSink struct {
	writer *kafka.Writer
}

func newKafkaAuditSink(brokers []string) (*kafkaAuditSink, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("audit kafka: no brokers provided")
	}
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        "audit.v1",
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafka.RequireAll,
	}
	return &kafkaAuditSink{writer: w}, nil
}

func (s *kafkaAuditSink) publish(ctx context.Context, evt eventbus.Event) error {
	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(payload)
	payloadHash := "sha256:" + hex.EncodeToString(sum[:])
	id := evt.TxHash
	if id == "" {
		id = fmt.Sprintf("bgw-%d", time.Now().UnixNano())
	}
	envelope := map[string]any{
		"schema_version": "1",
		"id":              id,
		"ts":              evt.EmittedAt.UTC().Format(time.RFC3339Nano),
		"source_service":  "blockchain-gateway",
		"actor_id":        "blockchain-gateway",
		"action":          string(evt.Type),
		"target_type":     "transaction",
		"target_id":       evt.TxHash,
		"payload_hash":    payloadHash,
		"payload":         json.RawMessage(payload),
	}
	body, err := json.Marshal(envelope)
	if err != nil {
		return err
	}
	return s.writer.WriteMessages(ctx, kafka.Message{Key: []byte(id), Value: body})
}

func (s *kafkaAuditSink) Close() error {
	if s.writer == nil {
		return nil
	}
	return s.writer.Close()
}

func splitCSV(s string) []string {
	out := []string{}
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func newAuditSinkFromEnv(devMode bool) (*kafkaAuditSink, error) {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		if devMode {
			log.Printf("warn: KAFKA_BROKERS unset and DEV_MODE=1; audit events will not be published to audit.v1")
			return nil, nil
		}
		return nil, fmt.Errorf("KAFKA_BROKERS unset and DEV_MODE not set; cannot start audit producer")
	}
	sink, err := newKafkaAuditSink(splitCSV(brokers))
	if err != nil {
		if devMode {
			log.Printf("warn: audit kafka init failed (DEV_MODE): %v", err)
			return nil, nil
		}
		return nil, err
	}
	return sink, nil
}