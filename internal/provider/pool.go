// Package provider implements RPC provider failover: a per-chain pool over
// rpc_urls / ws_urls with health checks, round-robin reads, primary-preferring
// writes, and per-provider circuit breakers. Failover is gated by
// RPC_PROVIDER_FAILOVER (default true).
package provider

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// ErrNoHealthyProvider is returned when all providers are tripped.
var ErrNoHealthyProvider = errors.New("no healthy rpc provider")

// ErrCircuitOpen is returned when a provider's circuit breaker is open.
var ErrCircuitOpen = errors.New("circuit breaker open")

// Provider is one RPC endpoint.
type Provider struct {
	URL     string
	primary bool
}

// breaker is a simple circuit breaker: trips after `threshold` consecutive
// failures, half-opens after `coolDown`, and closes on a successful probe.
type breaker struct {
	mu           sync.Mutex
	failures     int
	threshold    int
	coolDown     time.Duration
	openedAt     time.Time
	halfOpen     bool
}

func newBreaker(threshold int, coolDown time.Duration) *breaker {
	if threshold <= 0 {
		threshold = 5
	}
	if coolDown <= 0 {
		coolDown = 10 * time.Second
	}
	return &breaker{threshold: threshold, coolDown: coolDown}
}

func (b *breaker) allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failures < b.threshold {
		return true
	}
	if time.Since(b.openedAt) > b.coolDown {
		b.halfOpen = true
		return true
	}
	return false
}

func (b *breaker) onSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures = 0
	b.halfOpen = false
}

func (b *breaker) onFailure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures++
	if b.failures >= b.threshold {
		b.openedAt = time.Now()
		b.halfOpen = false
	}
}

// State reports the breaker's current state for metrics.
func (b *breaker) State() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failures < b.threshold {
		return "closed"
	}
	if time.Since(b.openedAt) > b.coolDown {
		return "half-open"
	}
	return "open"
}

// Pool is a per-chain provider pool. It is safe for concurrent use.
type Pool struct {
	mu          sync.Mutex
	providers   []*entry
	rr          uint64 // round-robin counter for reads
	failover    bool
	writeIdx    int
	failoverCnt int64
}

type entry struct {
	p        Provider
	breaker  *breaker
	healthy  int32 // atomic, 1 = healthy
}

// NewPool returns a Pool. The first provider is the primary.
func NewPool(urls []string, failover bool) *Pool {
	es := make([]*entry, 0, len(urls))
	for i, u := range urls {
		es = append(es, &entry{
			p:       Provider{URL: u, primary: i == 0},
			breaker: newBreaker(5, 10*time.Second),
			healthy: 1,
		})
	}
	return &Pool{providers: es, failover: failover, writeIdx: 0}
}

// ForWrite returns the next healthy provider for a write. Writes prefer the
// primary; on failure fail over to the next. If failover is disabled, only
// the primary is returned and ErrCircuitOpen is surfaced immediately.
func (p *Pool) ForWrite() (Provider, func(error), error) {
	return p.pick(true)
}

// ForRead returns the next healthy provider for a read using round-robin
// across healthy providers.
func (p *Pool) ForRead() (Provider, func(error), error) {
	return p.pick(false)
}

func (p *Pool) pick(write bool) (Provider, func(error), error) {
	if len(p.providers) == 0 {
		return Provider{}, nil, ErrNoHealthyProvider
	}
	if write {
		// Primary first.
		if e := p.providers[0]; e.breaker.allow() {
			return e.p, e.doneFunc(), nil
		}
		if !p.failover {
			return Provider{}, nil, ErrCircuitOpen
		}
		// Fail over.
		for i := 1; i < len(p.providers); i++ {
			if e := p.providers[i]; e.breaker.allow() {
				atomic.AddInt64(&p.failoverCnt, 1)
				return e.p, e.doneFunc(), nil
			}
		}
		return Provider{}, nil, ErrNoHealthyProvider
	}
	// Read: round-robin.
	n := len(p.providers)
	start := int(atomic.AddUint64(&p.rr, 1) % uint64(n))
	for i := 0; i < n; i++ {
		idx := (start + i) % n
		if e := p.providers[idx]; e.breaker.allow() {
			return e.p, e.doneFunc(), nil
		}
	}
	return Provider{}, nil, ErrNoHealthyProvider
}

func (e *entry) doneFunc() func(error) {
	return func(err error) {
		if err != nil {
			atomic.StoreInt32(&e.healthy, 0)
			e.breaker.onFailure()
			return
		}
		atomic.StoreInt32(&e.healthy, 1)
		e.breaker.onSuccess()
	}
}

// FailoverCount returns the total number of write failovers (test/metrics).
func (p *Pool) FailoverCount() int64 { return atomic.LoadInt64(&p.failoverCnt) }

// Healthy reports whether a provider URL is currently healthy.
func (p *Pool) Healthy(url string) bool {
	for _, e := range p.providers {
		if e.p.URL == url {
			return atomic.LoadInt32(&e.healthy) == 1
		}
	}
	return false
}

// BreakerState returns the breaker state for a provider URL.
func (p *Pool) BreakerState(url string) string {
	for _, e := range p.providers {
		if e.p.URL == url {
			return e.breaker.State()
		}
	}
	return "unknown"
}

// URLs returns the configured provider URLs.
func (p *Pool) URLs() []string {
	out := make([]string, len(p.providers))
	for i, e := range p.providers {
		out[i] = e.p.URL
	}
	return out
}