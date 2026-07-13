// Package prepayment provides per-sender, per-chain nonce coordination using a
// distributed mutex and a next-nonce counter cache. The RedisClient
// interface abstracts the Redis backend so unit tests can use an in-memory
// implementation; integration tests use the real go-redis client.
package prepayment

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// RedisClient is the subset of go-redis used by the nonce coordinator.
type RedisClient interface {
	// SetNX sets key=value only if key does not exist; returns true if set.
	SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error)
	// Set sets key=value unconditionally with ttl.
	Set(ctx context.Context, key, value string, ttl time.Duration) error
	// Del removes key and returns the number of keys deleted.
	Del(ctx context.Context, key string) (int64, error)
	// Get returns the value at key, or ErrNoKey if missing.
	Get(ctx context.Context, key string) (string, error)
	// Incr atomically increments key and returns the new value.
	Incr(ctx context.Context, key string) (int64, error)
}

// ErrNoKey is returned by RedisClient.Get when the key is absent.
var ErrNoKey = errors.New("redis: no key")

// ErrLockBusy is returned when the nonce mutex cannot be acquired.
var ErrLockBusy = errors.New("nonce lock busy")

// Coordinator hands out nonces for (chain, addr) pairs under a distributed
// mutex. It is safe for concurrent use.
type Coordinator struct {
	redis     RedisClient
	lockTTL   time.Duration
	lockWait  time.Duration
}

// NewCoordinator returns a Coordinator. lockTTL bounds the mutex lifetime;
// lockWait bounds how long Acquire waits before giving up.
func NewCoordinator(r RedisClient, lockTTL, lockWait time.Duration) *Coordinator {
	if lockTTL <= 0 {
		lockTTL = 10 * time.Second
	}
	if lockWait <= 0 {
		lockWait = 5 * time.Second
	}
	return &Coordinator{redis: r, lockTTL: lockTTL, lockWait: lockWait}
}

func lockKey(chainID, addr string) string {
	return fmt.Sprintf("nonce:lock:%s:%s", chainID, addr)
}

func nextKey(chainID, addr string) string {
	return fmt.Sprintf("nonce:next:%s:%s", chainID, addr)
}

// AcquireLock blocks until it obtains the nonce mutex for (chainID, addr)
// or ctx/lockWait expires. The returned release func MUST be called.
func (c *Coordinator) AcquireLock(ctx context.Context, chainID, addr string) (func(), error) {
	deadline := time.Now().Add(c.lockWait)
	key := lockKey(chainID, addr)
	token := fmt.Sprintf("%d", time.Now().UnixNano())
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		ok, err := c.redis.SetNX(ctx, key, token, c.lockTTL)
		if err != nil {
			return nil, err
		}
		if ok {
			release := func() {
				_, _ = c.redis.Del(context.Background(), key)
			}
			return release, nil
		}
		if time.Now().After(deadline) {
			return nil, ErrLockBusy
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

// NextNonce returns the next nonce for (chainID, addr). The caller MUST
// hold the nonce lock. The first call seeds the counter from the supplied
// seedFromWallet function (which calls Wallet Management in production).
func (c *Coordinator) NextNonce(ctx context.Context, chainID, addr string, seedFromWallet func(ctx context.Context) (uint64, error)) (uint64, error) {
	key := nextKey(chainID, addr)
	val, err := c.redis.Get(ctx, key)
	if err == nil {
		var cur uint64
		if _, e := fmt.Sscanf(val, "%d", &cur); e == nil {
			next := cur + 1
			_ = setKey(ctx, c.redis, key, fmt.Sprintf("%d", next))
			return cur, nil
		}
	}
	if !errors.Is(err, ErrNoKey) && err != nil {
		return 0, err
	}
	// Seed from wallet management.
	seed, err := seedFromWallet(ctx)
	if err != nil {
		return 0, err
	}
	_ = setKey(ctx, c.redis, key, fmt.Sprintf("%d", seed+1))
	return seed, nil
}

func setKey(ctx context.Context, r RedisClient, key, val string) error {
	return r.Set(ctx, key, val, 24*time.Hour)
}

// --- in-memory RedisClient for tests ---

// MemRedis is an in-memory RedisClient for unit tests. It is safe for
// concurrent use.
type MemRedis struct {
	mu   sync.Mutex
	data map[string]string
	exp  map[string]time.Time
}

// NewMemRedis returns an empty in-memory Redis client.
func NewMemRedis() *MemRedis {
	return &MemRedis{data: make(map[string]string), exp: make(map[string]time.Time)}
}

func (m *MemRedis) SetNX(_ context.Context, key, value string, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if v, ok := m.data[key]; ok {
		if exp, hasExp := m.exp[key]; hasExp && time.Now().Before(exp) {
			_ = v
			return false, nil
		}
	}
	m.data[key] = value
	if ttl > 0 {
		m.exp[key] = time.Now().Add(ttl)
	} else {
		delete(m.exp, key)
	}
	return true, nil
}

func (m *MemRedis) Set(_ context.Context, key, value string, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = value
	if ttl > 0 {
		m.exp[key] = time.Now().Add(ttl)
	} else {
		delete(m.exp, key)
	}
	return nil
}

func (m *MemRedis) Del(_ context.Context, key string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.data[key]; ok {
		delete(m.data, key)
		delete(m.exp, key)
		return 1, nil
	}
	return 0, nil
}

func (m *MemRedis) Get(_ context.Context, key string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if exp, hasExp := m.exp[key]; hasExp && !time.Now().Before(exp) {
		delete(m.data, key)
		delete(m.exp, key)
	}
	v, ok := m.data[key]
	if !ok {
		return "", ErrNoKey
	}
	return v, nil
}

func (m *MemRedis) Incr(_ context.Context, key string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var cur int64
	if v, ok := m.data[key]; ok {
		_, _ = fmt.Sscanf(v, "%d", &cur)
	}
	cur++
	m.data[key] = fmt.Sprintf("%d", cur)
	return cur, nil
}