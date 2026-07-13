// Package audit adapts the event bus into the Audit Event Log surface,
// providing an append-only audit trail for every broadcast, confirmation,
// and reorg. It is a thin wrapper over eventbus.Bus that records a local
// in-memory ring of recent audit entries (useful for diagnostics).
package audit

import (
	"context"
	"sync"

	"github.com/ai-crypto-onramp/blockchain-gateway/internal/eventbus"
)

// Log records audit events locally and forwards them to the event bus.
type Log struct {
	mu       sync.Mutex
	recent   []eventbus.Event
	max      int
	bus      *eventbus.Bus
}

// New returns a Log that forwards to bus.
func New(bus *eventbus.Bus, maxRecent int) *Log {
	if maxRecent <= 0 {
		maxRecent = 1024
	}
	return &Log{max: maxRecent, bus: bus}
}

// Record forwards evt to the bus and keeps a local copy.
func (l *Log) Record(ctx context.Context, evt eventbus.Event) error {
	if l.bus != nil {
		if err := l.bus.Emit(ctx, evt); err != nil {
			return err
		}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.recent = append(l.recent, evt)
	if len(l.recent) > l.max {
		l.recent = l.recent[len(l.recent)-l.max:]
	}
	return nil
}

// Recent returns a copy of the most recent audit entries.
func (l *Log) Recent() []eventbus.Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]eventbus.Event, len(l.recent))
	copy(out, l.recent)
	return out
}