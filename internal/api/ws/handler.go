// Package ws exposes the WebSocket endpoint `WS /v1/chains/:chain/heads`
// that streams head updates to external subscribers. It uses
// gorilla/websocket. Backpressure is handled by the tip.Subscriber hub
// (slow subscribers get their oldest event dropped, never blocking the
// follower).
package ws

import (
	"net/http"
	"sync"
	"time"

	"github.com/ai-crypto-onramp/blockchain-gateway/internal/chain"
	"github.com/ai-crypto-onramp/blockchain-gateway/internal/tip"

	"github.com/go-chi/chi/v5"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(_ *http.Request) bool { return true },
}

// Handler serves WS /v1/chains/{chain}/heads.
type Handler struct {
	mu        sync.Mutex
	followers map[string]*tip.Follower
}

// NewHandler returns a Handler bound to the given followers.
func NewHandler(followers map[string]*tip.Follower) *Handler {
	return &Handler{followers: followers}
}

// ServeHTTP upgrades to WebSocket and streams heads until the client
// disconnects or the connection times out.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	chainID := chi.URLParam(r, "chain")
	f, ok := h.followers[chainID]
	if !ok {
		http.Error(w, "unknown chain", http.StatusNotFound)
		return
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	sub, cancel := f.Subscriber().Subscribe(32)
	defer cancel()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case head, ok := <-sub:
			if !ok {
				return
			}
			if err := conn.WriteJSON(headToWS(head)); err != nil {
				return
			}
		case <-ticker.C:
			_ = conn.WriteMessage(websocket.PingMessage, nil)
		}
	}
}

// HeadMessage is the JSON shape streamed to WS subscribers.
type HeadMessage struct {
	Height     uint64 `json:"height"`
	Hash       string `json:"hash"`
	ParentHash string `json:"parent_hash"`
	Timestamp  int64  `json:"timestamp"`
}

func headToWS(h chain.Head) HeadMessage {
	ts := h.Timestamp.Unix()
	if ts == 0 {
		ts = time.Now().Unix()
	}
	return HeadMessage{Height: h.Height, Hash: h.Hash, ParentHash: h.ParentHash, Timestamp: ts}
}