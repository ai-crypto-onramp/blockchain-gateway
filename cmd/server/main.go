// Command blockchain-gateway is the on-chain execution boundary of the
// crypto on-ramp. It broadcasts signed transactions per chain, estimates
// gas/fees, prepays gas from hot wallets via Wallet Management, tracks
// confirmations to a configurable depth, detects and handles reorgs
// against block finality, monitors the mempool for the service's own
// transactions, and exposes a uniform ChainAdapter interface across EVM
// chains, Solana, Bitcoin, and others.
//
// Run with `go run .` (local dev) or `make run`. See README.md for the
// full configuration surface.
package main

import (
	"log"

	"github.com/ai-crypto-onramp/blockchain-gateway/internal/app"
)

func main() {
	cfg := app.LoadConfig()
	srv, err := app.Build(cfg)
	if err != nil {
		log.Fatalf("build: %v", err)
	}
	if err := srv.Run(); err != nil {
		log.Fatalf("run: %v", err)
	}
}