// Command migrate applies or reverts the embedded database migrations for
// blockchain-gateway. The migration is a single SQL file
// (internal/store/postgres/migrations/001_init.sql) made up of idempotent
// CREATE TABLE IF NOT EXISTS statements, so --up re-applies it safely.
// It reuses the embedded SQL exposed by internal/store/postgres.
//
// Usage:
//
//	migrate --up     apply the init schema (reads DB_URL)
//	migrate --down   drop all gateway tables (reads DB_URL)
//
// Run with `go run ./cmd/migrate --up` (local dev) or `make migrate-up`.
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"time"

	_ "github.com/lib/pq"

	"github.com/ai-crypto-onramp/blockchain-gateway/internal/store/postgres"
)

// dropSQL drops all tables created by the init schema, in reverse
// dependency order. It is idempotent.
const dropSQL = `
DROP INDEX IF EXISTS outbox_pending_idx;
DROP TABLE IF EXISTS outbox;
DROP INDEX IF EXISTS reorg_events_chain_id_idx;
DROP TABLE IF EXISTS reorg_events;
DROP TABLE IF EXISTS fee_estimates;
DROP TABLE IF EXISTS chain_tips;
DROP TABLE IF EXISTS tx_confirmations;
DROP TABLE IF EXISTS broadcasts;
`

func main() {
	up := flag.Bool("up", false, "apply the init schema")
	down := flag.Bool("down", false, "drop all gateway tables")
	flag.Parse()
	if !*up && !*down {
		fmt.Fprintln(os.Stderr, "usage: migrate [--up|--down]")
		os.Exit(2)
	}

	dsn := os.Getenv("DB_URL")
	if dsn == "" {
		fmt.Fprintln(os.Stderr, "DB_URL is required")
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open db:", err)
		os.Exit(1)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "ping db:", err)
		os.Exit(1)
	}

	switch {
	case *up:
		if _, err := db.ExecContext(ctx, postgres.InitSQL()); err != nil {
			fmt.Fprintln(os.Stderr, "migrate up:", err)
			os.Exit(1)
		}
		fmt.Println("migrations applied")
	case *down:
		if _, err := db.ExecContext(ctx, dropSQL); err != nil {
			fmt.Fprintln(os.Stderr, "migrate down:", err)
			os.Exit(1)
		}
		fmt.Println("schema dropped")
	}
}