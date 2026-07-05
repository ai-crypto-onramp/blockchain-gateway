# Blockchain Gateway

![CI](https://github.com/ai-crypto-onramp/blockchain-gateway/actions/workflows/ci.yml/badge.svg)
[![codecov](https://codecov.io/gh/ai-crypto-onramp/blockchain-gateway/branch/main/graph/badge.svg)](https://codecov.io/gh/ai-crypto-onramp/blockchain-gateway)

Per-chain broadcast, gas prepayment/estimation, confirmation tracking, reorg handling, and mempool monitoring for the crypto on-ramp.

## Overview / Responsibilities

The Blockchain Gateway is the on-chain execution boundary of the on-ramp. It is the
only service that talks to blockchain nodes for transaction broadcast and
confirmation tracking. It is called synchronously by the **Transaction
Orchestrator** during the payment saga and asynchronously emits confirmation,
reorg, and fee events to Notification, Reconciliation, and the Audit Event Log.

Core responsibilities:

- Broadcast signed transactions per chain.
- Estimate gas/fees per chain and EIP-1559 parameters where applicable.
- Prepay gas from hot wallets via Wallet Management.
- Track confirmations to a configurable depth per chain.
- Detect and handle reorgs against block finality.
- Monitor the mempool for the service's own transactions.
- Coordinate nonces across concurrent broadcasts.
- Follow the chain tip and expose tip/streaming head updates.
- Support multiple chains (EVM chains, Solana, Bitcoin, and others) through a
  uniform adapter pattern.
- Expose a consistent transaction status lifecycle:
  `broadcast -> mempool -> confirmed -> finalized`.

## Language & Tech Stack

- **Language:** Go (concurrency, low-latency I/O, ops maturity).
- **Architecture:** per-chain adapter pattern. Every chain implements a common
  `ChainAdapter` interface; the core service is chain-agnostic.
- **RPC providers:** Alchemy, Infura, QuickNode (multi-provider, failover),
  plus optionally self-hosted nodes for high-value chains.
- **Transports:** JSON-RPC over HTTP for broadcast/queries; WebSocket for
  `newHeads` and mempool subscriptions.
- **Observability:** structured logging, Prometheus metrics, OpenTelemetry
  tracing.

## System Requirements

### Functional

1. **Broadcast signed tx per chain.** Accept an already-signed transaction from
   the Transaction Orchestrator and submit it to the relevant chain's mempool.
   Never sign; only broadcast.
2. **Gas / fee estimation.** For EVM chains, produce EIP-1559 parameters
   (`maxFeePerGas`, `maxPriorityFeePerGas`) plus legacy `gasPrice`. For
   non-EVM chains, produce the chain-native fee model. Support priority tiers
   (`low`, `standard`, `high`).
3. **Gas prepayment from hot wallet.** Coordinate with Wallet Management to
   fund the sender account with sufficient native asset for fees before
   broadcast.
4. **Confirmation tracking with configurable depth per chain.** Track
   confirmations up to `FINALITY_BLOCKS_<CHAIN>` and expose intermediate
   counts.
5. **Reorg handling.** Detect chain reorganizations against block finality.
   On reorg, re-evaluate transactions whose confirmations were within the
   reorg window and re-broadcast if they fell out.
6. **Mempool monitoring for own txs.** Subscribe to mempool entry/exit events
   and flag own transactions that are dropped or replaced.
7. **Nonce coordination.** Maintain per-sender, per-chain nonce counters with
   locking to prevent nonce reuse under concurrency.
8. **Chain-tip follower.** Maintain the current tip per chain and stream new
   heads to internal consumers.
9. **Multi-chain support.** EVM chains (Ethereum, Polygon, Arbitrum, Optimism,
   Base, etc.), Solana, Bitcoin, and others behind the common adapter
   interface.
10. **Tx status lifecycle.** Track and expose a single state machine:
    `broadcast -> mempool -> confirmed -> finalized`, plus terminal error
    states (`dropped`, `replaced`, `reorged_out`, `failed`).

### Non-Functional

- **Broadcast latency:** p99 < 500ms after signing (RPC submit to
  `tx_hash` returned).
- **Confirmation detection:** p99 < one block time from block containing the tx
  to confirmation recorded.
- **Reorg safety margin:** never mark a tx `confirmed` before
  `FINALITY_BLOCKS_<CHAIN>`; never mark `finalized` before chain finality.
- **RPC provider failover:** automatic, transparent failover across configured
  providers; no broadcast lost on a single-provider outage.
- **Availability:** 99.99% for confirmation tracking (read path), 99.95% for
  broadcast (write path).
- **Scalability:** horizontally scalable stateless broadcast tier; sticky
  confirmation workers keyed by `(chain, tx_hash)` for exactly-once status
  updates.
- **Idempotency:** broadcasting the same signed tx is idempotent (same
  `tx_hash`).

## Technical Specifications

### API Surface

Two external surfaces:

- **REST** — used by the Transaction Orchestrator and internal services.
- **gRPC** — internal-only, used for high-throughput streaming (head
  subscriptions, confirmation streams).

All chain-specific behavior is encapsulated behind a common `ChainAdapter`
interface:

```go
type ChainAdapter interface {
    ChainID() string
    Broadcast(ctx context.Context, signedTx []byte) (txHash string, err error)
    GetTx(ctx context.Context, txHash string) (*Tx, error)
    GetTxStatus(ctx context.Context, txHash string) (*TxStatus, error)
    EstimateFee(ctx context.Context, req FeeEstimateReq) (*FeeEstimate, error)
    Height(ctx context.Context) (uint64, error)
    Balance(ctx context.Context, addr string) (*big.Int, error)
    SubscribeHeads(ctx context.Context) (<-chain.Head, func(), error)
    SubscribeMempool(ctx context.Context, ownAddrs []string) (<-chain.MempoolEvent, func(), error)
    FinalityBlocks() uint64
}
```

### Endpoints

| Method | Path | Body / Params | Returns |
|---|---|---|---|
| POST | `/v1/chains/:chain/broadcast` | `{ "signed_tx": "<hex|base64>" }` | `{ "tx_hash": "0x..." }` |
| GET | `/v1/chains/:chain/tx/:hash` | — | `{ "tx_hash", "status", "block_height", "confirmations", "from", "to", "value", "fee", "raw" }` |
| GET | `/v1/chains/:chain/tx/:hash/status` | — | `{ "status": "mempool\|confirmed\|finalized\|dropped\|replaced\|reorged_out\|failed", "confirmations": N, "finalized_at": <ts\|null> }` |
| POST | `/v1/chains/:chain/estimate-fee` | `{ "to", "amount", "priority": "low\|standard\|high" }` | `{ "gas_limit", "max_fee_per_gas", "max_priority_fee_per_gas", "gas_price", "total_fee", "priority" }` |
| GET | `/v1/chains/:chain/height` | — | `{ "height": N, "hash": "0x...", "finalized_height": N }` |
| GET | `/v1/chains/:chain/address/:addr/balance` | — | `{ "address", "balance", "decimals", "symbol" }` |
| WS | `/v1/chains/:chain/heads` | subscribe | stream of `{ "height", "hash", "parent_hash", "timestamp" }` |

### Data Model

Persisted in PostgreSQL:

- **`broadcasts`** — `(chain, tx_hash, signed_tx, from_addr, to_addr, value,
  nonce, submitted_at, submitted_by)`; one row per broadcast attempt.
- **`tx_confirmations`** — `(chain, tx_hash, status, block_height, block_hash,
  confirmations, first_seen_at, confirmed_at, finalized_at)`; updated by the
  confirmation tracker.
- **`chain_tips`** — `(chain, tip_height, tip_hash, finalized_height,
  updated_at)`; one row per chain, hot row.
- **`fee_estimates`** — `(chain, priority, max_fee_per_gas,
  max_priority_fee_per_gas, gas_price, sample_count, computed_at)`;
  time-series, periodically recomputed.
- **`reorg_events`** — `(chain, detected_at, old_tip_hash, new_tip_hash,
  common_ancestor_height, affected_tx_hashes[])`; append-only audit of reorgs.

Redis (ephemeral):

- **`nonce:lock:<chain>:<addr>`** — distributed mutex for nonce assignment.
- **`nonce:next:<chain>:<addr>`** — next-nonce counter cache.
- **`mempool:<chain>:<tx_hash>`** — mempool presence flag with TTL.

### Per-Chain Configuration

Each chain is configured via a record (loaded from env / config file):

```yaml
chains:
  - chain_id: ethereum
    rpc_urls:
      - https://eth-mainnet.alchemyapi.io/v2/<key>
      - https://mainnet.infura.io/v3/<key>
      - https://mainnet.quiknode.pro/<key>/
    ws_urls:
      - wss://eth-mainnet.ws.alchemyapi.io/v2/<key>
      - wss://mainnet.infura.io/ws/v3/<key>
    finality_blocks: 64
    gas_strategy: eip1559_dynamic
  - chain_id: polygon
    rpc_urls: [...]
    ws_urls: [...]
    finality_blocks: 256
    gas_strategy: eip1559_legacy_fallback
  - chain_id: solana
    rpc_urls: [...]
    ws_urls: [...]
    finality_blocks: 1            # Solana finality handled by adapter
    gas_strategy: solana_priority_fee
  - chain_id: bitcoin
    rpc_urls: [...]
    ws_urls: [...]
    finality_blocks: 6
    gas_strategy: bitcoin_rbf
```

`gas_strategy` options: `eip1559_dynamic`, `eip1559_legacy_fallback`,
`legacy_only`, `solana_priority_fee`, `bitcoin_rbf`, `custom`.

### Integrations

| Direction | Service | Protocol | Purpose |
|---|---|---|---|
| In (sync) | transaction-orchestrator | REST/gRPC | broadcast signed tx, estimate fee, query status |
| Out (sync) | wallet-management | gRPC | nonce allocation, gas prepayment (fund sender) |
| Out (async) | notification | event bus | tx status changes for user notifications |
| Out (async) | reconciliation | event bus | on-chain state for ledger matching |
| Out (async) | audit-event-log | event bus | append-only audit of broadcasts, confirmations, reorgs |
| Out (sync) | RPC providers / own nodes | JSON-RPC / WS | chain access |

### Reorg Policy

1. On each new head, compare `parent_hash` to the previous tip.
2. If mismatched, record a `reorg_event` with the common ancestor.
3. For every tx whose `block_height` is within the reorg window (i.e. above the
   common ancestor), set `status = reorged_out` and decrement confirmations.
4. Re-broadcast reorged-out txs if they are not present in the new chain within
   one block; otherwise mark `confirmed` again.
5. Never mark a tx `finalized` until it has `finality_blocks` confirmations and
   the chain's finality gadget agrees.
6. Emit a `tx.reorged` event to Notification, Reconciliation, and Audit Event
   Log.

## Dependencies

- **PostgreSQL** — persistent state: broadcasts, tx_confirmations, chain_tips,
  fee_estimates, reorg_events.
- **Redis** — distributed nonce locks, next-nonce cache, mempool presence
  flags.
- **RPC providers** — Alchemy, Infura, QuickNode (multi-provider with
  failover); optional self-hosted nodes.
- **wallet-management** — nonce allocation and gas prepayment.
- **audit-event-log** — append-only audit trail consumer.
- **Event bus** (e.g. Kafka / NATS JetStream) — async emission to
  Notification, Reconciliation, Audit.

## Configuration

Environment variables:

| Variable | Required | Default | Description |
|---|---|---|---|
| `PORT` | yes | `8080` | REST/gRPC listen port. |
| `GRPC_PORT` | no | `9090` | gRPC listen port. |
| `DB_URL` | yes | — | PostgreSQL DSN. |
| `REDIS_URL` | yes | — | Redis URL for nonce locks. |
| `CHAINS_SUPPORTED` | yes | — | Comma-separated chain IDs, e.g. `ethereum,polygon,solana`. |
| `RPC_URLS_<CHAIN>` | yes | — | Comma-separated RPC URLs for the chain (uppercased chain id). |
| `WS_URLS_<CHAIN>` | no | — | Comma-separated WebSocket URLs for the chain. |
| `FINALITY_BLOCKS_<CHAIN>` | yes | — | Finality depth for the chain. |
| `GAS_STRATEGY` | no | `eip1559_dynamic` | Default gas strategy; overridable per chain via `GAS_STRATEGY_<CHAIN>`. |
| `GAS_STRATEGY_<CHAIN>` | no | — | Per-chain gas strategy override. |
| `WALLET_MGMT_URL` | yes | — | wallet-management gRPC base URL. |
| `AUDIT_EVENT_LOG_URL` | no | — | audit-event-log URL (if synchronous fallback used). |
| `EVENT_BUS_URL` | no | — | Event bus broker URL for async emissions. |
| `BROADCAST_TIMEOUT` | no | `10s` | Per-broadcast RPC timeout. |
| `BROADCAST_RETRY_MAX` | no | `3` | Retry attempts on transient RPC failure. |
| `CONFIRMATION_POLL_INTERVAL` | no | `2s` | Fallback poll interval if WS heads unavailable. |
| `FEE_ESTIMATE_REFRESH` | no | `15s` | Fee estimate recompute interval. |
| `RPC_REQUEST_TIMEOUT` | no | `5s` | Generic RPC read timeout. |
| `RPC_PROVIDER_FAILOVER` | no | `true` | Enable provider failover. |
| `LOG_LEVEL` | no | `info` | Log level (`debug`, `info`, `warn`, `error`). |
| `METRICS_PORT` | no | `9100` | Prometheus metrics port. |
| `OTEL_ENDPOINT` | no | — | OpenTelemetry collector endpoint. |

## Local Development

```bash
# Build
make build

# Run (requires PostgreSQL + Redis + RPC provider env vars)
make run

# Run tests
make test

# Run linter
make lint

# Run integration tests (spins up Postgres + Redis via docker-compose)
make test-integration

# Generate gRPC/adapter mocks
make generate
```
