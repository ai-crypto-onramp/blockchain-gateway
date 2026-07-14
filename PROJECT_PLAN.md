# Project Plan — Blockchain Gateway

Implementation plan for the Blockchain Gateway, the on-chain execution boundary
of the crypto on-ramp. The gateway broadcasts signed transactions per chain,
estimates gas/fees, prepays gas from hot wallets via Wallet Management, tracks
confirmations to a configurable depth, detects and handles reorgs against block
finality, monitors the mempool for the service's own transactions, and exposes
a uniform `ChainAdapter` interface across EVM chains, Solana, Bitcoin, and
others. Stages are ordered to build the chain-agnostic core first, then layer on
broadcast, fees, prepayment, confirmations, reorgs, mempool, failover,
notifications/reconciliation/audit, and finally tests/coverage/Docker.

## Stage 1: ChainAdapter Common Interface + Per-Chain Configuration

### Goal

Establish the chain-agnostic core: define the `ChainAdapter` interface, the
per-chain configuration loader, the adapter registry, and the `chain` domain
types (`Head`, `MempoolEvent`, `Tx`, `TxStatus`) referenced by the interface.
Wire per-chain config from env / YAML into a registry of adapters keyed by
`chain_id`.

### Tasks

- [x] Define `ChainAdapter` interface in `internal/chain/adapter.go` matching the
      README signature (`ChainID`, `Broadcast`, `GetTx`, `GetTxStatus`,
      `EstimateFee`, `Height`, `Balance`, `SubscribeHeads`, `SubscribeMempool`,
      `FinalityBlocks`).
- [x] Define domain types `chain.Head`, `chain.MempoolEvent`, `chain.Tx`,
      `chain.TxStatus`, `chain.FeeEstimateReq`, `chain.FeeEstimate` in
      `internal/chain/types.go`.
- [x] Define the tx status lifecycle enum: `broadcast -> mempool -> confirmed ->
      finalized` plus `dropped`, `replaced`, `reorged_out`, `failed`.
- [x] Implement per-chain config struct (`rpc_urls`, `ws_urls`,
      `finality_blocks`, `gas_strategy`) and a loader reading
      `CHAINS_SUPPORTED`, `RPC_URLS_<CHAIN>`, `WS_URLS_<CHAIN>`,
      `FINALITY_BLOCKS_<CHAIN>`, `GAS_STRATEGY_<CHAIN>`.
- [x] Implement an adapter `Registry` (map `chain_id -> ChainAdapter`) with
      `Get(chainID) (ChainAdapter, error)` and `Chains() []string`.
- [x] Add a no-op `stubAdapter` for unit testing the registry and config loader.
- [x] Add unit tests for the config loader (env + defaults) and registry
      lookup/miss.

### Acceptance criteria

- `ChainAdapter` interface compiles and is fully documented with references to
  the README API surface.
- Config loader produces a populated `[]ChainConfig` from env vars for at least
  `ethereum`, `polygon`, `solana`, `bitcoin`.
- Registry returns a registered adapter for a known chain and a typed error for
  an unknown chain.
- All unit tests pass; `go vet ./...` is clean.

## Stage 2: Broadcast Signed Transaction Endpoint

### Goal

Implement the synchronous broadcast path: accept an already-signed transaction
from the Transaction Orchestrator, submit it to the relevant chain's mempool
via the adapter, persist a row in `broadcasts`, and return the `tx_hash`. The
service never signs; it only broadcasts. Broadcast must be idempotent (same
signed tx yields the same `tx_hash`).

### Tasks

- [x] Implement the `broadcasts` table migration (columns from README Data
      Model) in `internal/store/migrations`.
- [x] Implement a `BroadcastStore` interface and a PostgreSQL-backed
      implementation with `Insert`, `GetByTxHash`, `Exists`.
- [x] Implement the REST handler `POST /v1/chains/:chain/broadcast` accepting
      `{"signed_tx": "<hex|base64>"}`; decode, route to adapter, return
      `{"tx_hash": "..."}`.
- [x] Enforce idempotency: if `tx_hash` already persisted for `(chain, hash)`,
      return the existing hash without re-submitting.
- [x] Add per-broadcast timeout (`BROADCAST_TIMEOUT`) and retry on transient
      RPC failure (`BROADCAST_RETRY_MAX`).
- [x] Add structured logging with `chain`, `tx_hash`, `from`, `nonce`,
      `latency_ms`.
- [x] Add unit tests for the handler (happy path, unknown chain, malformed
      payload, idempotent re-broadcast) using a stub adapter.
- [x] Add an integration test against an embedded Postgres (or `docker-compose`)
      verifying the `broadcasts` row is written.

### Acceptance criteria

- `POST /v1/chains/:chain/broadcast` returns 200 with `tx_hash` for a valid
  signed tx against a stub adapter.
- Re-submitting the same signed tx returns the same `tx_hash` and does not call
  the adapter a second time.
- Unknown chain returns a 404-style error; malformed payload returns 400.
- `broadcasts` row is persisted with all required fields.
- p99 broadcast latency is logged; timeout/retry config is honored.

## Stage 3: Gas / Fee Estimation (EIP-1559 Priority Fees)

### Goal

Implement fee estimation per chain with EIP-1559 parameters
(`maxFeePerGas`, `maxPriorityFeePerGas`) plus legacy `gasPrice` for EVM chains,
and chain-native fee models for Solana and Bitcoin. Support priority tiers
(`low`, `standard`, `high`) and persist estimates to the `fee_estimates`
time-series table for trend analysis.

### Tasks

- [x] Implement the `fee_estimates` table migration.
- [x] Implement an EVM `FeeEstimator` strategy: read recent block base fees +
      priority fee percentiles, compute `maxFeePerGas` and
      `maxPriorityFeePerGas` per priority tier; fall back to `gasPrice` for
      `legacy_only` / `eip1559_legacy_fallback` strategies.
- [x] Implement Solana `solana_priority_fee` strategy and Bitcoin `bitcoin_rbf`
      strategy behind the `EstimateFee` adapter method.
- [x] Implement `POST /v1/chains/:chain/estimate-fee` handler returning
      `{gas_limit, max_fee_per_gas, max_priority_fee_per_gas, gas_price,
      total_fee, priority}`.
- [x] Add a periodic recompute loop (`FEE_ESTIMATE_REFRESH`) that refreshes and
      persists estimates for each registered chain and priority.
- [x] Add metrics: `fee_estimate_computed_total{chain,strategy,priority}`,
      `fee_estimate_latency_seconds`.
- [x] Add unit tests for each strategy with fixture blocks and percentile
      thresholds.

### Acceptance criteria

- `POST /v1/chains/:chain/estimate-fee` returns EIP-1559 fields for EVM chains
  and chain-native fields otherwise, keyed by `priority`.
- Estimates are recomputed on the configured interval and persisted to
  `fee_estimates`.
- `eip1559_legacy_fallback` falls back to `gasPrice` when the node does not
  support EIP-1559.
- Unit tests cover all three priority tiers and the legacy fallback path.

## Stage 4: Gas Prepayment from Hot Wallet via Wallet Management

### Goal

Before broadcasting, ensure the sender account has sufficient native asset to
cover gas. Coordinate with Wallet Management over gRPC to fund the sender from
the hot wallet, allocate the nonce, and only then submit the broadcast. Fail
fast (do not broadcast) if prepayment cannot be confirmed.

### Tasks

- [x] Implement a `WalletMgmtClient` gRPC client wrapper around the
      wallet-management service with `FundSender(chain, addr, amount)` and
      `AllocateNonce(chain, addr)`.
- [x] Implement `Balance(ctx, addr)` on adapters (EVM: `eth_getBalance`; Solana:
      `getBalance`; Bitcoin: UTXO sum).
- [x] In the broadcast flow, before submitting: estimate fee, check sender
      balance, request `FundSender` for the deficit, wait for funding tx
      confirmation up to a timeout, then proceed.
- [x] Implement per-sender, per-chain nonce coordination using Redis
      (`nonce:lock:<chain>:<addr>`, `nonce:next:<chain>:<addr>`) under the
      distributed mutex; integrate `AllocateNonce` as the source of truth.
- [x] On broadcast failure due to insufficient funds / nonce, surface a typed
      error and do not persist a `broadcasts` row.
- [x] Add metrics: `prepayment_requested_total{chain}`,
      `prepayment_latency_seconds`, `nonce_contention_total`.
- [x] Add unit tests with a mock `WalletMgmtClient` and a stub adapter;
      integration test against Redis for nonce locking.

### Acceptance criteria

- Broadcast flow never submits when the sender has insufficient balance until
  `FundSender` returns success.
- Nonces are never reused under concurrent broadcasts (verified by a parallel
  integration test).
- A failed prepayment short-circuits the broadcast and returns a typed error to
  the caller.
- Redis nonce lock is acquired and released around broadcast; lock TTL is
  bounded.

## Stage 5: Confirmation Tracking with Configurable Depth per Chain

### Goal

Track confirmations for each broadcast up to `FINALITY_BLOCKS_<CHAIN>`. Maintain
the `tx_confirmations` table, expose intermediate confirmation counts, and
implement the `broadcast -> mempool -> confirmed -> finalized` state machine
with sticky confirmation workers keyed by `(chain, tx_hash)` for exactly-once
status updates.

### Tasks

- [x] Implement the `tx_confirmations` and `chain_tips` table migrations.
- [x] Implement a `ConfirmationStore` with upserts keyed by `(chain, tx_hash)`
      and atomic status transitions enforcing the lifecycle order.
- [x] Implement a confirmation worker pool with sticky assignment
      `(chain, tx_hash) -> worker` (consistent hashing) so each tx is updated by
      at most one worker.
- [x] On each new head (or fallback poll at `CONFIRMATION_POLL_INTERVAL`), for
      each tracked tx compute `confirmations = tip_height - tx.block_height + 1`
      and advance `mempool -> confirmed` at >=1 confirmation and
      `confirmed -> finalized` at `>= FinalityBlocks()`.
- [x] Implement `GET /v1/chains/:chain/tx/:hash` and
      `GET /v1/chains/:chain/tx/:hash/status` returning status, confirmations,
      and `finalized_at`.
- [x] Add metrics: `tx_status_total{chain,from_status,to_status}`,
      `confirmations_depth`, `confirmation_lag_seconds`.
- [x] Add unit tests for the state machine transitions and the sticky worker
      assignment; integration test for confirmation advancement on simulated
      heads.

### Acceptance criteria

- A tx reaches `confirmed` only after >=1 confirmation and `finalized` only after
  `finality_blocks` confirmations.
- Status transitions are atomic and exactly-once per `(chain, tx_hash)`.
- `GET .../status` reflects the current on-chain depth.
- p99 confirmation detection latency is below one block time.

## Stage 6: Reorg Handling (Block Finality)

### Goal

Detect chain reorganizations on each new head by comparing `parent_hash` to the
previous tip. On mismatch, record a `reorg_event`, re-evaluate affected
transactions, re-broadcast those that fell out, and emit a `tx.reorged` event.
Never mark a tx `finalized` before the chain's finality gadget agrees.

### Tasks

- [x] Implement the `reorg_events` table migration (append-only).
- [x] In the chain-tip follower, on each head compare `parent_hash` to the
      stored `chain_tips` hash; on mismatch, walk back to the common ancestor
      and record the reorg event with `affected_tx_hashes`.
- [x] For each tx whose `block_height` is above the common ancestor, set
      `status = reorged_out` and decrement confirmations.
- [x] After one block, if a reorged-out tx is absent from the new chain,
      re-broadcast it; otherwise restore `status = confirmed`.
- [x] Enforce finality: do not advance `confirmed -> finalized` until the
      adapter reports the chain's finality gadget agrees (e.g. finalized block
      >= tx block + finality_blocks).
- [x] Emit `tx.reorged` events to the event bus (Notification, Reconciliation,
      Audit).
- [x] Add unit tests for the reorg detection algorithm with synthetic head
      sequences; integration test for re-broadcast of reorged-out txs.

### Acceptance criteria

- A synthetic reorg is detected within one head, recorded in `reorg_events`
  with the correct common ancestor and affected txs.
- Reorged-out txs are re-broadcast if missing from the new chain after one block.
- No tx is marked `finalized` before the chain finality gadget agrees.
- `reorg_events` is append-only and auditable.

## Stage 7: Mempool Monitoring + Chain-Tip Follower

### Goal

Maintain the current tip per chain and stream new heads to internal consumers
(confirmation workers, reorg detector, fee estimator). Subscribe to mempool
entry/exit events and flag the service's own transactions that are dropped or
replaced, surfacing `dropped` / `replaced` terminal states.

### Tasks

- [x] Implement `SubscribeHeads` on adapters (WebSocket `newHeads` for EVM;
      slot notifications for Solana; block notifications for Bitcoin) with a
      fallback to polling at `CONFIRMATION_POLL_INTERVAL` when WS is unavailable.
- [x] Implement the chain-tip follower loop that updates `chain_tips` on each
      head and publishes heads to an internal broadcast channel for consumers.
- [x] Implement `GET /v1/chains/:chain/height` returning `height`, `hash`, and
      `finalized_height`.
- [x] Implement the WebSocket endpoint `WS /v1/chains/:chain/heads` streaming
      `{height, hash, parent_hash, timestamp}`.
- [x] Implement `SubscribeMempool` on adapters and a mempool watcher that tracks
      `mempool:<chain>:<tx_hash>` presence with TTL; flag own txs that exit the
      mempool without confirmation as `dropped` or `replaced`.
- [x] Add metrics: `mempool_seen_total{chain}`,
      `mempool_dropped_total{chain}`, `tip_lag_seconds`.
- [x] Add unit tests for the tip follower update logic and the mempool
      drop/replaced detection with a stub subscription.

### Acceptance criteria

- `chain_tips` is updated within one block time of the chain producing a new
  head.
- `GET .../height` and `WS .../heads` reflect the live tip.
- Own txs that leave the mempool without confirmation transition to `dropped` or
  `replaced`.
- WS subscription backpressure does not block the tip follower.

## Stage 8: RPC Provider Failover

### Goal

Provide automatic, transparent failover across configured RPC/WS providers
(Alchemy, Infura, QuickNode, self-hosted nodes) so that no broadcast or
confirmation is lost on a single-provider outage. Health-check providers and
prefer the healthiest for read and write paths.

### Tasks

- [x] Implement a `ProviderPool` per chain over `rpc_urls` / `ws_urls` with
      health checks (latest block, latency, error rate).
- [x] Implement request routing: prefer the primary provider; on transient
      error or timeout, fail over to the next provider; round-robin reads.
- [x] Gate failover behind `RPC_PROVIDER_FAILOVER` (default `true`); when
      disabled, fail fast on the primary.
- [x] Add circuit breaker per provider with configurable trip threshold and
      recovery probe.
- [x] Add metrics: `rpc_provider_healthy{chain,provider}`,
      `rpc_request_total{chain,provider,op,status}`,
      `rpc_failover_total{chain,from_provider,to_provider}`.
- [x] Add unit tests for the pool selection / failover algorithm and an
      integration test that takes down one provider and verifies failover.

### Acceptance criteria

- A single-provider outage causes transparent failover with no lost broadcasts.
- Reads are load-balanced across healthy providers; writes prefer the primary
  then fail over.
- Circuit breaker trips after the configured error threshold and recovers via a
  probe.
- `RPC_PROVIDER_FAILOVER=false` disables failover and fails fast.

## Stage 9: Notification, Reconciliation, and Audit Emission on Confirmations

### Goal

On every tx status change (and on reorgs), asynchronously emit events to
Notification, Reconciliation, and the Audit Event Log via the event bus so users
are notified, the ledger can match on-chain state, and an append-only audit
trail is kept. Emission must be at-least-once and deduped by `(chain, tx_hash,
status, block_height)`.

### Tasks

- [x] Define event schemas: `tx.broadcasted`, `tx.mempool`,
      `tx.confirmed`, `tx.finalized`, `tx.dropped`, `tx.replaced`,
      `tx.reorged`, `tx.failed`; each with `chain`, `tx_hash`, `from`, `to`,
      `value`, `fee`, `block_height`, `block_hash`, `confirmations`,
      `finalized_at`, `emitted_at`.
- [x] Implement an `EventBus` publisher (Kafka / NATS JetStream) with
      at-least-once delivery and an outbox in PostgreSQL keyed by
      `(chain, tx_hash, status, block_height)` for dedup.
- [x] Wire the confirmation worker, reorg handler, and mempool watcher to emit
      the corresponding events on each transition.
- [x] Implement a fallback synchronous path to `AUDIT_EVENT_LOG_URL` when the
      event bus is unavailable, with retry/backoff.
- [x] Add metrics: `events_emitted_total{type,chain,status}`,
      `events_deduped_total`, `events_failed_total`.
- [x] Add unit tests for the outbox dedup and event schema serialization;
      integration test for end-to-end emission on a simulated confirmation.

### Acceptance criteria

- Each status transition emits exactly one event per consumer topic (deduped
  via outbox).
- Events contain all required fields and are serializable to the bus schema.
- Bus outage triggers the synchronous audit fallback with retry/backoff.
- Audit Event Log records an append-only entry for every broadcast,
  confirmation, and reorg.

## Stage 10: Tests, Coverage, and Docker

### Goal

Reach production readiness: comprehensive unit + integration test coverage,
lint clean, CI green with Codecov upload, and a reproducible Docker image plus
`docker-compose` for local Postgres + Redis + the gateway.

### Tasks

- [x] Raise unit test coverage across `internal/...`; add targeted tests
      for the broadcast, fee, confirmation, reorg, mempool, and failover paths.
- [x] Add `make test-integration` that spins up Postgres + Redis via
      `docker-compose` and runs the integration suite (broadcast, confirmation,
      reorg, nonce locking, failover).
- [x] Ensure `make lint` (golangci-lint) and `make vet` pass with zero findings.
- [x] Finalize the `Dockerfile` (multi-stage, distroless runtime, non-root user)
      and `docker-compose.yml` (postgres, redis, gateway with env wiring).
- [x] Wire CI (`ci.yml`) to run `make lint`, `make test`, `make test-integration`,
      and upload coverage to Codecov on `main`.
- [x] Add a `make e2e-smoke` target that exercises broadcast -> confirm ->
  finalize against a simulated chain.
- [x] Update README "Local Development" section to reflect the new targets.

### Acceptance criteria

- `make lint && make vet && make test` all pass; coverage reported to Codecov.
- `docker compose up` brings the gateway up connected to Postgres + Redis and
  `GET /v1/chains/:chain/height` returns 200.
- CI pipeline is green on `main` and uploads coverage.
- `make e2e-smoke` exercises the full lifecycle on a simulated chain.