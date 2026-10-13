# OPEX Next Stage Implementation Plan

> For agentic workers: use superpowers:subagent-driven-development or superpowers:executing-plans task by task. User has authorized full implementation with subagents; primary selects gpt-6.1-sol/max.

**Goal:** Ship a running INJAST UI and Hummingbot V2 market maker backed by traceable OPEX spot orders and verified risk controls.

**Architecture:** Keep OPEX matching and accounting. Add durable order intents, matcher-enforced post-only/STP, a typed INJAST facade, and independent oracle/risk/controller modules. Integrate on isolated data before changing the current demo.

**Tech Stack:** Kotlin/Spring/PostgreSQL/Kafka; React 19/TypeScript/Rsbuild/injast-core 1.0.79; Python/Hummingbot 2.17.0; Docker Compose.

**Specs:** ../specs/2026-10-13-injast-opex-ui-design.md and ../specs/2026-10-13-opex-market-maker-design.md.

## Global constraints

- IRT means Tomans; decimal amounts use exact arithmetic and strings over JSON.
- Preserve the original 39 scenarios and 16 ledger allocation references on the existing demo.
- No synthetic fills, balances, book order counts, provider timestamps or active feature flags.
- SourceArena credential stays in demo/.sourcearena.env, mode 0600, outside Git/UI/logs.
- UI uses official injast-core 1.0.79 from INJAST Nexus; retain source components and visual tokens.
- Hummingbot version 2.17.0 and immutable revision/image provenance; no market hedging or leverage in phase one.
- Native OPEX authentication and caller ownership must hold for every private read and write.

## Review focus

- Lost acceptance response must reconcile one stable identity, not create a duplicate order or reservation.
- Crossing post-only and same-owner orders must emit zero prohibited fills despite racing book changes.
- Partial fill plus cancel/restart must conserve balances and release only unused reservation once.
- Rial/Toman and USD/USDT differences must not become a silent 10x or parity assumption.
- Stale/quorum failure or dead risk supervisor must stop new quoting and preserve tracking until final cancellation.

## Ownership and interfaces

Backend worker owns core Kotlin, migrations and tests on an isolated core worktree. Frontend worker owns an isolated copy/worktree of the supplied exchange frontend plus demo/injast.py facade. Market-making worker owns market-making/ only. Primary owns demo orchestration, data snapshots, integration, browser delivery and final verification. Workers must accommodate others and must not revert their edits.

New API contract (backend may propose a documented correction before consumers finalize):

- POST /opex/v2/order: {clientOrderId,symbol,type,side,quantity,price,postOnly}; authenticated owner is derived from token. Durable response {clientOrderId,ouid,symbol,status,orderId}; status is ACCEPTED_PENDING until admitted.
- GET /opex/v2/order?clientOrderId=... or ouid=...: caller-owned authoritative intent plus matching status/filled/remaining quantities.
- DELETE /opex/v2/order with same identity: idempotent cancellation receipt, pending until real final state.
- Existing real owner orders/trades/balances support reconciliation; stable execution IDs deduplicate fills. The connector never infers identity from identical price/quantity tuples.
- Oracle snapshot: pair, normalizedQuote, bidVwap, askVwap, midpoint, sourceTimestamp, receivedAt, age, usable, rejectionReason, provenance; no secrets.
- Risk decision: allowNewQuotes, allowedSides, remainingQuoteBudget, leaseExpiresAt, reason. Quoting requires a fresh lease.

## Task 1: Runtime alignment and regression fixture

Files: demo/runtime-alignment.md, demo/snapshot.sh, demo/integration.compose.yml; backend Gradle/build configuration only if required by verified build failures.

- [ ] Record source revisions, deployed jar/image hashes, database schemas and current scenario fingerprints.
- [ ] Export or snapshot existing demo data without stopping unrelated INJAST services or deleting volumes.
- [ ] Build source-aligned backend images on isolated ports/data; fix build failures at their cause.
- [ ] Run legacy spot submit/fill/partial/cancel/fee/ownership checks on isolated fixtures.
- [ ] Record successful startup and rollback commands; commit only task-owned files.

## Task 2: Durable order admission and reconciliation

Files: API order/controller/proxy models; matching-gateway request/result/event/submitter/service; new intent repository/migration/outbox; accountant and market projections as required by ownership/idempotency tests.

- [ ] Write failures for concurrent same-key requests, conflicting payloads, lost responses and duplicate event delivery.
- [ ] Run targeted Gradle tests and verify they fail for the missing contract.
- [ ] Persist unique (owner,clientOrderId), payload hash and stable OUID before publication; return a durable pending receipt and implement retry/query/cancel semantics.
- [ ] Carry identity through events/projections and deduplicate financial references and admission. Never assert cross-database exactly-once without the idempotency evidence.
- [ ] Run targeted tests including unauthorized reads/cancels and restart recovery; commit.

## Task 3: Post-only, self-trade and settlement invariants

Files: matching-engine command metadata, SimpleOrderBook and command/event handlers; accountant rejection/cancel settlement handling; corresponding engine/accountant integration tests.

- [ ] Write failures for post-only equal/crossed prices, per-pair races, self-owner crosses and partial-fill/cancel races.
- [ ] Add explicit backwards-compatible flags; enforce admission and CANCEL_TAKER inside the serialized matching operation before forbidden trade events.
- [ ] Bind reserve-to-admit and reject/cancel releases to stable references; enforce cancel ownership.
- [ ] Assert filled+remaining<=original, exact fees/refunds, zero prohibited fills and event replay invariance; commit.

## Task 4: INJAST frontend and truthful facade

Files: isolated frontend-injast source based on clean local exchange dev; providers/theme/root/auth/IdentityGate; API service adapters and trade validation; demo/injast.py and tests. Primary integrates proxy/compose routes.

- [ ] Install the official pinned design-system package and compatible React19/icons peer; preserve provenance and source visual components.
- [ ] Adapt email login, OPEX token refresh/logout and private cache invalidation; remove INJAST-only gates and remote telemetry from the local flow.
- [ ] Map markets/book/orders/wallet/history/charts to real OPEX contracts. Preserve pending states and client IDs across retries; normalize symbols, units, notional limits and percentage fees.
- [ ] Use source UI tokens, RTL/font/layout, desktop sidebar and mobile shells; do not import malformed ZIP login JSX or fabricated-chart code.
- [ ] Build, check meaningful contract/validation tests, and run browser login→market→create/cancel→wallet/history on actual backend data.
- [ ] Verify desktop/mobile, loading/errors/empty states, keyboard controls and screenshots; commit.

## Task 5: Oracle and independent risk supervisor

Files: market-making/opex_mm/oracle/{models,depth,nobitex,wallex,sourcearena,aggregate}.py; market-making/opex_mm/risk/{limits,supervisor}.py; config and tests.

- [ ] Write tests for executable VWAP, insufficient depth, stale/missing provider timestamp, outliers, correlated conversions, Rial/Toman and USD/USDT.
- [ ] Implement typed provider parsing and equal-weight valid-source median/quorum. SourceArena uses all+last_update and remains indicative until its quote/cadence is proven.
- [ ] Implement risk lease, inventory/quote/daily budget reservation, loss/turnover persistence and kill switch; no automatic Market hedge.
- [ ] Verify provider outage and supervisor death halt quoting, while cancellation tracking continues; commit.

## Task 6: Hummingbot V2 spot connector and quote lifecycle

Files: market-making/hummingbot/connector/exchange/opex/*; custom V2 controller/executor; persisted recovery state, scripts, configs, tests and runtime image support.

- [ ] Write connector lifecycle failures for unknown acceptance, delayed cancel, duplicated fill/status and restart with outstanding quotes.
- [ ] Implement authenticated OPEX submit/query/cancel/balance/orderbook/trade interfaces with durable IDs and Hummingbot event ordering.
- [ ] Implement a spot quote executor and three-level controller that obey oracle/risk leases, tick/lot rules and final cancel confirmation before replacement.
- [ ] Start with dedicated mm-bot test account and isolated replay data; then validate live-reference observation and quote only with quorum.
- [ ] Verify bounded inventory/fee/PnL and truthful pause reasons; commit.

## Task 7: End-to-end delivery and review

Files: demo startup/configuration docs, accounts/scenario manifest, screenshots outside repos, frontend bot-status views, integration test scripts.

- [ ] Run distinct provider-outage/outlier/partial-fill/late-cancel/restart/inventory/kill scenarios on isolated data.
- [ ] Exercise manual buyer/seller trades against bot quotes, proving real executions and account balance changes without self-trades.
- [ ] Reconcile all order identities, fills, fees, reserves and bot state; verify original demo evidence remains intact.
- [ ] Review the integrated branch independently, fix material issues, run affected checks once more and deliver running URLs, credentials, controls and screenshots.
- [ ] Keep missing future Stop/OCO/Margin/Earn features explicitly identified; do not report full Nobitex parity prematurely.

Execution selection: user explicitly requested subagents and authorized choosing sol6.1 where preferable. Use independent workers at gpt-6.1-sol/max for backend, frontend and market-making; primary integrates and verifies. This plan is published for review while the user-directed implementation proceeds; further feedback steers the active work.
