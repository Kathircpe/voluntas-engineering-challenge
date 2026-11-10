# Polymarket L2 order book collector

A Rust service that discovers every live Polymarket market, maintains an independent L2 book per outcome token, and streams FIX SBE snapshots and deltas to stdout. Built to run unattended for hours without silently losing data.

Target scale: roughly **370k outcome tokens**, a few thousand books moving at once, and an exchange that does not always tell you when it has dropped you.

Built for the [Voluntas engineering challenge](https://voluntas.io/engineering-challenge).

## Approach

I treated the exchange as an undocumented, unreliable system: probe it, measure where the pipeline actually breaks, design around those measurements, and make every failure mode explicit. Most of the work below came from failures that produce no error message at all.

## Requirement coverage

| Requirement | Implementation |
|---|---|
| All live symbols | `PM_MAX_SYMBOLS=all` default; full Gamma keyset scan |
| Regular markets only | `active && !closed && !archived && enableOrderBook && acceptingOrders` |
| Continuous discovery | `PM_REFRESH_SECS` re-discovery, diffed in `Store::reconcile` |
| Deltas + snapshot per minute | SBE to `/dev/stdout`, `PM_SNAPSHOT_SECS=60` |
| Provided helpers | `AlignedSbeWriter` / `AlignedSbeReader`, no custom framing |
| SIGINT | `Command::Shutdown` -> `Close` -> writer join and flush |
| Load balancing | Shards by capacity, least-loaded placement |
| Completeness | Resync, standby reconciliation, quiet-symbol resubscribe |
| Time accuracy | Receive timestamp before decode and channel hop |
| 1. Redundant sockets | `PM_REDUNDANCY=2`, active/standby with stall detection |
| 2. Subscription monitoring | Per-symbol last-update tracking, targeted resubscription |
| 3. Generic encode/decode | `Exchange` trait owns discovery, framing, heartbeat, decoding |
| 4. Latency | `TCP_NODELAY`, batched frames, [LATENCY.md](LATENCY.md) |

## Architecture

```
              Gamma keyset API
                     |
                  discovery
                     |
              +------+------+
              |    Store    |  symbol -> shard, per-socket books,
              +------+------+  active source per shard
                     |
     +---------------+---------------+
   shard 0         shard 1   ...   shard N-1
   /     \         /     \
socket  socket  socket  socket          (redundancy = 2)
active standby active  standby
   |       |       |       |
   +--- updates ---+       |
           |               |
     decode + apply   standby books
           |
   bounded channel (1024)
           |
     SBE writer thread
           |
       /dev/stdout
```

| Module | Responsibility |
|---|---|
| `exchange/mod.rs` | Venue-agnostic `Exchange` trait and `Update` |
| `exchange/polymarket.rs` | Gamma discovery, subscription frames, decoding |
| `collector.rs` | Sharding, health, source switching, refresh |
| `socket.rs` | One connection: batching, watchdogs, reconnects |
| `store.rs` | Per-socket books, active sources, reconciliation |
| `book.rs` | Fixed-point L2 book with change detection |
| `output.rs` | Bounded channel and dedicated SBE writer |
| `config.rs` | Validated environment configuration |

A second venue only needs an `Exchange` implementation; the rest is unchanged.

## Bottlenecks found, measured, fixed

### 1. Discovery was slower than collection

CLOB `/markets` returns every market ever created. A live scan walked **over 1.04M rows and was still going** to find **~3.9k live markets (~7.8k tokens)**, taking **9+ minutes** per pass, which makes a five-minute refresh impossible.

**Fix:** Gamma `/markets/keyset` with server-side filters and `after_cursor`. A cold full scan is now **~370k tokens in ~11 minutes**, still bounded by the API's `limit=100` cap (~3.7k sequential requests).

**Trade-off:** there is no market delta endpoint, so refresh is still a full scan. It runs in a background task and never blocks collection, but for the full universe the timer effectively runs back-to-back. Next step: persist the symbol set and diff locally.

### 2. A subscribe frame fails silently above ~750 assets

Oversized frames are accepted by the handshake and partially applied: no error, no close code, missing books simply never arrive.

| Assets in one frame | Books received |
|---:|---:|
| 500 / 600 / 750 | all |
| 800 | 12 / 800 |
| 900 | 22 / 900 |
| 1,000 | 14 / 1,000 |

**Fix:** 500 assets in the initial frame, then `operation: "subscribe"` frames of 500, paced. Tested to 10,000 assets on one socket with 9,996 books (the four missing were markets with no book yet, recovered by resync).

**Trade-off:** batching adds 100 ms per batch, so a 5,000-symbol shard takes ~1s to ramp. A silent gap is far worse than a one-second start.

### 3. One connection cannot hold the universe

**Fix:** shards are sized by `ceil(symbols / PM_SYMBOLS_PER_CONNECTION)` with a floor of `PM_CONNECTIONS`. New symbols go to the least-loaded shard; existing assignments never move, so refresh does not churn correct books. Defaults imply **~75 shards and ~150 sockets** for the full universe.

**Trade-off:** connection count grows with the universe. `PM_SYMBOLS_PER_CONNECTION` is the lever: higher means fewer connections but a slower ramp and larger blast radius.

### 4. Redundancy must not double-count

**Fix:** each shard has `PM_REDUNDANCY` sockets with independent books. Only the **active** socket emits; standbys apply updates silently so they stay warm.

Switch when the active socket disconnects, or produces no market data for `PM_SOURCE_STALL_SECS` (10s) while a standby has. On switch, a **full snapshot from the standby** is emitted before any of its deltas, so a consumer never sees two book states interleaved. A 30s cooldown prevents flapping.

**Trade-off:** redundancy doubles bandwidth, decoding, and book memory. That is the price of not having a hole when one TCP connection dies, which at this scale is routine. `PM_REDUNDANCY=1` disables it.

### 5. Gaps appear without a disconnect

Three stale-book failure modes: a delta before its snapshot (common after reconnects), a silently dropped subscription on a live socket, and a market that simply goes quiet.

**Fix:** three guards.

- **Missing snapshot:** a delta with no local book requests a per-symbol resync (unsubscribe + resubscribe).
- **Quiet symbol:** per-symbol last-update tracking; idle past 300s triggers a resubscribe with a 60s cooldown.
- **Dead socket:** 10s `PING` heartbeat with a 30s idle watchdog and a 120s market-data watchdog.

**Trade-off:** a genuinely quiet market is resubscribed roughly every six minutes. A little wasted traffic beats a permanently stale book.

### 6. Timestamps must reflect receive time

**Fix:** `received_at` (and `backtest_timestamp`) is captured immediately after the frame is read, before JSON decoding and before the channel send.

**Trade-off:** the exchange batches events per frame, so events share one receive instant. One honest frame timestamp beats invented per-event precision.

### 7. The writer must not drop data

**Fix:** a bounded channel (1,024) feeds a dedicated writer thread. A stalled sink backpressures ingestion instead of dropping messages; Ctrl-C drains and flushes before exit. The 64 MiB chunk matches the provided reader, which hardcodes it.

**Trade-off:** lossless means a slow consumer can slow ingestion. A recorded gap is worse than a late record; a deployment would page on writer lag.

## Correctness details

- **No floats.** Decimal strings parse into `i64` at 1e-6 scale; overflow and malformed fields are rejected.
- **Absolute sizes.** `price_change.size` is the new quantity; `0` removes the level and still emits a removal delta.
- **Idempotent.** Deltas that do not change the book are not emitted.
- **Ordered snapshots.** Bids descend from best, asks ascend.
- **Partial failure.** A malformed event in a batch is dropped individually; the rest still applies.
- **Freshness.** A periodic snapshot carries `received_at = emission time` and `exchange_timestamp = last event seen`.
- **Delisting.** Removed symbols emit zero-quantity deltas for every level before state is dropped.

## Results

| Check | Result |
|---|---|
| `cargo test` | 42 tests, all passing, offline |
| `cargo clippy --all-targets` / `cargo fmt --check` | clean |
| Live run, 996 symbols, 2 shards x 2 sockets | 4,105 snapshots, 44,973 deltas, 329,665 levels |
| Source switch / SIGINT | standby promoted with snapshot; sockets closed, writer flushed |

## Run it

```bash
PM_MAX_SYMBOLS=1000 cargo run --release > out.sbe 2> collector.log
cargo run --example decode -- out.sbe        # or --all
```

```
SNAPSHOT <token> received=... exchange=... bids=47 asks=20 best_bid=0.79 best_ask=0.80
DELTA <token> side=Buy price=0.79 qty=120 received=... exchange=...
snapshots=4105 deltas=44973 symbols=996 levels=329665
```

Logs go to stderr; SBE is the only thing on stdout.

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `PM_MAX_SYMBOLS` | `all` | Cap discovery for short runs |
| `PM_CONNECTIONS` | `8` | Minimum shards |
| `PM_SYMBOLS_PER_CONNECTION` | `5000` | Shard capacity |
| `PM_REDUNDANCY` | `2` | Sockets per shard |
| `PM_SUBSCRIBE_BATCH` | `500` | Assets per frame |
| `PM_SNAPSHOT_SECS` | `60` | Snapshot interval |
| `PM_REFRESH_SECS` | `300` | Discovery refresh |
| `PM_PING_SECS` / `PM_IDLE_SECS` | `10` / `30` | Heartbeat and idle watchdog |
| `PM_DATA_IDLE_SECS` | `120` | Market-data watchdog |
| `PM_SYMBOL_IDLE_SECS` / `PM_SYMBOL_RESUBSCRIBE_SECS` | `300` / `60` | Quiet-symbol handling |
| `PM_SUBSCRIPTION_CHECK_SECS` | `30` | Quiet-symbol scan |
| `PM_RECONNECT_SECS` | `3600` | Scheduled reconnect |
| `PM_SOURCE_STALL_SECS` / `PM_SWITCH_COOLDOWN_SECS` | `10` / `30` | Switch policy |
| `PM_OUTPUT` | `/dev/stdout` | Output path |

Reconnects add deterministic jitter up to 10% of the interval (capped at 30s) so redundant sockets do not reconnect in lockstep.

## Trade-offs

| Decision | Cost | Why |
|---|---|---|
| Lossless bounded output | Slow consumer backpressures ingestion | Gaps cannot be recovered; delay can |
| Redundant sockets | 2x bandwidth, decode, memory | Single-connection failures are normal here |
| Batch + pace subscriptions | ~1s ramp per 5,000 symbols | Silent partial subscriptions are undetectable |
| Full refresh every 5 min | Expensive at 370k symbols | Delisted markets must stop emitting |
| Per-symbol resubscription | Wasted frames on quiet markets | Prevents permanently stale books |
| Keep shard assignments | Slight imbalance over time | Avoids churning correct books |

## Next

Incremental discovery, horizontal sharding by symbol hash, writer-lag metrics, SBE replay tests that assert deltas reproduce the final book, and a second `Exchange` implementation to prove the abstraction.

## About the starter repo

The provided YAML schema and `AlignedSbeWriter` / `AlignedSbeReader` are used as-is. The custom `timestamp` field maps to `chrono::DateTime<Utc>` and is stored as microseconds since the Unix epoch.
