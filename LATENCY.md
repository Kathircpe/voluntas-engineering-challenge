# Latency and connection findings

These notes come from probing Polymarket's public CLOB WebSocket while building the collector. They are observations, not guarantees; the exchange can change its limits at any time.

## Subscription frame size

A single `subscribe` frame with too many asset IDs is accepted by the WebSocket handshake but only partially applied. In repeated tests the cutoff was between 750 and 800 assets:

| Assets in one frame | Books received |
|---:|---:|
| 500 | 500 / 500 |
| 600 | 600 / 600 |
| 750 | 750 / 750 |
| 800 | 12 / 800 |
| 900 | 22 / 900 |
| 1,000 | 14 / 1,000 |

The 750-asset frame is roughly 60 KB of JSON. The failure mode looks like a frame-size limit rather than an asset-count limit, because the server does not return an error; it simply never sends the missing snapshots.

## Batching instead of more sockets

Sending the initial `subscribe` frame with 500 assets and then adding the rest with `operation: "subscribe"` frames of 500 assets works far beyond the single-frame limit:

| Total assets | Batch size | Books received |
|---:|---:|---:|
| 2,000 | 500 | 2,000 / 2,000 |
| 5,000 | 500 | 4,998 / 5,000 |
| 10,000 | 500 | 9,996 / 10,000 |

The few missing books are markets that did not have a book at the time, not dropped subscription frames. This lets one WebSocket cover thousands of symbols and keeps the total connection count manageable.

The collector uses `PM_SUBSCRIBE_BATCH` (default 500) and pauses briefly between batches so the exchange is not hit with a burst of large frames.

## TCP_NODELAY

The collector enables `TCP_NODELAY` on the underlying TCP stream as soon as the WebSocket handshake completes. Order-book updates are small and latency-sensitive; Nagle's algorithm would add avoidable delay by waiting for more data before sending.

## Receive timestamps

`received_at` and `backtest_timestamp` are captured immediately after a WebSocket frame is read, before JSON decoding and before the update crosses a channel. The timestamp therefore reflects the collector's receive time rather than the time the update reached the book writer.

## Redundant sockets

Each shard runs `PM_REDUNDANCY` connections (default 2). All connections subscribe to the same symbols and maintain independent books. Only the active connection emits deltas.

A standby becomes the source when:

- the active connection disconnects, or
- the active connection has not delivered market data for `PM_SOURCE_STALL_SECS` (default 10 seconds) while the standby has.

When the source changes, the collector emits a full snapshot from the standby's book before applying any new deltas. That avoids mixing two potentially different book states.

A cooldown (`PM_SWITCH_COOLDOWN_SECS`, default 30 seconds) prevents flapping when two connections are close in freshness.

## Scheduled reconnects

Every connection is recycled after `PM_RECONNECT_SECS` (default 1 hour). The delay includes a deterministic jitter of up to 10% of the interval, capped at 30 seconds, so a shard's redundant connections do not reconnect at the same instant.

## Quiet-symbol resubscription

Each connection tracks the last market-data time for every symbol. Symbols that have been quiet for `PM_SYMBOL_IDLE_SECS` (default 300 seconds) are unsubscribed and resubscribed in batches. This forces a fresh snapshot for symbols whose subscription may have gone stale without a socket-level disconnect.

## Measurement caveats

These numbers were collected from a single machine and a single network path. They are useful for choosing conservative defaults, not for comparing exchanges. Handshake latency is logged at debug level (`websocket connected latency=...`) so the same measurements can be repeated from another location.
