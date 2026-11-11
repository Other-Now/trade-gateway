# Study sheet: trade-gateway

What to know cold before the interview, in the order an interviewer usually
walks it. About 2,800 lines in total. The gateway itself (`src/gw/`, about
1,500 lines) is the part that matters; the rest is mock, bench and tests.

## 1. The 30-second version

A C++20 client gateway on Boost.Beast/Asio:

- **Clients.** Clients connect over HTTP or WebSocket with an API key. Each one
  sees only its own orders and trades, plus market data for the instruments
  it is permitted to use.
- **Config.** Config (instruments, per-account limits, API keys) lives in
  Postgres. Every running gateway reloads it live on `LISTEN/NOTIFY`.
- **Upstream.** Upstream is Binance spot testnet: a bookTicker stream for
  market data, and the WS-API for orders and execution reports.
- **Event loop.** One thread with one `io_context` and C++20 coroutines. libpq
  runs non-blocking on that same loop, so there are no locks anywhere.

## 2. File map

| file | what's in it |
|---|---|
| `src/gw/main.cpp` | flags; wires Hub + Upstream + PgWatcher onto one io_context |
| `src/gw/session.cpp` | HTTP routes, `/ws` upgrade + auth, `WsSession` reader/writer coroutines |
| `src/gw/hub.cpp` | config apply/diff, routing tables, order table, risk + rate limit, exec routing |
| `src/gw/pgwatch.cpp` | async libpq: LISTEN, coalesced snapshot reload, reconnect + resync |
| `src/gw/upstream.cpp` | TLS WebSockets to Binance, HMAC signing, live SUBSCRIBE diffs |
| `src/gw/risk.cpp` | pure pre-trade checks (unit-tested) |
| `sql/schema.sql` | tables + the statement-level trigger that bumps the version and NOTIFYs |
| `src/mockex/` | local Binance stand-in (same wire subset, verifies HMAC) |
| `src/bench/` | every measured number + the PASS/FAIL checks |

## 3. Threading model: why one thread

- **Everything on one strand.** Every handler and coroutine runs on one
  `io_context` thread. The Hub's maps, the config pointer and the order table
  are all touched from that one thread, so there is no mutex and no atomics.
- **What it costs.** Throughput is capped at one core. The measured order path
  is 208 µs p50 to ack, and most of that is loopback plus the bench's own
  thread handoffs, not the gateway.
- **How it would scale.** Shard sessions across N io_contexts, with routing
  per account. Or run more gateway processes, which the Postgres design already
  supports: that is why config is in Postgres and not a file.
- **Why no lock even for config.** A config snapshot is an immutable
  `shared_ptr<const Config>`. A reload builds a new one and swaps the pointer.
  On one thread that needs no lock. Across threads it would need
  `std::atomic<std::shared_ptr>` or RCU.

## 4. The config path (the part to explain best)

1. Any write to the four config tables fires a **statement-level** trigger.
   - It runs `UPDATE config_version SET version = version+1 RETURNING version`.
   - Then `pg_notify('tg_config', {"v":…, "table":…, "ts":…})`.
2. NOTIFY is **transactional**: it is delivered at COMMIT, never on rollback.
3. The gateway's PgWatcher holds one libpq connection.
   - That connection's socket is `dup()`ed into an Asio `posix::stream_descriptor`.
   - `async_wait(wait_read)`, then `PQconsumeInput`, then drain `PQnotifies`.
4. It takes the **highest** version among all queued notifications. If that is
   above the applied version, it reloads once. One reload covers any number of
   NOTIFYs, because it reads the whole snapshot.
5. The reload is one `REPEATABLE READ READ ONLY` transaction with five SELECTs.
   All five see the same snapshot, so the version it records is exactly the
   version of what it loaded.
6. `Hub::apply_config` swaps the pointer, then walks every live session:
   - **Key revoked or account disabled:** close with 1008.
   - **Lost permission for a symbol:** drop the subscription and send `unsubscribed`.
   - **Everyone:** send `{"type":"config","version":N}`.
7. The Upstream diffs the enabled instruments and sends live
   SUBSCRIBE/UNSUBSCRIBE to Binance.

### Correctness points you will be asked about

- **LISTEN before the initial load.** Load-then-listen loses any change
  committed between the SELECT and the LISTEN.
- **NOTIFY is not durable.** If the listener's connection is down, the
  notifications are simply gone. So on every reconnect the gateway reloads
  ("resync").
  - **Demonstrated:** `--no-resync` is the control. With it, the bench kills
    the listener backend and tightens `max_qty` while it is down. The control
    stays on the old version and **accepts an order that violates the new
    limit**. The correct gateway reconnects and reloads 124 ms later, and
    rejects that order.
  - **Why 124 ms:** that is the 100 ms reconnect backoff plus the reload.
- **Coalescing.**
  - 1000 UPDATEs in one transaction produce 1000 NOTIFYs at COMMIT and
    exactly 1 reload per gateway.
  - With separate commits it was about 1 reload per commit. A reload (≈0.5 ms)
    finishes before the next commit (≈1–2 ms fsync) arrives, so there is
    nothing to coalesce. That is fine.
- **The version row is a global lock.** Every config write takes the
  `config_version` row lock until commit.
  - Measured: 8 concurrent writers gave 1003 commits/s vs 1057 for one
    writer. No gain at all: they serialise on that row through fsync.
  - Fine for config, which changes a few times a day. It would be wrong for
    anything hot. The fix is `pg_current_xact_id()` or a sequence instead of a
    counter row, which costs the "commit order == version order" property.
- **Payload limit.** NOTIFY payloads must be under 8000 bytes. That is why the
  payload carries only a version number, and the gateway reads the data
  itself.
- **Two latencies, both real:**
  - COMMIT returned → client sees the config event: 0.60 ms p50 (gateway a) and 0.66 ms
    (gateway b), over 300 commits.
  - Trigger fired → applied in process: about 2.1 ms p50. The trigger's
    `clock_timestamp()` is taken *before* the commit's WAL fsync, so this
    number includes the fsync.
  - The difference between the two is the fsync.

## 5. Per-client isolation

- **Authentication.** The client sends `X-API-Key`, and the gateway hashes it
  with SHA-256 to look it up. The database stores hashes only. A key is valid
  only if it is unrevoked AND its account is enabled. Auth is **re-checked on
  every config change**, not only at connect time.
- **One exchange account for all clients.** Binance knows a single account,
  the gateway's. The gateway puts the client's account into the client order
  id: `tg-<acct>-<gw><seq>`. Every executionReport is routed by parsing that
  id, and only to sockets of that account.
  - On a cancel, Binance's `c` is the cancel's own id and `C` is the original.
    Routing must use `C`.
  - Reports with no `tg-` prefix (an order placed on the testnet web UI) are
    counted and dropped.
- **Cancel probing.** Cancelling another account's id returns `unknown order`,
  identical to an id that doesn't exist, so ids can't be probed.
- **Several gateways.** Every gateway subscribes to the user data stream, so a
  client connected to gateway B sees fills for orders it placed through
  gateway A. The isolation bench checks exactly that.
  - 5 accounts × 2 sockets (one per gateway) × 200 orders.
  - 4000 exec events delivered, 0 to the wrong account, 0 missing.

## 6. Slow consumers

- **Two queues per session:**
  - **Order events** are never dropped. Past `max_queue` (10k), the gateway
    cuts the client off; it reconnects and asks for `orders`.
  - **Market data** is **conflated** per symbol: a slow client gets the latest
    book, not a backlog.
- **Measured** at 5k ticks/s with one reader that never reads:
  - The conflating gateway's RSS stayed at 15.9 MB, with about 62k ticks
    conflated per 15 s.
  - The control `--no-conflate` grew from 16.0 to 34.5 MB in 20 s (35.4 MB in an earlier run) and would
    keep growing.
- **Honest note: inconclusive.** Does a stalled reader hurt the others?
  - In 3 back-to-back pairs, no: readers' p50 was 112–118 µs with the
    stalled reader vs 115–126 µs without it.
  - In 2 full-suite runs, readers' p90 was worse with it: 1.7 ms vs 0.19 ms,
    and 2.4 ms vs 0.28 ms.
  - The tails swing a lot between identical runs. Both runs also had
    unexplained ~2 s maxima. That is a 4-vCPU WSL VM running gateway, mock and
    bench together.
  - Writes are async, so a stalled socket can't block the loop by design.
  - Claim the memory result, not a latency result.

## 7. Binance specifics

- **Market data.** `wss://stream.testnet.binance.vision/ws`, then send
  `{"method":"SUBSCRIBE","params":["btcusdt@bookTicker"],"id":n}`. Raw frames
  have `u,s,b,B,a,A`.
- **Trading.** `wss://ws-api.testnet.binance.vision/ws-api/v3`. Methods:
  `order.place`, `order.cancel`, and `userDataStream.subscribe.signature`,
  which delivers executionReports on the same socket.
- **Signing.**
  - Take all params except `signature`, sort them by name, and join them as
    `k=v&k=v`.
  - HMAC-SHA256 that string with the secret and hex-encode it.
  - Unit-tested against Binance's documented example vector.
- **Precision.** Prices and quantities are `int64` in 1e-8 units. Tick and lot
  checks are integer `%`. The notional multiply is done in `__int128`, because
  `qty*px` in 1e-8² overflows int64 (unit-tested).
- **Upstream disconnect with orders in flight.** Those orders are reported to
  their owners as `UNKNOWN`, not as rejected. A real system would reconcile
  with `openOrders.status` on reconnect (not built).

## 8. What is real vs simulated

| thing | status |
|---|---|
| Market data from Binance testnet | **real**, measured |
| Live SUBSCRIBE/UNSUBSCRIBE driven by a Postgres change | **real** |
| Orders to Binance testnet | code path written; **not run live** (needs testnet API key) |
| Orders in benchmarks | against `tg-mockex`, which speaks the same wire subset and checks HMAC |
| Postgres | real PG16, local |

Don't claim live testnet trading unless you have run it with a key:
`TG_API_KEY=… TG_API_SECRET=… ./tgw`.

## 9. Likely questions

- **Why Beast and not a framework?** It gives full control over buffers, write
  ordering and back-pressure. Beast allows exactly one outstanding async_write
  per stream, hence the per-connection outbox/writer coroutine.
- **Why coroutines?** Linear code for connect → handshake → read loop →
  reconnect. With callbacks, the reconnect logic is where the bugs live.
- **Why not poll the config table?**
  - Polling trades latency against load: poll every 1 s and you are up to 1 s
    stale.
  - NOTIFY gave about 0.7 ms p50, and polling is still needed after a
    reconnect anyway (the resync).
- **Rate limit with N gateways?** The token bucket is per process, so an
  account can get N× its configured rate. Fixes:
  - Shard accounts so each lives on one gateway.
  - Or a shared counter (Redis or Postgres) at a latency cost.
- **What breaks first under load?** The single thread. Then JSON serialisation
  per message: market data is serialised once per tick and the string is
  shared by all subscribers, but exec events are serialised per account.
- **What would you add next?**
  1. Order reconciliation on upstream reconnect.
  2. Persist the order table, since it is in memory today.
  3. TLS on the client side.
  4. Auth on the first WS message as well as the header, because browsers
     can't set headers on WS.
  5. Prune terminal orders.

## 10. Commands

```bash
bash scripts/deps.sh && bash scripts/pg.sh start      # local, no sudo
bash scripts/build.sh && build/tg-unit
bash scripts/bench-all.sh                             # everything in results/local
bash scripts/stack.sh up                              # mock + gateways a,b on :9001/:9002
```
