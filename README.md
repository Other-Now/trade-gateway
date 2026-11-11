# trade-gateway

A small C++20 client gateway on **Boost.Beast/Asio**, with live config from
**PostgreSQL LISTEN/NOTIFY** and an upstream connection to **Binance spot
testnet**.

- **Client API.** HTTP and WebSocket. Every client authenticates with an API
  key and sees only its own orders and trades, plus market data for the
  instruments it is permitted to use.
- **Configuration in Postgres.** Instruments, per-account limits and API keys
  live there. Every change NOTIFYs all running gateways, which reload a
  consistent snapshot without restarting. That includes revoking a key whose
  socket is open right now.
- **Upstream.** Binance spot testnet: a `bookTicker` stream (real, measured
  below) and the WS-API (`order.place` / `order.cancel` / user data stream,
  HMAC-signed).
  - Benchmarks trade against `tg-mockex`, a local stand-in that speaks the same
    wire subset and verifies the signatures.

```
 clients ──HTTP/WS──►┌──────────────── tgw (1 thread, 1 io_context) ───────────────┐
  X-API-Key          │ WsSession ─► Hub: auth · risk · rate limit · order table    │──wss──► Binance testnet
                     │   ▲ events (never dropped)        routing by account/symbol │         /ws  bookTicker
                     │   ▲ market data (conflated)                                 │         /ws-api/v3 orders
                     │ PgWatcher: libpq non-blocking on the same loop              │
                     └──────────────▲──────────────────────────────────────────────┘
                                    │ LISTEN tg_config  /  snapshot reload
                     PostgreSQL ────┘  trigger: bump config_version + pg_notify
```

## Design in five points

1. **One thread, no locks.**
   - Beast, the Postgres connection and both Binance sockets all run on one
     `io_context` with C++20 coroutines. libpq is non-blocking, with its socket
     in an Asio `stream_descriptor`.
   - A config is an immutable `shared_ptr<const Config>`, swapped whole on
     reload.
2. **Config changes are transactional and coalesced.**
   - A statement-level trigger bumps `config_version` and NOTIFYs at COMMIT.
   - The gateway reloads all tables in one `REPEATABLE READ` snapshot. Any
     number of queued NOTIFYs cause one reload.
3. **LISTEN before load, and reload after every reconnect.** NOTIFY is not
   queued for a listener that isn't connected. `--no-resync` is the control
   that shows what happens otherwise.
4. **Isolation via the client order id.**
   - Binance sees one account, the gateway's. The id is
     `tg-<account>-<gw><seq>`, so every executionReport routes back to the
     right client, on any gateway instance.
   - Another account's order id gets the same `unknown order` as a
     non-existent one.
5. **Slow clients.**
   - Order events are never dropped; past 10k queued, the client is
     disconnected.
   - Market data is conflated per symbol, so a stalled reader costs O(symbols)
     memory, not O(time).

## Results

The bench drives two gateways, the mock exchange and a real Postgres 16, all
local: WSL2, 4 vCPUs, one laptop.
- **Source:** `scripts/bench-all.sh` produces every number below. The raw
  output is in [`results/local/`](results/local/).
- **CI:** CI runs the same script and posts the summary on every push.
- **Latency figures:** the bench timestamps each frame on its own reader
  thread. Treat tails (p99/max) as noisy on this host.

| what | result |
|---|---|
| Unit tests | 52 checks: decimals, Binance HMAC vector, risk rules, 128-bit notional overflow, auth, rate limiter |
| Risk reject, gateway only (WS round trip) | p50 **123 µs**, p90 161 µs (2000 orders) |
| Order → ack / exchange NEW / fill (mock exchange over TLS) | p50 **208 / 434 / 469 µs** |
| Config change: COMMIT → client sees new version, on **both** gateways | p50 **0.60 / 0.66 ms**, p99 1.2 / 1.3 ms (300 commits) |
| Snapshot reload query (5 tables, one transaction) | p50 425 µs |
| 1000 UPDATEs in one transaction | 1000 NOTIFYs → **1 reload** per gateway |
| 1000 separate commits, 1 vs 8 concurrent writers | 1057 vs 1003 commits/s: the `config_version` row lock serialises config writes (by design) |
| API key revoked while its socket is open | socket closed with 1008 in p50 **0.60 ms**, max 1.0 ms; 50/50 closed, 0/50 could reconnect |
| Isolation: 5 accounts × 2 sockets (one per gateway) × 200 orders | 4000 exec events, **0 to the wrong account, 0 missing**; cross-account cancel → `unknown order`; unpermitted subscribe → `not permitted` |
| LISTEN connection killed, limit tightened while it was down | resynced in **124 ms**, over-limit order **rejected** |
| …same, control `--no-resync` | **still stale after 3 s; over-limit order accepted** |
| 5k ticks/s, 4 readers + 1 reader that never reads | gateway RSS 16.0 → **16.0 MB** (75k ticks conflated) |
| …same, control `--no-conflate` | RSS 16.0 → **34.5 MB in 20 s** and climbing |
| Live Binance testnet `bookTicker` (BTCUSDT, ETHUSDT), 60 s | 67 updates, 0 reconnects, gateway rx → client p50 181 µs ([log](results/local/live-testnet.txt)) |

**Not measured or not claimed:**
- **Orders on the real testnet.** The path is written (`TG_API_KEY` /
  `TG_API_SECRET` enable it) but has not been run live; it needs a testnet
  key.
- **A slow reader's effect on the other readers' tail latency.** The results
  were inconclusive across runs; see `docs/NOTES.md` §6.

## Run it

```bash
# Ubuntu 24.04. No sudo needed locally: deps.sh unpacks the packages under ~/tg-deps
bash scripts/deps.sh && bash scripts/pg.sh start
bash scripts/build.sh && build/tg-unit
bash scripts/stack.sh up            # schema+seed, mock exchange, gateways on :9001 and :9002
bash scripts/bench-all.sh           # everything above

# real testnet market data (and trading, if you export a testnet key)
build/tgw --port 9001 --pg "host=127.0.0.1 port=5544 user=tg dbname=tg"
```

### Client API

```
GET    /health                    config version, upstream state
GET    /metrics                   counters, config-apply latency, RSS
GET    /api/instruments           what this key may trade, with limits     (X-API-Key)
POST   /api/orders                {"symbol","side","type","qty","price"}  -> 202 {"cid"} | 422 {"error"}
DELETE /api/orders/{cid}
GET    /api/orders                this account's orders only

WS /ws  (X-API-Key header)
  -> {"op":"subscribe","symbols":["BTCUSDT"]}      <- {"type":"md",...}   (conflated)
  -> {"op":"order","req_id":1,...}                 <- {"type":"ack"|"reject",...}
  -> {"op":"cancel","cid":"..."}                   <- {"type":"exec",...} own orders only
                                                   <- {"type":"config","version":N}
```

Live config, for example:

```sql
UPDATE account_limits SET max_qty = 0.5 WHERE account_id = 1 AND symbol = 'BTCUSDT';
UPDATE api_keys SET revoked = true WHERE account_id = 2;    -- bob's sockets close now
UPDATE instruments SET enabled = false WHERE symbol = 'ETHUSDT';  -- upstream UNSUBSCRIBE
```

## Layout

- `src/gw/`: the gateway (hub, sessions, pgwatch, upstream, risk).
- `src/mockex/`: the local exchange stand-in.
- `src/bench/`: the measurements and the PASS/FAIL checks.
- `sql/`: schema and trigger, plus seed data.
- `docs/NOTES.md`: design notes and trade-offs.
