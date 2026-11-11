#!/usr/bin/env bash
# Bring up the local stack: schema+seed, mock exchange, two gateways (a, b).
#   stack.sh up [extra tgw flags for gateway b]   |   stack.sh down
# Gateway a is always the correct build; b can be started as a control
# (e.g. --no-resync, --no-conflate). Logs go to $RUN/*.log.
set -euo pipefail
cd "$(dirname "$0")/.."
BIN=${BIN:-$HOME/tg-build}
RUN=${RUN:-$HOME/tg-run}
PGPORT=${TG_PG_PORT:-5544}
PGCI="host=127.0.0.1 port=$PGPORT user=tg dbname=tg"
PSQL=${PSQL:-"$HOME/tg-deps/root/usr/lib/postgresql/16/bin/psql"}
[ -x "$PSQL" ] || PSQL=psql
mkdir -p "$RUN"
case "${1:-up}" in
  up)
    shift || true
    pkill -f "$BIN/tgw" 2>/dev/null || true; pkill -f "$BIN/tg-mockex" 2>/dev/null || true; sleep 0.3
    [ -f certs/mock.pem ] || { mkdir -p certs; openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
        -subj /CN=localhost -keyout certs/mock.key -out certs/mock.pem 2>/dev/null; }
    PGOPTIONS='--client-min-messages=warning' "$PSQL" "$PGCI" -q -v ON_ERROR_STOP=1 -f sql/schema.sql -f sql/seed.sql
    "$BIN/tg-mockex" --port 9443 --cert certs/mock.pem --key certs/mock.key --secret mock-secret \
        --rate "${MOCK_RATE:-50}" > "$RUN/mockex.log" 2>&1 &
    sleep 0.3
    COMMON=(--pg "$PGCI" --md-url wss://127.0.0.1:9443/ws --api-url wss://127.0.0.1:9443/ws-api/v3 --insecure-upstream)
    TG_API_KEY=mock-key TG_API_SECRET=mock-secret "$BIN/tgw" --port 9001 --id a "${COMMON[@]}" > "$RUN/gw-a.log" 2>&1 &
    TG_API_KEY=mock-key TG_API_SECRET=mock-secret "$BIN/tgw" --port 9002 --id b "${COMMON[@]}" "$@" > "$RUN/gw-b.log" 2>&1 &
    for p in 9001 9002; do
      for i in $(seq 50); do curl -sf localhost:$p/health | grep -q '"upstream_trading":true' && break; sleep 0.1; done
      curl -s localhost:$p/health; echo
    done ;;
  down)
    pkill -f "$BIN/tgw" 2>/dev/null || true; pkill -f "$BIN/tg-mockex" 2>/dev/null || true ;;
esac
