#!/usr/bin/env bash
# Local PG16 for dev/bench, unpacked (no sudo). Usage: pg.sh start|stop|psql
set -euo pipefail
B=$HOME/tg-deps/root/usr/lib/postgresql/16/bin
export LD_LIBRARY_PATH=$HOME/tg-deps/root/usr/lib/x86_64-linux-gnu:${LD_LIBRARY_PATH:-}
D=$HOME/tg-pgdata
PORT=${TG_PG_PORT:-5544}
case "${1:-start}" in
  start)
    if [ ! -d "$D" ]; then
      "$B/initdb" -D "$D" -U tg --auth=trust >/dev/null
      echo "unix_socket_directories = '/tmp'" >> "$D/postgresql.conf"
    fi
    "$B/pg_ctl" -D "$D" -o "-p $PORT" -l "$D/log" -w start
    "$B/psql" -h 127.0.0.1 -p $PORT -U tg -d postgres -tAc "select 1 from pg_database where datname='tg'" | grep -q 1 \
      || "$B/createdb" -h 127.0.0.1 -p $PORT -U tg tg ;;
  stop) "$B/pg_ctl" -D "$D" -m fast stop ;;
  psql) shift; "$B/psql" -h 127.0.0.1 -p $PORT -U tg -d tg "$@" ;;
esac
