#!/usr/bin/env bash
# Local only (no sudo): unpack Boost headers, libpq-dev and a PostgreSQL 16
# server into ~/tg-deps/root. CI installs the same packages with apt-get.
set -euo pipefail
ROOT=$HOME/tg-deps/root
mkdir -p "$ROOT" "$HOME/tg-deps/dl"
cd "$HOME/tg-deps/dl"
for p in libboost1.83-dev libpq-dev postgresql-16 postgresql-client-16; do
  ls ${p}_*.deb >/dev/null 2>&1 || apt-get download "$p"
done
for d in *.deb; do dpkg-deb -x "$d" "$ROOT"; done
ln -sf /usr/lib/x86_64-linux-gnu/libpq.so.5 "$ROOT/usr/lib/x86_64-linux-gnu/libpq.so"
ls "$ROOT/usr/include/boost/beast.hpp" "$ROOT/usr/include/postgresql/libpq-fe.h" "$ROOT/usr/lib/postgresql/16/bin/postgres"
