#!/usr/bin/env bash
# Configure + build. Uses ~/tg-deps/root when present (local, no sudo), else system packages (CI).
set -euo pipefail
cd "$(dirname "$0")/.."
BUILD=${BUILD_DIR:-build}
EXTRA=()
if [ -d "$HOME/tg-deps/root/usr" ]; then
  EXTRA+=(-DCMAKE_PREFIX_PATH="$HOME/tg-deps/root/usr" -DCMAKE_INCLUDE_PATH="$HOME/tg-deps/root/usr/include")
fi
cmake -S . -B "$BUILD" -DCMAKE_BUILD_TYPE=${BUILD_TYPE:-RelWithDebInfo} "${EXTRA[@]}" >/dev/null
cmake --build "$BUILD" -j"$(nproc)"
