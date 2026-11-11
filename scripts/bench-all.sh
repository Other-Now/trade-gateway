#!/usr/bin/env bash
# Every measured number in README.md comes from this script.
#   OUT=results/local bash scripts/bench-all.sh
set -uo pipefail
cd "$(dirname "$0")/.."
B=${BIN:-$HOME/tg-build}/tg-bench
OUT=${OUT:-results/local}
mkdir -p "$OUT"
A=127.0.0.1:9001; G=127.0.0.1:9001,127.0.0.1:9002
KEYS=alice-key,bob-key,carol-key,dave-key,erin-key
rc=0
run() { local name=$1; shift; echo "== $name" | tee -a "$OUT/summary.txt"; "$B" "$@" 2>&1 | tee "$OUT/$name.txt" | tee -a "$OUT/summary.txt"; [ "${PIPESTATUS[0]}" = 0 ] || rc=1; }
: > "$OUT/summary.txt"
{ echo "host: $(nproc) cpus, $(uname -sr)"; date -u; } >> "$OUT/summary.txt"

bash scripts/stack.sh up >/dev/null
run rtt        rtt --gw $A --n ${N_RTT:-2000}
run config     config --gws $G --n ${N_CFG:-300}
run burst-seq  burst --gws $G --n 1000
run burst-par  burst --gws $G --n 1000 --writers 8
run burst-txn  burst --gws $G --n 1000 --txn
run revoke     revoke --gw $A --n ${N_REV:-50}
run isolation  isolation --gws $G --keys $KEYS --n ${N_ISO:-200}
run resync     resync --gw $A --id a

# Control: gateway b does NOT reload after reconnecting its LISTEN connection.
bash scripts/stack.sh up --no-resync >/dev/null
run resync-control resync --gw 127.0.0.1:9002 --id b --expect-stale

# Fan-out with a stalled reader, correct (conflating) vs control (unbounded queue).
MOCK_RATE=${MD_RATE:-5000} bash scripts/stack.sh up --no-conflate >/dev/null
run fanout-baseline   fanout --gw $A --readers 4 --stalled 0 --secs ${FAN_SECS:-20} --label conflate
run fanout-stalled    fanout --gw $A --readers 4 --stalled 1 --secs ${FAN_SECS:-20} --label conflate
run fanout-control    fanout --gw 127.0.0.1:9002 --readers 4 --stalled 1 --secs ${FAN_SECS:-20} --label no-conflate
bash scripts/stack.sh down
exit $rc
