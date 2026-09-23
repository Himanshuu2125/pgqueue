#!/usr/bin/env bash
# Chaos test: SIGKILL random workers under load, then verify no job was lost.
set -euo pipefail
cd "$(dirname "$0")/.."

RUN_ID=$(date +%s)
BENCH_KEY=bench-key-change-me
export DATABASE_URL="postgres://queue:queue@localhost:5433/queue?sslmode=disable"
LOG="bench/results/chaos-$RUN_ID.log"
: > "$LOG"

go run ./cmd/loadgen run -key "$BENCH_KEY" -n 20000 -type record -payload '{"ms":100}' \
  -max-attempts 25 -rate 2000 -run-id "$RUN_ID" &
LOADGEN=$!

END=$((SECONDS + 180))
while [ "$SECONDS" -lt "$END" ]; do
  sleep 10
  ids=($(docker compose ps -q worker))
  [ "${#ids[@]}" -eq 0 ] && continue
  id=${ids[$((RANDOM % ${#ids[@]}))]}
  docker kill -s KILL "$id" >/dev/null || true
  echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) killed $id" >> "$LOG"
  sleep 2
  docker start "$id" >/dev/null || true
done

wait "$LOADGEN" || true
go run ./cmd/loadgen verify -run-id "$RUN_ID" -timeout 10m
