#!/usr/bin/env bash
# Runs a whole pool on this machine and takes one member from signing up to
# being paid: a Livepeer devnet, an on-chain orchestrator in pool mode, Kafka,
# the pool service, the member agent with a real app and tunnel, and a
# customer paying with real tickets. See test/e2e/openpool_test.go for what
# each step shows.
#
#   pool/e2e.sh              run it
#   pool/e2e.sh keep-logs    run it and keep every process's log in pool/.e2e/
#
# Needs Go, Docker with the compose plugin, and network access to pull the
# devnet, Kafka, registry and echo images. Takes three to five minutes.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

if [ "${1:-}" = "keep-logs" ]; then
  export OPENPOOL_E2E_DIR="$root/pool/.e2e"
  rm -rf "$OPENPOOL_E2E_DIR"
  echo "logs: $OPENPOOL_E2E_DIR"
fi

OPENPOOL_E2E=1 exec go test ./test/e2e -run 'TestOpenPool$' -count=1 -v -timeout 30m
