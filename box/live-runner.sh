#!/bin/bash
# Smoke test the live runner path with the published example apps from
# https://github.com/livepeer/runner-app-examples.
set -e

DOCKER=${DOCKER:-false}
APP=${APP:-hello-world}
ORCH_SECRET=${ORCH_SECRET:-abcdef}
ORCH_URL=${ORCH_URL:-https://127.0.0.1:8935}
EXAMPLES_REF=${EXAMPLES_REF:-main}
FFMPEG=${FFMPEG:-ffmpeg}
FFPLAY=${FFPLAY:-ffplay}
CACHE_DIR="$(dirname "${BASH_SOURCE[0]}")/.cache"

ORCH_ARGS=(
  -orchestrator
  -useLiveRunners
  -orchSecret "$ORCH_SECRET"
  -serviceAddr 127.0.0.1:8935
  # Bound on every interface so containers on a bridge can reach the tunnel route.
  -httpAddr 0.0.0.0:8935
  -liveRunnerAddr "$ORCH_URL"
  -cliAddr 127.0.0.1:7935
  -network offchain
  -monitor=false
  -v 6
)

discover() {
  curl -sk "$ORCH_URL/discovery"
}

wait_for_runner() {
  for _ in $(seq 1 30); do
    URL=$(discover | jq -r '.[0].runners[0].url // empty')
    [ -n "$URL" ] && return 0
    sleep 1
  done
  echo "No live runner registered at $ORCH_URL/discovery" >&2
  exit 1
}

case "$1" in
  orchestrator)
    if [ "$DOCKER" = "false" ]; then
      ./livepeer "${ORCH_ARGS[@]}"
    else
      docker run --rm --name live-runner-orchestrator --network host livepeer/go-livepeer "${ORCH_ARGS[@]}"
    fi
    ;;
  app)
    docker run --rm --name "live-runner-$APP" --network host \
      "ghcr.io/livepeer/runner-example-$APP:latest" \
      --host=0.0.0.0 --orchestrator="$ORCH_URL" --orchSecret="$ORCH_SECRET" \
      --runner-url=http://127.0.0.1:8989
    ;;
  call)
    wait_for_runner
    discover | jq '.[].runners[] | {app, mode, url}'
    curl -sk -X POST "$URL/hello" -H 'Content-Type: application/json' -d '{"name":"box"}'
    echo
    ;;
  echo)
    wait_for_runner
    mkdir -p "$CACHE_DIR"
    if [ ! -f "$CACHE_DIR/echo-client.py" ]; then
      curl -sfo "$CACHE_DIR/echo-client.py" \
        "https://raw.githubusercontent.com/livepeer/runner-app-examples/$EXAMPLES_REF/echo/client.py"
    fi
    "$FFMPEG" -re -f lavfi -i testsrc=size=1280x720:rate=30 \
      -c:v libx264 -tune zerolatency -preset ultrafast -pix_fmt yuv420p -f mpegts - \
      | uv run --with av --with aiohttp --with 'livepeer-gateway>=1.0.0' \
          "$CACHE_DIR/echo-client.py" - --mode blur --discovery "$ORCH_URL/discovery" --output - \
      | "$FFPLAY" -fflags nobuffer -flags low_delay -framedrop -i -
    ;;
  tunnel-app)
    # An app the orchestrator is never given an address for: it publishes a
    # port on the orchestrator host's loopback only, and the tunnel command
    # beside it is what registers (runner_url becomes tunnel://<runner_id>).
    # Host networking rather than a bridge because rootless Docker has no
    # route from a bridge back to the host.
    docker rm -f box-tunnel-app box-tunnel-runner >/dev/null 2>&1 || true
    docker run -d --rm --name box-tunnel-app -p 127.0.0.1:18989:8080 mendhak/http-https-echo:latest >/dev/null
    TUNNEL_ARGS=(
      -orchestrator "$ORCH_URL" -orchSecret "$ORCH_SECRET"
      -app box/echo -mode single-shot -capacity 4
      -target http://127.0.0.1:18989 -health /
      -metadata '{"box":1}'
    )
    if [ "$DOCKER" = "true" ]; then
      docker run --rm --name box-tunnel-runner --network host \
        --entrypoint livepeer_runner_tunnel livepeer/go-livepeer "${TUNNEL_ARGS[@]}"
    else
      ./livepeer_runner_tunnel "${TUNNEL_ARGS[@]}"
    fi
    ;;
  tunnel-call)
    wait_for_runner
    discover | jq '.[].runners[] | {app, mode, url}'
    curl -sk -X POST "$URL/hello" -H 'Content-Type: application/json' -d '{"name":"box"}' | jq '{path, method, json}'
    ;;
  tunnel-stop)
    docker rm -f box-tunnel-app box-tunnel-runner >/dev/null 2>&1 || true
    ;;
  *)
    echo "Usage: $0 {orchestrator|app|call|echo|tunnel-app|tunnel-call|tunnel-stop}"
    exit 1
    ;;
esac
