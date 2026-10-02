#!/usr/bin/env bash
# Checks this region's files against what already runs on this host, before
# anything is started. It starts nothing and changes nothing: it reads .env,
# the secrets directory, and Docker's lists of containers and networks.
#
#   ./preflight.sh          (with sudo if Docker needs it here)
#
# It answers two questions. Is everything this region needs in place? And
# can starting it touch anything that is already running: a name, a
# hostname, a port or a network that something else has.
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

problems=0 warnings=0
ok()   { printf '  ok    %s\n' "$*"; }
warn() { printf '  WARN  %s\n' "$*"; warnings=$((warnings + 1)); }
bad()  { printf '  FAIL  %s\n' "$*"; problems=$((problems + 1)); }
section() { printf '\n%s\n' "$*"; }

[ -f .env ] || { echo "there is no .env here; copy .env.example to .env and fill it in"; exit 2; }
docker ps >/dev/null 2>&1 || { echo "Docker does not answer; run this with sudo"; exit 2; }
# .env is read the way Compose reads it: NAME=value lines, the value taken
# as it stands, spaces and all. It is not run as a shell script.
while IFS= read -r line || [ -n "$line" ]; do
  case "$line" in ''|'#'*) continue ;; esac
  name="${line%%=*}" value="${line#*=}"
  [[ "$name" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || continue
  case "$value" in \"*\") value="${value#\"}" value="${value%\"}" ;; \'*\') value="${value#\'}" value="${value%\'}" ;; esac
  printf -v "$name" '%s' "$value"
done < .env
project="openpool-${REGION_ID:-}"
secrets="${SECRETS_DIR:-./secrets}"
subnet="${INNER_SUBNET:-172.31.250.0/24}"

# Containers that are not this region's own.
others() { docker ps -a --format '{{.Names}}' --filter "label=com.docker.compose.project" | while read -r c; do
  [ "$(docker inspect -f '{{index .Config.Labels "com.docker.compose.project"}}' "$c")" = "$project" ] || echo "$c"; done
  docker ps -a --format '{{.Names}}' | while read -r c; do
  [ -n "$(docker inspect -f '{{index .Config.Labels "com.docker.compose.project"}}' "$c")" ] || echo "$c"; done; }
others="$(others | sort -u)"
# The key file of this region's account among what a container has mounted, if any.
holds_key() {
  local src
  is_address "${ORCH_ETH_ADDRESS:-}" && [ "$ORCH_ETH_ADDRESS" != "0x0000000000000000000000000000000000000000" ] || return 0
  docker inspect -f '{{range .Mounts}}{{println .Source}}{{end}}' "$1" | while read -r src; do
    [ -d "$src" ] || continue
    # A monitoring container mounts the whole host; a node mounts its data.
    case "$src" in /|/proc*|/sys*|/dev*|/run*|/var/run*|/etc*|/usr*|/lib*|/boot*|/var/lib/docker|/var/lib/docker/volumes) continue ;; esac
    timeout 20 find "$src" -xdev -maxdepth 3 -type f -size -8k -exec grep -liF "\"${ORCH_ETH_ADDRESS#0x}\"" {} + 2>/dev/null </dev/null | head -1
  done | head -1
}
labels_of() { docker inspect -f '{{range $k, $v := .Config.Labels}}{{println $k "=" $v}}{{end}}' "$1" | grep '^traefik\.' || true; }

section "Settings (.env)"
for name in REGION_ID REGION_NAME ORCHESTRATOR_IMAGE OPENPOOL_IMAGE REDPANDA_IMAGE AGENT_IMAGE TUNNEL_IMAGE ORCH_HOST POOL_API_HOST \
  MARKETING_SITE_HOST MEMBER_SITE_HOST ADMIN_SITE_HOST TRAEFIK_NETWORK TRAEFIK_ENTRYPOINT TRAEFIK_CERTRESOLVER ETH_RPC_URL \
  ORCH_ETH_ADDRESS ADMIN_EMAIL MAIL_FROM KAFKA_TOPIC; do
  [ -n "${!name:-}" ] || bad "$name is not set"
done
case "${TUNNEL_IMAGE:-}" in
  *@sha256:0000000000000000000000000000000000000000000000000000000000000000) bad "TUNNEL_IMAGE is still the placeholder; it is the tunnel image's digest, known once the image is pushed" ;;
  *@sha256:????????????????????????????????????????????????????????????????) ok "TUNNEL_IMAGE is named by digest" ;;
  *) bad "TUNNEL_IMAGE must be named by digest, as name@sha256:…" ;;
esac
case "${ORCH_ETH_ADDRESS:-}" in 0x0000000000000000000000000000000000000000|"") bad "ORCH_ETH_ADDRESS is still the placeholder" ;; esac
is_address() { [[ "$1" =~ ^0x[0-9a-fA-F]{40}$ ]]; }
is_address "${ORCH_ETH_ADDRESS:-}" || bad "ORCH_ETH_ADDRESS is not an address: 0x and 40 hex digits"
if [ -n "${ORCH_ONCHAIN_ADDRESS:-}" ]; then
  if is_address "$ORCH_ONCHAIN_ADDRESS"; then
    ok "tickets are made out to $ORCH_ONCHAIN_ADDRESS; this node signs and redeems as $ORCH_ETH_ADDRESS"
    [ "${ORCH_REWARD:-false}" = "false" ] || warn "ORCH_REWARD is on with a separate on-chain orchestrator: the node will not start unless its account is that orchestrator's reward caller"
  else
    bad "ORCH_ONCHAIN_ADDRESS is not an address: 0x and 40 hex digits, or empty"
  fi
fi
case "${ORCH_REWARD:-false}" in true|false) ;; *) bad "ORCH_REWARD is true or false" ;; esac
case "${ADMIN_EMAIL:-}${MAIL_FROM:-}" in *example.com*) bad "ADMIN_EMAIL or MAIL_FROM is still an example address" ;; esac
case "${RESEND_BASE_URL:-}" in
  ""|http://?*|https://?*) ;;
  *) bad "RESEND_BASE_URL must be a full address starting with https://, or empty for Resend itself; the pool service will not start with \"$RESEND_BASE_URL\"" ;;
esac
[ "${ORCH_CLI_TX_ROUTES:-false}" = "false" ] || warn "ORCH_CLI_TX_ROUTES is on: livepeer_cli may send transactions. Turn it off once the orchestrator is activated"
[ "${ORCH_HOST:-a}" != "${POOL_API_HOST:-b}" ] || bad "ORCH_HOST and POOL_API_HOST are the same name; Traefik tells them apart by name"

section "Secrets ($secrets)"
for file in pool_secret session_secret orch_secret resend_api_key payout_keystore; do
  if [ -s "$secrets/$file" ]; then ok "$file"; else bad "$file is missing or empty"; fi
done
for file in orch_eth_password payout_password; do
  if [ -f "$secrets/$file" ]; then ok "$file"; else bad "$file is missing (an empty file is right for an empty password)"; fi
done
if [ -d "$secrets/orch_keystore" ] && [ -n "$(ls -A "$secrets/orch_keystore" 2>/dev/null)" ]; then
  # A key file names its account in its "address" field.
  if grep -rqiF "\"${ORCH_ETH_ADDRESS#0x}\"" "$secrets/orch_keystore" 2>/dev/null; then
    ok "orch_keystore holds the key of $ORCH_ETH_ADDRESS"
  else
    bad "no key file in orch_keystore is for ORCH_ETH_ADDRESS ($ORCH_ETH_ADDRESS): the orchestrator will not find its account"
  fi
else
  bad "orch_keystore is missing or empty: it holds the orchestrator account's key file"
fi
for file in pool_secret session_secret payout_keystore; do
  [ -f "$secrets/$file" ] && [ ! -r "$secrets/$file" ] && bad "$file cannot be read"
  [ -f "$secrets/$file" ] && [ "$(stat -c '%a' "$secrets/$file" | cut -c3)" -lt 4 ] && bad "$file is not readable by the pool service, which does not run as root: chmod 644 it (the directory, mode 700, is what keeps others out)"
done

section "What this region would create"
if rendered="$(docker compose config 2>&1)"; then
  ok "the compose file is valid with these settings"
  if echo "$rendered" | grep -q 'published:'; then bad "a service publishes a port on the host"; else ok "no port is published on the host: everything arrives through Traefik"; fi
else
  bad "docker compose config fails: $(echo "$rendered" | tail -1)"
fi
if [ -n "$(docker ps -a -q --filter "label=com.docker.compose.project=$project")" ]; then
  warn "containers of project $project already exist: this region has been started here before"
else
  ok "no project called $project exists yet"
fi
for volume in redpanda orchestrator openpool; do
  docker volume inspect "${project}_$volume" >/dev/null 2>&1 && warn "volume ${project}_$volume already exists and will be used, with what it holds"
done

section "Traefik"
if docker network inspect "${TRAEFIK_NETWORK:-}" >/dev/null 2>&1; then
  ok "network ${TRAEFIK_NETWORK} exists"
  traefik="$(docker ps --filter "network=${TRAEFIK_NETWORK}" --format '{{.Names}} {{.Image}}' | grep -i traefik | head -1)"
  if [ -n "$traefik" ]; then ok "Traefik is on it: $traefik"; else warn "no running container on ${TRAEFIK_NETWORK} looks like Traefik"; fi
else
  bad "network ${TRAEFIK_NETWORK:-(unset)} does not exist: it is the one your Traefik watches"
fi
used_resolvers="$(for c in $others; do labels_of "$c"; done | sed -n 's/.*certresolver = //p' | sort -u | tr '\n' ' ')"
used_entrypoints="$(for c in $others; do labels_of "$c"; done | sed -n 's/.*entrypoints = //p' | sort -u | tr '\n' ' ')"
if [ -n "$used_resolvers" ]; then
  case " $used_resolvers" in *" ${TRAEFIK_CERTRESOLVER:-} "*) ok "certificate resolver ${TRAEFIK_CERTRESOLVER} is the one other services here use" ;;
    *) warn "certificate resolver ${TRAEFIK_CERTRESOLVER:-(unset)} is not one other services here use ($used_resolvers)" ;; esac
fi
# A resolver that is named but cannot issue leaves Traefik serving its own
# self-signed certificate. Ask it for a name another service here has.
if [ -n "${traefik:-}" ] && command -v openssl >/dev/null 2>&1; then
  traefik_ip="$(docker inspect -f "{{(index .NetworkSettings.Networks \"${TRAEFIK_NETWORK}\").IPAddress}}" "${traefik%% *}" 2>/dev/null)"
  known="$(for c in $others; do labels_of "$c"; done | grep -E '^traefik\.http\.routers\..*\.rule = ' | grep -o 'Host(`[^`]*`)' | head -1 | cut -d'`' -f2)"
  if [ -n "$traefik_ip" ] && [ -n "$known" ]; then
    issuer="$(echo | timeout 10 openssl s_client -connect "$traefik_ip:443" -servername "$known" 2>/dev/null | openssl x509 -noout -issuer 2>/dev/null)"
    case "$issuer" in
      *"TRAEFIK DEFAULT CERT"*) warn "Traefik answers for $known with its own self-signed certificate, so it is not issuing certificates. Browsers will refuse ${POOL_API_HOST:-the pool API} unless a proxy that accepts that certificate, such as Cloudflare's, is in front of it" ;;
      "") ;;
      *) ok "Traefik serves an issued certificate for $known" ;;
    esac
  fi
fi
if [ -n "$used_entrypoints" ]; then
  case " $used_entrypoints" in *" ${TRAEFIK_ENTRYPOINT:-} "*) ok "entrypoint ${TRAEFIK_ENTRYPOINT} is the one other services here use" ;;
    *) warn "entrypoint ${TRAEFIK_ENTRYPOINT:-(unset)} is not one other services here use ($used_entrypoints)" ;; esac
fi

section "Names already taken on this host"
clash=0
for c in $others; do
  mine="$(labels_of "$c")"
  for name in "openpool-orch-${REGION_ID:-}" "openpool-api-${REGION_ID:-}"; do
    if echo "$mine" | grep -q "\.\(routers\|services\|middlewares\)\.$name\."; then bad "container $c already defines a Traefik router or service called $name"; clash=1; fi
  done
  for host in "${ORCH_HOST:-}" "${POOL_API_HOST:-}"; do
    if [ -n "$host" ] && echo "$mine" | grep -qF "\`$host\`"; then bad "container $c already has a Traefik rule for $host: two routers for one name fight over it"; clash=1; fi
  done
  if echo "$mine" | grep -q 'HostSNI(`\*`)'; then warn "container $c has a TCP router for every name (HostSNI(\`*\`)) and may take the orchestrator's connections"; fi
  # Nodes that send from one account can take each other's transaction
  # number when they send at the same moment; the one that loses tries again.
  if is_address "${ORCH_ETH_ADDRESS:-}" && [ "${ORCH_ETH_ADDRESS}" != "0x0000000000000000000000000000000000000000" ] \
    && docker inspect -f '{{json .Args}}{{json .Config.Env}}{{json .Config.Cmd}}' "$c" | grep -qiE "ethAcctAddr[^a-z0-9]{1,5}(0x)?${ORCH_ETH_ADDRESS#0x}"; then
    warn "container $c also sends from $ORCH_ETH_ADDRESS. That works, with a transaction retried now and then; an account for each node avoids it"
  elif key="$(holds_key "$c")" && [ -n "$key" ]; then
    warn "container $c also has the key of $ORCH_ETH_ADDRESS ($key). Sharing it works, with a transaction retried now and then; an account for each node avoids it"
  fi
done
[ "$clash" = 0 ] && ok "no other container uses this region's router names or hostnames"

section "The inner network ($subnet)"
to_int() { local IFS=.; read -r a b c d <<<"$1"; echo $(( (a << 24) + (b << 16) + (c << 8) + d )); }
overlaps() { # two CIDRs
  local a="${1%/*}" am="${1#*/}" b="${2%/*}" bm="${2#*/}" m
  m=$(( am < bm ? am : bm ))
  [ $(( $(to_int "$a") >> (32 - m) )) -eq $(( $(to_int "$b") >> (32 - m) )) ]
}
taken=0
for network in $(docker network ls --format '{{.Name}}'); do
  [ "$network" = "${project}_inner" ] && continue
  for other in $(docker network inspect -f '{{range .IPAM.Config}}{{.Subnet}} {{end}}' "$network"); do
    case "$other" in *:*) continue ;; esac
    if overlaps "$subnet" "$other"; then bad "$subnet overlaps network $network ($other): set INNER_SUBNET and ORCH_INNER_IP to a free range"; taken=1; fi
  done
done
[ "$taken" = 0 ] && ok "$subnet overlaps no existing Docker network"
case "${ORCH_INNER_IP:-172.31.250.10}" in "${subnet%.*}".*) ok "the orchestrator's inner address is in it" ;; *) warn "ORCH_INNER_IP (${ORCH_INNER_IP:-172.31.250.10}) does not look like an address in $subnet" ;; esac

section "Names and images"
mine_addresses=" $(hostname -I 2>/dev/null) "
for host in "${ORCH_HOST:-}" "${POOL_API_HOST:-}"; do
  [ -n "$host" ] || continue
  address="$(getent ahostsv4 "$host" 2>/dev/null | awk 'NR==1{print $1}')"
  if [ -z "$address" ]; then bad "$host does not resolve yet"
  elif [[ "$mine_addresses" == *" $address "* ]]; then ok "$host resolves to this host ($address)"
  else warn "$host resolves to $address, which is not an address of this host. That is right if a proxy is in front; the orchestrator's name must reach this host directly, with nothing ending TLS on the way"; fi
done
for image in "${ORCHESTRATOR_IMAGE:-}" "${OPENPOOL_IMAGE:-}" "${REDPANDA_IMAGE:-}" "${AGENT_IMAGE:-}" "${TUNNEL_IMAGE:-}"; do
  [ -n "$image" ] || continue
  if docker image inspect "$image" >/dev/null 2>&1 || docker manifest inspect "$image" >/dev/null 2>&1; then ok "$image can be had"
  else bad "$image is not in its registry (or needs a login this host does not have)"; fi
done

printf '\n%s problem(s), %s warning(s). Nothing was started or changed.\n' "$problems" "$warnings"
[ "$problems" = 0 ]
