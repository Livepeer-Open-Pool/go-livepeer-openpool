# Deploying Open Pool

Two compose files, deployed separately, both behind a Traefik you already run:

| Directory | What it runs | How many |
| --- | --- | --- |
| [`region/`](region/docker-compose.yml) | an orchestrator in pool mode, Kafka (one Redpanda node) and the pool service | one per region, each on its own host |
| [`sites/`](sites/docker-compose.yml) | the public, member and admin sites | one, anywhere |

Neither builds anything: they run images from your registry, by the tags in their `.env`.

## 1. Build and publish the images

```bash
pool/build-images.sh        # builds and tags all seven; pushes nothing
```

It ends by listing the tags and the `docker push` commands. The seven images:

| Image | What it is | Runs |
| --- | --- | --- |
| `tztcloud/go-livepeer:v<version>-<commit>` | the orchestrator, built from this branch | in `region/` |
| `tztcloud/openpool:<version>` | the pool service | in `region/` |
| `tztcloud/livepeer-runner-tunnel:v<version>-<commit>` | the tunnel, alone | on members' machines, beside each app |
| `tztcloud/openpool-agent:<version>` | the member agent | on members' machines |
| `tztcloud/openpool-web-marketing:<version>` | the public site | in `sites/` |
| `tztcloud/openpool-web-member:<version>` | the member site | in `sites/` |
| `tztcloud/openpool-web-admin:<version>` | the admin site | in `sites/` |

Members' machines pull the agent and the tunnel, so those two must be in a registry anyone can pull from.

**The tunnel is named by digest.** A machine is told to run images by digest, never by tag, and a registry assigns the digest when the image is pushed. After pushing the tunnel:

```bash
docker image inspect --format '{{index .RepoDigests 0}}' tztcloud/livepeer-runner-tunnel:<tag>
```

prints `tztcloud/livepeer-runner-tunnel@sha256:…`, which is the value of `TUNNEL_IMAGE` below.

**The public site is built for its addresses.** Its pages carry their own address and the member site's. They default to `https://open-pool.com` and `https://app.open-pool.com`; for others, set `OPENPOOL_SITE_URL` and `OPENPOOL_MEMBER_URL` when building.

## 2. A region

On the region's host, with this directory copied there:

```bash
cd region
cp .env.example .env        # and fill it in
```

`.env` holds the image tags, the names (`ORCH_HOST`, `POOL_API_HOST`, the three sites' hosts), your Traefik's network, entrypoint and certificate resolver, the chain's RPC address, the orchestrator's account and the operator's email. Point the DNS of `ORCH_HOST` and `POOL_API_HOST` at the host.

### Secrets

Each is a file in `region/secrets/`. All of them must exist before the first start.

| File | What it holds | How to make it |
| --- | --- | --- |
| `pool_secret` | shared by the orchestrator and the pool service | `openssl rand -hex 32` |
| `session_secret` | signs sign-in sessions; **the same file in every region** | `openssl rand -hex 32`, once, then copied |
| `orch_secret` | the orchestrator's own secret, which no pool member uses | `openssl rand -hex 32` |
| `orch_keystore/` | a directory holding the key file of the account the node signs with (`ORCH_ETH_ADDRESS`) | your existing keystore |
| `orch_eth_password` | that key's password | |
| `payout_keystore` | the key file of this region's payout wallet | `geth account new`, or any tool that writes a keystore file |
| `payout_password` | that key's password | |
| `resend_api_key` | the Resend API key the operator's emails are sent with | from Resend |

```bash
chmod 700 secrets && chmod 644 secrets/* && chmod 755 secrets/orch_keystore
```

The directory is closed to other users of the host; the files inside are readable so that the pool service, which does not run as root in its container, can read the ones it is given.

`SECRETS_DIR` in `.env` says where this directory is. Give its full path when the compose file is run from somewhere else, as Portainer does with a stack.

### Check it against what already runs on the host

```bash
./preflight.sh          # with sudo if Docker needs it
```

It starts nothing and changes nothing. It says what is still missing (settings, secrets, images not yet in the registry, names that do not resolve) and whether starting the region could touch anything already running:

- a Traefik router or service with one of this region's names, or a rule for one of its hostnames, on another container;
- another container running with the orchestrator's account;
- a Docker network overlapping the region's inner one;
- a port published on the host (the region publishes none);
- a TCP router that takes every name, which would take the orchestrator's connections.

Everything the region creates is named after it: the compose project `openpool-<region>`, its volumes and inner network, and the Traefik routers `openpool-api-<region>` and `openpool-orch-<region>`. It joins your Traefik's network and adds nothing else to it.

### Start it

```bash
docker compose up -d
docker compose ps                                  # all healthy
curl https://<POOL_API_HOST>/healthz               # 200
curl -k https://<ORCH_HOST>/discovery              # the orchestrator, through Traefik
```

### What Traefik is asked to do

| Service | Router | Why |
| --- | --- | --- |
| pool service | HTTP router for `POOL_API_HOST`, with a certificate from your resolver, to port 8090 | browsers call it from the three sites |
| orchestrator | TCP router for `ORCH_HOST` with TLS passed through, to port 8935 | an orchestrator serves its own self-signed certificate, which Livepeer gateways and the tunnel expect; Traefik forwards the connection without ending TLS |

Both use the same HTTPS entrypoint. If you would rather publish the orchestrator's port directly, remove its labels, add `ports: ["8935:8935"]`, and set `ORCH_PUBLIC_PORT=8935` in `.env`.

The orchestrator's CLI port, which controls the node, listens only on the compose file's inner network at a fixed address. It is not reachable from the Traefik network or from outside.

### The orchestrator's account

The node needs one key: the account it signs with and sends transactions from, `ORCH_ETH_ADDRESS`, whose key file is in `secrets/orch_keystore/`. There are two ways to set it up.

| | `ORCH_ETH_ADDRESS` | `ORCH_ONCHAIN_ADDRESS` | `ORCH_REWARD` |
| --- | --- | --- | --- |
| The node's account is the registered orchestrator | the orchestrator | empty | `true`, on one node |
| The registered orchestrator is another account, and this node only redeems tickets for it | an account of the node's own, holding only ETH for gas | the orchestrator | `false` |

In the second, tickets are made out to the orchestrator and their value is credited to it on-chain, while the key that holds its stake stays off this host. An account for each node is the cleaner arrangement. Nodes that share one still work: when two send at the same moment one of the transactions is refused and that node sends it again. `./preflight.sh` warns when another container on the host has this node's key.

What the pool counts as a window's pot is what this node redeemed, whichever account it was credited to.

### After the first start

1. **Activate the orchestrator on-chain**, if the node's account is the orchestrator and is not active yet. `livepeer_cli` may send transactions only while `ORCH_CLI_TX_ROUTES=true` is in `.env`: set it, `docker compose up -d`, run `docker compose exec orchestrator livepeer_cli -host 172.31.250.10 -http 7935`, and set it back to `false` afterwards. Set `ORCH_REWARD=true` if this node is to call reward. Skip this step when the orchestrator is another account (see below). Either way the orchestrator must keep 100% of fees (a fee share of 0% to delegators): the payout rule assumes the whole of what the node redeems is the pot.
2. **Fund the payout wallet** with ETH on Arbitrum. The admin site shows its address and balance.
3. **Sign in to the admin site** and set the region up: GPU models, apps, the terms. A region takes applications once the pool's terms are published in it.

### Trying it without a chain

To check names, Traefik and secrets before going on-chain, start with the orchestrator off-chain and no payout wallet:

```bash
docker compose -f docker-compose.yml -f offchain.override.yml up -d
```

Everything works except payments: sign in, define apps, connect a machine. Start again without the override when ready; the volumes are kept.

## 3. The sites

```bash
cd sites
cp .env.example .env                    # the three tags, the three hosts, your Traefik
cp config.example.json config.json      # your regions
docker compose up -d
```

`config.json` is mounted into all three sites and read by the browser each time a site is opened. It names the regions with their API addresses, in the order the sites list them and ask them. Adding a region is adding it to this file: nothing is rebuilt or restarted.

Each region's `.env` must name the three sites' hosts, or its pool service refuses their calls.

## 4. Another region

A second region is the `region/` directory on another host with its own `.env` (another `REGION_ID`, names, orchestrator account, payout wallet, Kafka topic) and **the same `session_secret`**. Then add it to the sites' `config.json`. Regions share nothing else: each has its own members, catalog, books and wallet. The admin site copies apps between regions and publishes the pool's terms to all of them.

## Keeping it running

- **Back up the `openpool` volume.** It holds the region's database (members, earnings, payments) and the key that signs what members' machines run. Without the key every member has to install the agent again.
- **Upgrade** by changing a tag in `.env`, then `docker compose pull && docker compose up -d`. The pool service is back within seconds; Traefik routes to it once it is healthy.
- **Changing `TUNNEL_IMAGE`** changes what every machine runs, so every agent restarts its tunnels on its next round and sessions on them end. Do it when the pool is quiet.
- **A new version of an app** is released on the admin site, not here. It reaches machines a few at a time.
- **Logs:** `docker compose logs -f openpool` and `orchestrator`. With no Resend key the operator's sign-in code is written to the pool service's log instead of being emailed.
