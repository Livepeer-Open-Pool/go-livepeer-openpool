# Open Pool service

The pool service for one region of Open Pool. It runs beside one go-livepeer orchestrator node and decides who may join, what each member's machine runs and whether it works, records what it earned, and pays it out. The design and the build order are in [DESIGN.md](DESIGN.md); the orchestrator's side is in [`doc/live-runner-pool.md`](../doc/live-runner-pool.md).

It is its own Go module and shares no code with go-livepeer.

The three web sites (public, member, admin) are in [`web/`](web/README.md), a TypeScript workspace of their own. They are plain files that call this service from the browser.

## Build and test

```bash
cd pool
go build -o openpool ./cmd/openpool
CGO_ENABLED=0 go build -o openpool-agent ./cmd/openpool-agent
go test ./...
```

Two tests drive a real Docker daemon and run only when asked to. The second runs the agent's real image, so it needs the two images below, the tunnel image pushed to a registry so that it has a digest:

```bash
OPENPOOL_DOCKER_TEST=1 go test ./internal/agent -run RealDocker
OPENPOOL_DOCKER_TEST=1 OPENPOOL_TEST_AGENT_IMAGE=<agent image> OPENPOOL_TEST_TUNNEL_IMAGE=<tunnel image>@sha256:<digest> \
  go test ./internal/api -run AgentImageOnRealDocker
```

`server/ai_http_openpool_test.go` in go-livepeer runs this service as a process beside a real orchestrator and takes a member from application to serving work. Run it from the repository root with `go test ./server/ -run OpenPoolServiceAndOrchestratorAgree`.

### The whole pool on one machine

```bash
pool/e2e.sh              # about three minutes
pool/e2e.sh keep-logs    # the same, keeping every process's log in pool/.e2e/
```

This runs everything for real and takes one member from signing up to being paid: a Livepeer devnet, an on-chain orchestrator in pool mode, Kafka, this service with a funded payout wallet, the member agent's image starting an app and its tunnel, and a customer paying with tickets. It builds the agent and tunnel images itself. It needs Go, Docker with the compose plugin, and network access to pull the devnet, Kafka, registry and echo images. `test/e2e/openpool_test.go` says what each step shows.

## Run

Configuration comes from `OPENPOOL_*` environment variables. Any of them can be given as `<NAME>_FILE`, naming a file that holds the value.

| Variable | Required | Default | Meaning |
| --- | --- | --- | --- |
| `OPENPOOL_REGION_ID` | yes | | Short id of this region, e.g. `us-east`: lowercase letters, digits and dashes |
| `OPENPOOL_REGION_NAME` | | the id | Name shown to people, e.g. `US East` |
| `OPENPOOL_LISTEN` | | `127.0.0.1:8090` | Address the API listens on |
| `OPENPOOL_PUBLIC_URL` | yes | | This API's address as the sites and agents reach it |
| `OPENPOOL_DB_PATH` | | `openpool.db` | The SQLite file |
| `OPENPOOL_ORCHESTRATOR_URL` | yes | | The orchestrator's public service address, which runners connect to |
| `OPENPOOL_ORCHESTRATOR_CLI_URL` | | `http://127.0.0.1:7935` | The orchestrator's CLI port |
| `OPENPOOL_ORCHESTRATOR_INSECURE_TLS` | | `false` | `true` skips certificate verification when testing runners through the orchestrator, for one that serves its default self-signed certificate |
| `OPENPOOL_POOL_SECRET` | yes | | The orchestrator's `-liveRunnerPoolSecret` |
| `OPENPOOL_RECONCILE_INTERVAL` | | `15s` | How often the service looks over the registered runners: which are due a test, which to drain, which run an old version |
| `OPENPOOL_SESSION_SECRET` | yes | | Signs session tokens; at least 32 characters, the same in every region |
| `OPENPOOL_SESSION_TTL` | | `12h` | How long a sign-in lasts |
| `OPENPOOL_SIGN_IN_DOMAINS` | | `app.open-pool.com` | Domains a wallet sign-in message may be for, comma-separated |
| `OPENPOOL_CHAIN_ID` | | `42161` | Chain id in wallet sign-in messages (Arbitrum One) |
| `OPENPOOL_ADMIN_EMAIL` | yes | | The operator's email, where sign-in codes and notices go |
| `OPENPOOL_ADMIN_URL` | | `https://admin.open-pool.com` | The admin site, for links in emails |
| `OPENPOOL_RESEND_API_KEY` | | | Resend API key |
| `OPENPOOL_RESEND_BASE_URL` | | Resend's own | Base URL of a Resend-compatible API |
| `OPENPOOL_MAIL_FROM` | | | From address of the emails |
| `OPENPOOL_ALLOWED_ORIGINS` | | the three Open Pool sites | Sites whose pages may call this API, comma-separated |
| `OPENPOOL_TUNNEL_IMAGE` | | | The image holding `livepeer_runner_tunnel` that member machines run beside each app, pinned by digest (`name@sha256:…`). Without it the agent is not offered and members start their runners by hand |
| `OPENPOOL_AGENT_IMAGE` | | `tztcloud/openpool-agent:latest` | The member agent's image, shown in the install command |
| `OPENPOOL_COMPOSE_KEY_FILE` | | `openpool-compose.key` | The key that signs the compose files agents apply. Created on first start. Keep it: agents trust the key they were installed with |
| `OPENPOOL_KAFKA_BROKERS` | | | Kafka brokers the orchestrator's events are read from, comma-separated |
| `OPENPOOL_KAFKA_TOPIC` | | | The orchestrator's `-kafkaOrchestratorTopic`. One topic per region |
| `OPENPOOL_KAFKA_GROUP` | | `openpool-<region id>` | Consumer group, which is how the service remembers where it stopped reading |
| `OPENPOOL_KAFKA_USER`, `OPENPOOL_KAFKA_PASSWORD` | | | Sign in to Kafka with SASL over TLS |
| `OPENPOOL_ETH_RPC_URL` | | | Ethereum node the payout wallet sends through. Set together with the keystore |
| `OPENPOOL_PAYOUT_KEYSTORE` | | | Keystore file of this region's payout wallet |
| `OPENPOOL_PAYOUT_PASSWORD` | | | The keystore's password; give it as `OPENPOOL_PAYOUT_PASSWORD_FILE` |
| `OPENPOOL_PAYOUT_INTERVAL` | | `30s` | How often windows are checked and payments moved forward |
| `OPENPOOL_SETTLE_DELAY` | | `10m` | How long after a window's last round the window is closed, so events in flight are counted |

Without `OPENPOOL_RESEND_API_KEY` and `OPENPOOL_MAIL_FROM`, email is not sent: notices and the operator's sign-in code are written to the service's log instead. That is for local development only.

Without Kafka, earnings are recorded only from events uploaded to `POST /v1/orchestrator/events`. Without a payout wallet, payout batches are built and can be approved, and nothing is paid.

The orchestrator is started in pool mode, pointing at this service:

```bash
livepeer -orchestrator -useLiveRunners \
  -liveRunnerAuthWebhookUrl https://us-east.api.open-pool.com/v1/orchestrator/admit \
  -liveRunnerPoolSecret <the same secret as OPENPOOL_POOL_SECRET> \
  -monitor -kafkaBootstrapServers <kafka> -kafkaOrchestratorTopic <topic> \
  …
```

It needs no `-liveRunnerPrices` file: the pool service sends the orchestrator the prices of the apps in its catalog, when the catalog changes and once a minute.

## The member agent

A member's machine runs one agent per region it joined. The agent fetches the machine's compose file from this service, checks the pool's signature and its own allowlist, applies the file with Docker, and reports back. The member starts it once with the command their machine's page shows; [DESIGN.md](DESIGN.md#the-member-agent) describes what it accepts and what it does when things go wrong.

The pool needs two images in a registry its members can pull from:

```bash
# The agent, from this directory.
docker build -f docker/Dockerfile.agent --build-arg VERSION=0.1.0 -t tztcloud/openpool-agent:0.1.0 .
docker push tztcloud/openpool-agent:0.1.0

# The tunnel, whose context is the repository root. Pushing prints the digest OPENPOOL_TUNNEL_IMAGE needs.
docker build -f ../docker/Dockerfile.runner-tunnel -t tztcloud/livepeer-runner-tunnel:0.9.3 ..
docker push tztcloud/livepeer-runner-tunnel:0.9.3
```

Then set `OPENPOOL_AGENT_IMAGE=tztcloud/openpool-agent:0.1.0` and `OPENPOOL_TUNNEL_IMAGE=tztcloud/livepeer-runner-tunnel@sha256:<digest>`. Changing the tunnel image changes every machine's compose file, so every agent restarts its tunnels on its next round; sessions in progress on them end.

The agent takes `--pool` (this service's public address), `--key` (the compose public key, which the service logs at start and `GET /v1/public/pool` returns), and `--interval` (default `30s`), and reads the machine's credential from `OPENPOOL_CREDENTIAL` or the file named by `OPENPOOL_CREDENTIAL_FILE`.
