# Open Pool service

The pool service for one region of Open Pool. It runs beside one go-livepeer orchestrator node and decides who may join, what each member's machine runs and whether it works, and (in later steps) what it earned, and paying it out. The design and the build order are in [DESIGN.md](DESIGN.md); the orchestrator's side is in [`doc/live-runner-pool.md`](../doc/live-runner-pool.md).

It is its own Go module and shares no code with go-livepeer.

## Build and test

```bash
cd pool
go build -o openpool ./cmd/openpool
go test ./...
```

`server/ai_http_openpool_test.go` in go-livepeer runs this service as a process beside a real orchestrator and takes a member from application to serving work. Run it from the repository root with `go test ./server/ -run OpenPoolServiceAndOrchestratorAgree`.

## Run

Configuration comes from `OPENPOOL_*` environment variables. Any of them can be given as `<NAME>_FILE`, naming a file that holds the value.

| Variable | Required | Default | Meaning |
| --- | --- | --- | --- |
| `OPENPOOL_REGION_ID` | yes | | Short id of this region, e.g. `us-east` |
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

Without `OPENPOOL_RESEND_API_KEY` and `OPENPOOL_MAIL_FROM`, email is not sent: notices and the operator's sign-in code are written to the service's log instead. That is for local development only.

The orchestrator is started in pool mode, pointing at this service:

```bash
livepeer -orchestrator -useLiveRunners \
  -liveRunnerAuthWebhookUrl https://us-east.api.open-pool.com/v1/orchestrator/admit \
  -liveRunnerPoolSecret <the same secret as OPENPOOL_POOL_SECRET> \
  -monitor -kafkaBootstrapServers <kafka> -kafkaOrchestratorTopic <topic> \
  …
```

It needs no `-liveRunnerPrices` file: the pool service sends the orchestrator the prices of the apps in its catalog, when the catalog changes and once a minute.
