# Open Pool: design of the pool service

**Status:** steps 1 to 6 of the [build order](#build-order) are built, 2026-10-01; the end-to-end harness is design.\
**Companion:** `doc/live-runner-pool.md` describes the orchestrator side, which is built (changes 0 to 8 on `feat/open-pool-enhancements`).\
**Reader:** Mike, who made these decisions in conversation and is checking them before the build; later, whoever builds or operates the pool.

Open Pool lets people with GPUs ("members") serve Livepeer Live Runner apps through an orchestrator they do not own, and pays them for it. The orchestrator admits, routes and records. Everything else lives here: who may join, what each machine runs, whether it works, what it earned, and paying it out.

Points marked **(assumed)** were not decided in conversation. They are collected in [Assumptions to confirm](#assumptions-to-confirm).

## What is being built

| Piece | Language | Runs | Does |
| --- | --- | --- | --- |
| Pool service | Go, own module in `pool/` | once per region, beside one orchestrator node | members, machines, assignments, credentials, admission, tests, earnings, payouts, emails, the API |
| Member agent | Go, same module | on each member machine | keeps the machine running what the pool assigned it |
| Web sites | TypeScript (Lit, Effect, modern CSS, semantic HTML; SEO on public pages) | three static sites | `open-pool.com` marketing, `app.open-pool.com` members, `admin.open-pool.com` operator |
| go-livepeer additions | Go | in the orchestrator | three small additions, listed [below](#go-livepeer-additions) |

The pool service and go-livepeer share no code. `pool/` has its own `go.mod`, so neither can import the other and the directory can move to its own repository unchanged. They talk over four channels: the admission webhook, the orchestrator's CLI port, probe requests on the orchestrator's public port, and Kafka.

```text
                    open-pool.com   app.open-pool.com   admin.open-pool.com
                          └──────────────┬───────────────────┘
                              browser calls each region's API
              ┌──────────────────────────┴──────────────────────────┐
     region us-east                                          region eu-west
  ┌────────────────────┐                                  ┌────────────────────┐
  │ pool service  ◀────┼── webhook ── orchestrator node   │ pool service …     │
  │  SQLite            ├── CLI port ─▶                    │                    │
  │  payout wallet     ├── probes ───▶                    │                    │
  │                ◀───┼── Kafka ────                     │                    │
  └─────────▲──────────┘                                  └────────────────────┘
            │ desired state, reports            ▲ tunnel (work)
     member machine: agent ─ docker ─▶ app container + livepeer_runner_tunnel
```

## Decisions

| Topic | Decision |
| --- | --- |
| Regions | One pool service per orchestrator node. An operator may run several nodes in different regions. Each region is independent: its own database, payout wallet and payouts. The sites show all regions together and keep working when one is down. |
| Joining | People apply through the member site and the operator approves them. An applicant chooses one or more regions. |
| Member sign-in | Sign in with an Ethereum wallet. |
| Operator sign-in | Email one-time code. One admin email, set in the deployment config. |
| Email | Resend SDK with a configured base URL and API key. Sent to the admin only: a new application needs approval; a payout batch needs approval. Members get no email; everything is visible in the site. |
| Application form | Payout address and the machines the applicant has, each with GPUs chosen from a list the operator maintains in the admin site. |
| Apps | The operator decides what each machine runs. A machine can be assigned more than one app. |
| Machine | One box with one or more GPUs. One install, one credential. |
| Running apps | A member agent with access to the Docker socket applies a signed compose file the pool generates. Images are upstream images the operator picks, pinned by digest; Open Pool publishes none of its own. |
| Platform | Linux with NVIDIA and Docker. Others can be certified later. |
| Payout address | The wallet the member signs in with. Cannot be changed; a different address means applying again. |
| Operator's own capacity | Joins like any member's. There are no static runners in a pool. |
| Banned members | Not paid for the window they were banned in unless the operator switches them on in the batch. Money not paid stays in the payout wallet. |
| Reinstatement | A runner suspended by tests becomes active again by itself after enough passes. |
| Kafka | Required in every region; earnings arrive as events. |
| Tests | One test per app, defined by the operator. A pass promotes a runner automatically, repeated failures suspend it automatically, and the operator can override. |
| App versions | A new required version applies to new registrations at once; runners on the old one get a deadline, then are suspended. |
| Reassigning | Running sessions finish before a machine switches app. |
| Machines later | A member may request more machines (needs approval) and remove their own freely. |
| Earnings pot | What the region's node redeemed, known from the node itself, not looked up on-chain. Each node redeems its own tickets. A ticket counts in the round it was redeemed. |
| Payout window | 7 rounds. |
| Payout rule | Every member is paid `pot × (1 − commission) × member's fees ÷ all fees`. The commission is the orchestrator's and defaults to 15%. The operator's own machines are paid like anyone's if they join as a member. |
| Minimum payout | None. Whatever was earned in a window is paid. |
| Gas | Paid by the pool, from the payout wallet. |
| Approval | A payout batch waits for the operator's approval by default; this can be set to automatic. |
| Fee cut | The orchestrator keeps 100% of fees (no share to delegators), which is what makes the payout rule add up. |
| Terms | Global terms for the whole pool, and optionally regional terms per region. Accepted by signing up. Members can be banned for any reason at any time; malicious use is an immediate ban. Editable in the admin site and versioned. |
| Public data | Apps, prices, capacity, and a leaderboard ranked by earnings. A marketing site explains how to connect and sign up and carries the terms. |
| Name and domains | Open Pool. Three separate UI apps: `open-pool.com` marketing, `app.open-pool.com` members, `admin.open-pool.com` operator. |
| Storage | SQLite. |
| Orchestrator control | Through the orchestrator's CLI port. |

## Terms used here

- **Region:** one orchestrator node and its pool service.
- **Member:** an approved applicant, identified by a wallet address, which is also where they are paid.
- **Machine:** one box a member declared, with its GPUs.
- **App:** a catalog entry the operator defined: what customers ask for, what it costs, how to run it and how to test it.
- **Assignment:** the operator's decision that a machine runs an app, with a cap on concurrent sessions.
- **Credential:** the secret a machine's agent presents. One per machine, shown once.
- **Runner:** one app running on one machine, as the orchestrator sees it. A machine with two assignments is two runners.
- **Test:** the operator's check that a runner really serves its app, run through an unpaid probe session.
- **Round:** a Livepeer round. Every earnings event is stamped with the round it happened in.
- **Window:** 7 consecutive rounds.
- **Pot:** the face value of the winning tickets the region's node redeemed during a window.
- **Batch:** the list of payments for one window.

## One member, start to finish

Alice has a box with two RTX 4090s.

1. On `app.open-pool.com` she signs in with her wallet, picks region `us-east`, adds one machine with two RTX 4090s, and submits. Signing up accepts the current terms.
2. The operator gets an email. On the admin site they approve Alice and assign her machine the app `vllm/qwen2.5-7b-instruct` with a cap of 4 sessions.
3. Alice's machine page now shows one command. She runs it; it starts the agent with her credential.
4. The agent fetches the machine's compose file, checks its signature, and starts two containers: the vLLM image the operator pinned, and `livepeer_runner_tunnel` beside it.
5. The tunnel registers with the orchestrator, which asks the pool service. The pool service recognizes the credential, checks the app, version and capacity against the assignment, and admits the runner on probation.
6. The pool service runs the app's test against the runner. It passes, and the runner becomes active. Customers can now reach it.
7. Customers use it. Each fee the orchestrator debits arrives as an event and is added to Alice's earnings for that round.
8. After round 7 the window closes. The node redeemed 0.5 ETH in those rounds; Alice's runners earned 20% of all fees. Her payment is `0.5 × 85% × 20% = 0.085 ETH`. The operator gets an email, reviews the batch and approves it. The payout wallet sends 0.085 ETH to Alice's address and her payouts page shows the transaction.
9. Later the operator moves her machine to another app. Her runner stops taking new work, finishes its sessions, and the agent switches containers without Alice doing anything.

## The pool service

### Layout

```text
pool/
  DESIGN.md
  go.mod
  cmd/openpool/         the pool service
  cmd/openpool-agent/   the member agent
  docker/               Dockerfile.agent, the agent's image
  internal/
    config/             deployment settings from the environment
    store/              SQLite schema and migrations
    core/               the pool's rules: catalog (GPUs, apps, terms, settings), applications,
                        members, machines, assignments, credentials, admission, runners, audit,
                        and the pass that tests, promotes, suspends and drains runners
    auth/               wallet sign-in, admin code, session tokens
    orchestrator/       CLI-port client, and the prober that tests a runner through the probe route
    mail/               the admin emails and the sign-in code
    api/                HTTP handlers
    testkit/            what the tests share
    ledger/             Kafka consumer, earnings per member per round, pot per round, demand
    payouts/            windows, batches, approval, the payout wallet, sending
    compose/            the compose file a machine runs: generating it, signing it, and the
                        allowlist an agent checks it against. Shared by the service and the agent
    agent/              the member agent: fetch, verify, apply with Docker, report
  web/                  the three sites, a pnpm workspace of its own; see web/README.md
```

### Configuration

Deployment settings come from a config file or environment. Pool policy lives in the database and is edited on the admin site.

| Deployment setting | Purpose |
| --- | --- |
| Region id and name | e.g. `us-east`, "US East" |
| Listen address, public API URL | where the sites and agents reach this service |
| Database path | the SQLite file |
| Orchestrator service URL, CLI URL, pool secret | the node this service belongs to; the secret is `-liveRunnerPoolSecret` |
| Kafka brokers, topic, consumer group | where the node's events are read from |
| Ethereum RPC URL, chain id | Arbitrum One, for sending payments |
| Payout keystore and password file | this region's payout wallet |
| Session signing secret | shared by all regions, so a session from one is accepted by the others |
| Compose signing key file | signs the files agents apply; created on first start. Agents trust the key they were installed with, so it must be kept: losing it means every member installs again |
| Agent image, tunnel image | the agent's image, shown in the install command, and the image holding `livepeer_runner_tunnel`, pinned by digest. Without a tunnel image the agent is not offered and members start their runners by hand |
| Admin email | the one operator address |
| Resend base URL, API key, from address | email |
| Allowed web origins | the three sites |

| Pool policy (admin site) | Default |
| --- | --- |
| Commission | 15% |
| Window length | 7 rounds |
| Payout approval | required |
| Test interval | 10 minutes **(assumed)** |
| Failures before suspension, passes before reinstatement | 3 and 3 **(assumed)** |
| Version deadline | 24 hours **(assumed)** |

### What it stores

| Table | Holds |
| --- | --- |
| `settings` | pool policy |
| `gpus` | supported GPU models: name, memory, whether still accepted |
| `apps` | name, mode, price, required version and the previous one with its deadline, container definition, eligible GPUs, default and maximum capacity, test, discovery text, whether offered |
| `terms` | each published version of the global terms and of this region's terms, its text and hash |
| `applications` | address, display name, machines, global and regional terms versions, signature, status, reason |
| `members` | address, display name, status (active, banned, left), reason |
| `machines` | member, label, declared GPUs, the agent's last report (containers, GPUs found, agent version, error), status, last seen |
| `assignments` | machine, app, capacity cap, status (active, draining, removed), the app version and container the machine was last told to run, whether it is being moved to a new one |
| `credentials` | machine, hash of the secret, created, revoked |
| `runners` | what the orchestrator reports per runner, joined to its machine: state, status, hold, last test |
| `test_results` | runner, time, pass or fail, latency, detail |
| `events` | ids of events already processed, so a redelivered event is ignored |
| `sessions` | session id, runner, member, payer, rounds, fee totals |
| `earnings` | fees per member, app and round |
| `redemptions` | redeemed value per round |
| `refusals` | requests turned away per app and hour |
| `batches`, `batch_items` | windows and the payments in them |
| `nonces`, `admin_codes` | sign-in challenges |
| `audit` | who did what, when |

### Background work

- Read the orchestrator's events from Kafka into the ledger.
- Poll the orchestrator's runner list and current round.
- Run tests on schedule and apply promotion, suspension, version deadlines and draining.
- Move machines to their apps' current versions, a few at a time.
- Push the price table to the orchestrator when the catalog changes and whenever the orchestrator comes back.
- Close windows, build batches, send approved payments, confirm them.

## Features

### Signing in

**Members and applicants** sign a standard "Sign-In with Ethereum" message for the domain `app.open-pool.com` on Arbitrum One. The service issues a one-time nonce, verifies the signature, and returns a session token for that address. Ordinary wallets only in this version; contract wallets are not supported **(assumed)**.

**The operator** enters the admin email on `admin.open-pool.com`. If it matches the configured address, a six-digit code is emailed, valid for ten minutes, usable once, with attempts limited. Entering it returns an admin session token.

**Sessions** are signed tokens carrying who and until when (12 hours **(assumed)**). All regions share the signing secret, so a token from any region works on every region: one wallet signature or one emailed code signs a person in everywhere, and it keeps working if the region that issued it goes down. The sites send the token as a bearer header; no cookies cross domains.

### Applying and approval

An application carries: the signer's address, a display name for the leaderboard **(assumed)**, one or more machines each with a label and its GPUs (model and count, from the supported list), and the terms version in force. It is sent to each region the applicant chose; each region decides separately.

Submitting takes a second wallet signature, over a short text the service composes from the application: the region, the address, the display name, the number of machines, and the version and hash of the terms in force. That signature is the applicant's acceptance of those terms and is stored with the application. If the terms change between composing the text and submitting, the signature no longer matches and the applicant is asked to sign again.

The operator sees pending applications across regions and approves or rejects each with a reason. Apps can be assigned to the applicant's machines at approval or later. The applicant's site shows the status and the reason.

A member whose application is approved in two regions is two independent memberships that share an address.

### Catalog

**Supported GPUs** are a list the operator edits. Retiring a model stops new applications from choosing it and leaves existing machines alone.

**An app** is defined by:

| Field | Meaning |
| --- | --- |
| Name | the Live Runner app name customers ask for, e.g. `vllm/qwen2.5-7b-instruct` |
| Mode | single-shot or persistent |
| Price | amount, USD, per hour, per 720p, or fixed. Becomes the orchestrator's price for the app |
| Container | image pinned by digest, arguments, environment, the port the app listens on, its health path, GPU count, shared memory, a cache volume for model files |
| Required version | the string runners must report. A version is the string and the container together and they change in one action, which starts a [rollout](#tests-promotion-and-suspension) |
| Eligible GPUs | which supported models may be assigned it |
| Capacity | default and maximum concurrent sessions per machine |
| Test | see [Tests](#tests-promotion-and-suspension) |
| Discovery text | what customers see in discovery |
| Offered | pausing an app stops new work and hides it from customers |

Apps and prices are per region. The admin site can copy an app to another region.

### Machines, assignments and credentials

- A machine belongs to one member in one region and is approved by the operator, at application or when the member requests it later. A member removes their own machine freely; its runners are drained and removed.
- The operator assigns apps to a machine, each with a session cap no higher than the app's maximum. The site only offers apps whose eligible GPUs match the machine.
- A machine has one credential. The member creates it on the machine's page, where it is shown once together with the install command. Creating a new one revokes the old, and the member starts the agent again with the new one.
- Changing or removing an assignment drains it first: the runner stops taking new work, its sessions finish, then it is removed and the machine's compose file changes.

**What the operator sees when deciding.** Per app in the region: runners online, slots busy out of total, sessions and fees in the last 24 hours, and requests turned away for lack of capacity. This table will be adjusted once it shows real data.

### Admission

The orchestrator calls `POST /v1/orchestrator/admit` when a runner registers, with the credential and the runner's description. The pool service allows the registration only if all of these hold:

1. The credential belongs to a machine, is not revoked, and the machine's member is active.
2. The machine has an active assignment for the runner's app, and the app is offered.
3. The runner's mode matches the app's.
4. The runner's version is the one the machine was last told to run, or the app's required version, or the previous one while its deadline has not passed.
5. The runner's capacity is within the assignment's cap.

It answers with the member, the payout address, and the state `probation`. A refusal carries a reason the member's agent logs, for example `this machine is not assigned flux/schnell`.

One runner per machine and app. If that machine already has a runner for the app, the new registration replaces it: the old one is evicted **(assumed)**. This is what lets a machine reconnect after a network drop without waiting for the old registration to expire.

### The member agent

The agent is one container the member starts once, with the command the machine's page shows when the credential is created:

```text
docker run -d --name openpool-agent-us-east --restart unless-stopped --gpus all \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e OPENPOOL_CREDENTIAL=<shown once> \
  tztcloud/openpool-agent --pool https://us-east.api.open-pool.com --key <compose public key>
```

`--gpus all` is what lets the agent list the machine's GPUs, and it fails at once on a box without the NVIDIA container toolkit, which is the first thing a member needs to know. With rootless Docker the socket to mount is the one `DOCKER_HOST` names.

Every 30 seconds it repeats three steps:

1. **Fetch** `GET /v1/agent/desired`: the machine's compose file, the machine id, when it was issued, and the pool's Ed25519 signature over all three.
2. **Verify and apply.** It checks the signature against the public key it was started with and refuses anything else, a file issued for another machine, and a file older than one it already applied. It then checks the file against its own allowlist, whoever signed it: decoding is strict, so a key the agent does not know is refused, and privileged mode, host networking, host process space, added capabilities, devices, published ports, secrets, includes and volume or network drivers are keys it does not know. Images must be pinned by digest; a mount must be a named volume declared in the file; a network must be one declared in the file; only NVIDIA GPUs may be reserved. Then it runs `docker compose up --detach --remove-orphans` when the file changed, and again every five minutes, which brings back a container removed by hand.
3. **Report** `POST /v1/agent/report`: the containers and their state and health, the GPUs `nvidia-smi` lists, its own version, the file it applied, and the error if it could not apply it. Member and operator see the report on the machine's page, so "why is nothing running" has an answer there.

The compose file has, per assignment, the app's container and a `livepeer_runner_tunnel` container on a private network of their own. The app's port is never published on the host. The credential is not in the file: the file says `${OPENPOOL_CREDENTIAL}` and the agent supplies the value from its own environment when it applies it, so the signed file contains no secret. A `$` in anything the operator wrote is escaped, so it cannot become a variable. Model files go in a named volume per app, which a new version of the app keeps.

A draining assignment stays in the file until its runner is gone, which is what lets its sessions finish. Containers are kept in a Compose project named after the pool's address, so a machine that joined two regions runs one agent for each without either removing the other's containers.

What the agent does when things go wrong:

| Situation | The agent |
| --- | --- |
| The pool is unreachable or answers with an error | keeps running what it was last told, and tries again |
| The file fails the signature or the allowlist | leaves Docker alone, logs why, and reports the reason to the pool |
| Docker refuses the file (an image cannot be pulled, no GPU) | reports Docker's error and tries again on the next round |
| The pool refuses the credential (machine removed, member banned, credential replaced) | leaves everything running for an hour, so sessions of a removed machine can finish, then stops what the pool had it start. Volumes stay |
| The agent itself stops | the containers keep running and restart by themselves; the agent picks them up when it is back |

The tunnel image is built from go-livepeer's `docker/Dockerfile.runner-tunnel` (the tunnel binary alone, a few megabytes) and the agent's from `docker/Dockerfile.agent` (the agent, the Docker CLI and its compose plugin).

What this gives: a member installs once; version changes and reassignments need nothing from them; the operator sees real hardware.

What it does not give: proof. A member owns the machine and can replace an image, patch the agent or read customer inputs. Nothing short of hardware attestation prevents that. The agent makes the honest path the easy one; catching cheats is the job of the tests, the operator's approvals, and bans. Members in turn must trust the agent with their Docker socket, which is effectively root on their box. The terms say so, and the agent's source is public.

### Tests, promotion and suspension

A test is: a request (method, path, headers, body), what a good answer looks like (status, and text or JSON values the body must contain), and a time limit. For a persistent app the test opens a session first, sends the request to the session, and stops the session. Video itself is not checked in this version.

Tests run through the orchestrator's probe: an unpaid session on one named runner that the runner cannot tell from paid work.

| Runner is | Test result | Then |
| --- | --- | --- |
| on probation | pass | active |
| on probation | fail | stays on probation; retested |
| active | 3 failures in a row | suspended |
| suspended by tests | 3 passes in a row | active |
| any | operator sets a hold | the pool keeps testing it and otherwise leaves it alone until the hold is lifted |

A runner is tested when it registers and then on the test interval, while it reports itself ready. Every result is kept for two weeks and shown to the member with its detail, so "why is my runner suspended" has an answer on the page. The operator can also test a runner on demand.

**Holds.** When the operator sets a runner's state by hand, that is a hold: the pool stops promoting, suspending and draining that runner. Lifting the hold hands it back; if it is suspended at that point, three passes bring it back. An app with no test defined is never promoted by the pool: its runners wait on probation for the operator.

**New versions.** A version of an app is its version string and its container, and the operator changes both in one action. The new version then reaches machines a few at a time, so the app is never without runners: at most a quarter of the machines running it, and always at least one, are being moved at once. Idle machines go first. A machine being moved:

1. stops taking new work (its runner is suspended, reason "upgrade") and finishes its sessions;
2. is then told the new version: its compose file changes, its agent replaces the containers, and the new runner registers, replacing the old one;
3. counts as moved once the new runner has passed its test and is active. If that has not happened after 15 minutes the pool moves on to the next machine anyway, and a runner it had stopped goes back to the tests.

A machine with nothing running is told the new version at once. A runner the operator put on hold is not moved until the hold is lifted.

**Version deadline.** Until its turn comes, a machine keeps running the version it was last told to run, and is admitted on it, whatever the deadline says: the deadline is for machines that were told and did not follow, which in practice means runners started by hand. Such a runner still on the old version when the deadline passes is suspended, and passing tests do not bring it back.

**Draining.** Unassigning an app, or removing a machine, does not cut its runner off. The runner is suspended so it takes no new work, and it is removed once the sessions it was running have finished. Assigning the app again before then keeps the runner. A ban is the exception: it removes a member's runners at once.

### Earnings, windows and payouts

**Earnings.** The service reads the orchestrator's events from Kafka, one topic per region. Each fee event adds to `earnings` for its member, app and round. Events are recorded by id, so a redelivered event changes nothing. A session's released event carries the session's totals; a session is worth the larger of that total and the sum of its fee events, so lost fee events do not lose a member money and nothing is counted twice, whatever order events arrive in. The pool's own tests are unpaid and are not counted as work.

Events can also be uploaded to `POST /v1/orchestrator/events` with the pool secret, as a JSON array or one event per line. The orchestrator writes any event it had to drop to its log whole; this is how such events are put back. Uploading an event twice changes nothing.

**Pot.** Each ticket the node redeems arrives as an event with its value and round. The pot for a window is the sum over its rounds.

**Windows.** Windows are consecutive blocks of 7 rounds, counted from the first round the pool saw. The service reads the current round from the orchestrator. When it passes the end of a window, the service waits a settle delay (ten minutes by default) for events still in flight, then closes the window and builds its batch. A closed window is settled: an event that arrives late for one of its rounds is booked in the window still open, where it will be paid. A service that was down through several windows closes them one after another when it comes back.

**The batch.**

```text
payable        = pot × (1 − commission)
member payment = payable × member's fees ÷ all fees in the window
```

"All fees" are the fees of members' runners. A pool has no other runners: the operator's own machines join as a member. Should the orchestrator report fees for a runner that belongs to no member (a static runner left in its config), the admin site flags it and those fees are left out of the split. Amounts round down to the wei.

A member banned during the window is listed and marked, and is not paid unless the operator switches them on. Money not paid stays in the payout wallet.

One property of this rule worth knowing: winning tickets are a lottery, so a window can have fees and a small pot, or the reverse. Members are paid from what was actually redeemed in the window, split by who earned fees in that same window.

**Approval and sending.**

| Batch state | Meaning |
| --- | --- |
| open | the window is still running |
| ready | built; the operator has been emailed |
| approved | by the operator, or at once when approval is automatic |
| sending | payments are going out |
| sent | every payment confirmed |
| blocked | the payout wallet cannot cover what is left plus gas; the batch says how much is missing and resumes by itself once the wallet is funded |
| failed | some payments could not be made, for instance to an address that rejects ETH; the others were paid. The operator can retry |
| empty | the window closed with nothing to pay: nothing redeemed, or no member earned fees |

Each payment is one transfer from the region's payout wallet on Arbitrum One, one batch at a time, oldest first. A payment gets exactly one signed transaction, and that transaction is stored before it is broadcast. After a restart the service finds the stored transaction and either sees it confirmed or sends that same transaction again; it signs another only when the first can never be mined. A crash at any point therefore cannot pay twice. The service refuses to send at all if the Ethereum node is on another chain than the one configured. Gas is paid by the wallet and not deducted from members.

The operator funds the payout wallet. The admin site shows its balance and what the next batch needs.

### Public data and the leaderboard

Each region serves, without sign-in: its name, the apps it offers with prices and capacity, how many runners are online, the supported GPUs, the current terms, and a leaderboard. The leaderboard ranks members by earnings in the current window and all time, showing a display name and a shortened address. The sites merge regions by address.

### Terms

There are two kinds, both edited on the admin site and published as versions:

- **Global terms** apply to the whole pool. They state that signing up is acceptance, that a member can be banned for any reason at any time, that malicious use is an immediate ban, and what the agent's access to the Docker socket means.
- **Regional terms** are optional and apply to one region, on top of the global ones.

An application to a region records the global version and that region's version in force, with their hashes, and is covered by the applicant's wallet signature. A new version applies from the moment it is published; members are not asked to accept again, and their site shows that the terms changed **(assumed)**.

Regions are independent, so the global terms are held as a copy in every region; regional terms are held only in theirs. The admin site publishes the global terms to every region at once and shows any region that is out of step, with a button to bring it level. The supported GPU list is kept level across regions the same way **(assumed)**.

### Emails

All go to the admin address through Resend.

| Email | When |
| --- | --- |
| Sign-in code | the operator asks to sign in |
| Application needs approval | an application or a machine request arrives |
| Payout batch needs approval | a window closes, when approval is required |

### Audit log

Every operator action and every automatic state change is recorded with who or what did it, when, and why: approvals, rejections, bans, assignments, holds, evictions, catalog changes, batch approvals, payments.

## HTTP API

Per region, under `/v1`. JSON throughout. Errors are `{"error": "<text>"}` with a matching status.

| Group | Authenticated by | Endpoints |
| --- | --- | --- |
| Orchestrator | pool secret | `POST /orchestrator/admit`, `POST /orchestrator/events` |
| Agent | machine credential | `GET /agent/desired`, `POST /agent/report` |
| Public | none | `GET /public/pool`, `GET /public/apps`, `GET /public/gpus`, `GET /public/terms`, `GET /public/leaderboard` |
| Sign-in | none | `POST /auth/wallet/nonce`, `POST /auth/wallet/verify`, `POST /auth/admin/code`, `POST /auth/admin/verify` |
| Member | wallet session | `POST /applications/message`, `POST /applications`, `GET /me`, `GET /me/machines`, `POST /me/machines`, `DELETE /me/machines/{id}`, `POST /me/machines/{id}/credential`, `DELETE /me/machines/{id}/credential`, `GET /me/runners`, `GET /me/tests`, `GET /me/earnings`, `GET /me/payouts` |
| Admin: people | admin session | `GET /admin/applications`, `POST /admin/applications/{id}/approve`, `POST /admin/applications/{id}/reject`, `GET /admin/members`, `GET /admin/members/{id}`, `POST /admin/members/{id}/ban`, `POST /admin/members/{id}/unban` |
| Admin: machines | admin session | `GET /admin/machines`, `POST /admin/machines/{id}/approve`, `PUT /admin/machines/{id}/assignments`, `GET /admin/demand` |
| Admin: catalog | admin session | `GET/POST/PUT /admin/gpus`, `GET/POST/PUT /admin/apps`, `POST /admin/apps/{id}/version`, `POST /admin/apps/{id}/pause`, `GET/POST /admin/terms` |
| Admin: runners | admin session | `GET /admin/runners`, `POST /admin/runners/{id}/state` (also a hold), `POST /admin/runners/{id}/hold`, `POST /admin/runners/{id}/evict`, `POST /admin/runners/{id}/test`, `GET /admin/tests` |
| Admin: money | admin session | `GET /admin/batches`, `GET /admin/batches/{id}`, `PUT /admin/batches/{id}/items/{member}`, `POST /admin/batches/{id}/approve`, `POST /admin/batches/{id}/retry`, `GET /admin/wallet` |
| Admin: pool | admin session | `GET/PUT /admin/settings`, `GET /admin/audit`, `GET /admin/status` |
| Health | none | `GET /healthz` |

## The web sites

Three separate UI apps, one per domain, in one TypeScript workspace in `pool/web`. They share components and one data layer and are built and deployed separately.

| Site | Pages | Rendering |
| --- | --- | --- |
| `open-pool.com` | home; how it works; how to connect; apps and prices; leaderboard; terms; join (leads to the member site) | prerendered at build time for search engines; prices, capacity, the leaderboard and the current terms load live from the regions |
| `app.open-pool.com` | sign in; apply; application status; machines with install command; runners with test results; earnings; payouts | in the browser; not indexed |
| `admin.open-pool.com` | sign in; overview with each region's health; applications; members; machines and assignments with the demand table; catalog (GPUs, apps, tests); runners; payout batches and wallet; terms; settings; audit log | in the browser; not indexed |

Each app is a standalone Vite build into plain files (HTML, JavaScript, CSS) that any web server or CDN can host. No server process of ours runs behind them: the app runs in the visitor's browser and calls each region's pool service directly. How to build, configure and host them is in [`web/README.md`](web/README.md).

**Regions.** Each site reads `config.json` from beside its pages when it starts: the regions with their API addresses, and the addresses of its sister sites. Adding a region is editing that file. Every view that spans regions asks all of them at once, gives each 15 seconds, and shows what answered under each region's name. A region that did not answer is named on the page with the reason, and the rest of the page works. Actions go to the region they concern; what is done in several regions at once, such as publishing the pool's terms, reports each region's outcome.

**Sessions.** A session is a bearer token kept in the browser's storage and sent in the `Authorization` header. Because the regions share the signing secret, one sign-in is good in all of them: a wallet signs in at the first region that answers, and the operator's code is checked by the region that emailed it. A region that no longer accepts the session ends it everywhere on the site.

**The public site and search engines.** Its pages are written as HTML when it is built, each with its own title, description, canonical address and structured data, and listed in a sitemap. The parts that show live data are filled in by the browser; in the file each holds a sentence saying what loads there, so a page reads without scripts.

**Stack**, from the skills named for this work: Lit components with its reactive state, context and routing on platform APIs; Vite and Vitest; Effect for everything that is not a component, with every API response decoded by a schema before it is used; modern CSS without a utility framework; semantic HTML; Google's guidance for the public pages (titles, descriptions, structured data, sitemap, canonical URLs).

## go-livepeer additions

| Addition | Why |
| --- | --- |
| Every orchestrator event carries `round`, the current round when it happened | windows are counted in rounds |
| New event `orchestrator_ticket_redeemed` with the ticket's value, sender, transaction and round, emitted when the node's redemption succeeds (`pm/queue.go`, where the ticket is marked redeemed) | the pot, known from the node and not from the chain |
| New event `live_runner_requests_refused`, a count per app of requests answered `503` for lack of capacity, at most one event per app every ten seconds | the "turned away" column |

A ticket the node gives up on is also marked redeemed in its database; the event is emitted only when the redemption succeeded.

## What this version does not do

- Prove what a member's machine runs. See [The member agent](#the-member-agent).
- Protect customer inputs and outputs from the member serving them.
- Check video in tests for live-video apps.
- Notify members by email.
- Support contract wallets, or platforms other than Linux with NVIDIA and Docker.
- Fund the payout wallet automatically.
- Share anything between regions except the session signing secret.

## Build order

| Step | Work | Done when |
| --- | --- | --- |
| 1 | go-livepeer additions | **Built.** Events carry rounds; a redeemed ticket and refused requests produce events; Go tests |
| 2 | Pool service core: store, catalog, applications, members, machines, assignments, credentials, admission, orchestrator control, both sign-ins, the API for these | **Built.** A runner started with a machine credential is admitted on probation only for its assigned app; everything else is refused with a reason. Covered by the module's own tests and by a test in go-livepeer (`server/ai_http_openpool_test.go`) that runs the real pool service as a process beside a real orchestrator. |
| 3 | Tests, promotion and suspension, holds, version deadlines, draining | **Built.** A passing runner goes active by itself; a failing one is suspended and comes back after passing again; reassigning lets sessions finish. The go-livepeer test shows all of it against a real orchestrator and runner |
| 4 | Ledger, windows, batches, approval, sending, emails | **Built.** A window of events produces the batch the formula predicts; an approved batch pays each member once, also across a restart mid-send. The go-livepeer test feeds a real orchestrator's events to the pool service and sees the batch built and approved. Not exercised yet: a real Kafka broker (the tests upload events over HTTP, and the consumer loop is untested) and a real chain (the tests pay on an in-memory one that follows a node's nonce, receipt and error behaviour) |
| 5 | Member agent and signed compose files | **Built.** One command on a box brings its assigned app online beside its tunnel; changing the assignment or the app's version switches it with no action from the member; unassigning takes it down. Shown by running the agent's real image against the service and a real Docker daemon (`TestAgentImageOnRealDocker`, run on request), with the allowlist, the signature, the rollout and the agent's failure handling covered by the module's tests. Not exercised yet: a tunnel started by the agent registering with a real orchestrator, which is the harness (step 7), and a machine with NVIDIA GPUs (the test box has none, so the GPU reservation and `nvidia-smi` listing are untested on hardware) |
| 6 | The three sites | **Built.** Every page listed above works against two regions, and with one of them stopped: 31 end-to-end tests drive the three sites in a browser against two real pool services, with real wallet signatures and the operator's emailed code, and end by stopping each region in turn. Under them are the unit tests of the shared packages (the service's JSON decoded by schema, the workflows against stand-in regions, components in a real browser) and of the public site's rendered pages. Not exercised yet: a real orchestrator behind the regions (the tests use a stand-in for its CLI and service ports), a real browser wallet extension (the tests inject the same interface one provides), and a real payout wallet (payout batches are approved and shown, and nothing is sent) |
| 7 | End-to-end harness | one command runs an orchestrator, Kafka, a pool service, an agent and an echo app locally, through signup, approval, a paid call and a payout |

## Assumptions to confirm

These were not decided in conversation. Each is what the design above does unless changed.

1. **Display name.** The applicant gives one at signup for the leaderboard.
2. **A new registration for the same machine and app replaces the old runner.**
3. **Numbers:** tests every 10 minutes, 3 failures to suspend, 3 passes to reinstate, 24-hour version deadline, 12-hour sessions.
4. **Members are not asked to accept new terms again**; a new version applies when published.
5. **Global terms and the GPU list are copied to every region** and the admin site keeps the regions level.
6. **Ordinary wallets only** for sign-in in this version.
7. **The three apps are served as plain files** and call the regions from the browser, so each region's API must be reachable from the internet over HTTPS. *Confirmed 2026-10-01: three standalone Vite builds.*
