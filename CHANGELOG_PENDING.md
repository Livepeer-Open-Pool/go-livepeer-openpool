# Unreleased Changes

## v0.X.X

### Breaking Changes 🚨🚨

### Features ⚒

#### General

- Remote signer: leave evidence when a `create_signed_ticket` event is dropped (`livepeer_remote_signer_events_dropped_total` counter, an error log with the ids needed to reconstruct the record) and expose `livepeer_remote_signer_event_queue_length`. Add `-kafkaRequireDelivery` and `-kafkaEnqueueTimeout` to refuse signing when the event cannot be enqueued.

#### Broadcaster

#### Orchestrator

- Live Runners can register with `tunnel: true` and receive their work over a connection they open (`GET /runners/{id}/tunnel`, WebSocket carrying a yamux session), so a runner needs no public address; the new `livepeer_runner_tunnel` command is the runner side for any plain HTTP app.

#### Transcoder

### Bug Fixes 🐞

#### General

#### Broadcaster

#### CLI
