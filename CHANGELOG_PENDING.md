# Unreleased Changes

## v0.X.X

### Breaking Changes 🚨🚨

### Features ⚒

#### General

#### Broadcaster

#### Orchestrator

- Live Runners can register with `tunnel: true` and receive their work over a connection they open (`GET /runners/{id}/tunnel`, WebSocket carrying a yamux session), so a runner needs no public address; the new `livepeer_runner_tunnel` command is the runner side for any plain HTTP app.

#### Transcoder

### Bug Fixes 🐞

#### General

#### Broadcaster

#### CLI
