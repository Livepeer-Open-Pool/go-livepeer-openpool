package runner

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"

	"github.com/hashicorp/yamux"

	"github.com/livepeer/go-livepeer/ai/runner/tunnelconn"
)

// LiveRunnerTunnelScheme is the scheme of the synthetic runner_url a tunneled
// runner registers with; the orchestrator dials such a runner through its
// tunnel instead of the network.
const LiveRunnerTunnelScheme = "tunnel"

// TunnelDialFunc opens one stream on a runner's tunnel. It has the shape of
// http.Transport.DialContext; the network and address are ignored.
type TunnelDialFunc func(ctx context.Context, network, addr string) (net.Conn, error)

// LiveRunnerTunnel is one connected tunnel: the yamux session the
// orchestrator opens streams on. Done is closed when the session ends.
type LiveRunnerTunnel struct {
	runnerID string
	session  *yamux.Session
	done     chan struct{}
	doneOnce sync.Once

	mu     sync.Mutex
	reason string
}

// Done is closed when the tunnel's session has ended for any reason.
func (t *LiveRunnerTunnel) Done() <-chan struct{} { return t.done }

// Reason says why the tunnel ended: "replaced", "removed", "closed" (the
// runner hung up or the keepalive timed out), or "" while it is connected.
func (t *LiveRunnerTunnel) Reason() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.reason
}

// Streams is the number of streams currently open on the tunnel.
func (t *LiveRunnerTunnel) Streams() int {
	return t.session.NumStreams()
}

func (t *LiveRunnerTunnel) closeWithReason(reason string) {
	t.mu.Lock()
	if t.reason == "" {
		t.reason = reason
	}
	t.mu.Unlock()
	_ = t.session.Close()
}

func (t *LiveRunnerTunnel) finish() {
	t.mu.Lock()
	if t.reason == "" {
		t.reason = "closed"
	}
	t.mu.Unlock()
	t.doneOnce.Do(func() { close(t.done) })
}

// AuthorizeTunnel says whether a tunnel may be attached to the runner with
// this Authorization value. It runs before the WebSocket upgrade so a refusal
// is an ordinary HTTP status: 404 for an unknown, expired, static, or
// non-tunneled runner, 401 for a wrong heartbeat secret.
func (r *LiveRunnerRegistry) AuthorizeTunnel(runnerID, auth string) error {
	runner, unlock, err := r.lockLiveRunner(runnerID)
	if err != nil {
		return err
	}
	defer unlock()
	return runner.authorizeTunnelLocked(auth)
}

func (runner *liveRunner) authorizeTunnelLocked(auth string) error {
	if runner.static || !runner.Tunnel {
		return &RunnerError{StatusCode: http.StatusNotFound, Message: "runner is not tunneled"}
	}
	if !validSecret(auth, runner.heartbeatSecret) {
		return &RunnerError{StatusCode: http.StatusUnauthorized, Message: "invalid tunnel authorization"}
	}
	return nil
}

// AttachTunnel runs a yamux session over conn and makes it the runner's
// tunnel. A tunnel already attached is closed with reason "replaced". The
// registry closes the session when the runner is removed, and clears it when
// the session ends on its own.
func (r *LiveRunnerRegistry) AttachTunnel(runnerID, auth string, conn net.Conn) (*LiveRunnerTunnel, error) {
	runner, unlock, err := r.lockLiveRunner(runnerID)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if err := runner.authorizeTunnelLocked(auth); err != nil {
		return nil, err
	}
	session, err := yamux.Server(conn, tunnelconn.YamuxConfig(io.Discard))
	if err != nil {
		return nil, &RunnerError{StatusCode: http.StatusInternalServerError, Message: fmt.Sprintf("tunnel session: %v", err)}
	}
	t := &LiveRunnerTunnel{runnerID: runnerID, session: session, done: make(chan struct{})}
	if old := runner.tunnel; old != nil {
		runner.tunnel = nil
		old.closeWithReason("replaced")
	}
	runner.tunnel = t
	slog.Info("live runner tunnel connected", "runner_id", runnerID, "app", runner.App, "remote", conn.RemoteAddr())
	go r.watchTunnel(runner, t)
	return t, nil
}

func (r *LiveRunnerRegistry) watchTunnel(runner *liveRunner, t *LiveRunnerTunnel) {
	<-t.session.CloseChan()
	runner.mu.Lock()
	if runner.tunnel == t {
		runner.tunnel = nil
	}
	runner.mu.Unlock()
	t.finish()
	slog.Info("live runner tunnel closed", "runner_id", t.runnerID, "app", runner.App, "reason", t.Reason())
}

// TunnelDialer returns a dialer that opens a stream on the runner's current
// tunnel, and false when the runner is not a tunneled runner. The tunnel is
// looked up at each dial, so a reconnected tunnel is used without any caller
// noticing; a dial while no tunnel is attached fails.
func (r *LiveRunnerRegistry) TunnelDialer(runnerID string) (TunnelDialFunc, bool) {
	r.mu.Lock()
	runner := r.runners[runnerID]
	r.mu.Unlock()
	if runner == nil {
		return nil, false
	}
	runner.mu.Lock()
	tunneled := runner.Tunnel && !runner.static
	runner.mu.Unlock()
	if !tunneled {
		return nil, false
	}
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		runner.mu.Lock()
		t := runner.tunnel
		runner.mu.Unlock()
		if t == nil {
			return nil, &RunnerError{StatusCode: http.StatusBadGateway, Message: "runner tunnel not connected"}
		}
		stream, err := t.session.OpenStream()
		if err != nil {
			return nil, &RunnerError{StatusCode: http.StatusBadGateway, Message: fmt.Sprintf("runner tunnel: %v", err)}
		}
		return stream, nil
	}, true
}

// readyLocked is the single rule for "this runner can take work": the
// status it reports, and for a tunneled runner a connected tunnel.
func (runner *liveRunner) readyLocked() bool {
	// readyLocked requires runner.mu.
	return isReadyStatus(runner.Status) && (!runner.Tunnel || runner.tunnel != nil)
}

func (runner *liveRunner) closeTunnelLocked(reason string) {
	// closeTunnelLocked requires runner.mu.
	if t := runner.tunnel; t != nil {
		runner.tunnel = nil
		t.closeWithReason(reason)
	}
}
