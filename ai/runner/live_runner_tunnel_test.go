package runner

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/hashicorp/yamux"
	"github.com/stretchr/testify/require"

	"github.com/livepeer/go-livepeer/ai/runner/tunnelconn"
)

func tunnelHeartbeat(t *testing.T, registry *LiveRunnerRegistry, runnerID string) *LiveRunnerHeartbeatResponse {
	t.Helper()
	resp, err := registry.Heartbeat(LiveRunnerHeartbeatRequest{
		RunnerID: runnerID, App: "test/app", Mode: LiveRunnerModeSingleShot, Capacity: 2, Tunnel: true,
	}, liveRunnerTestBootstrapSecret)
	require.NoError(t, err)
	return resp
}

// attachPipeTunnel attaches a tunnel over a net.Pipe and returns the runner
// side's session, which accepts the streams the orchestrator opens.
func attachPipeTunnel(t *testing.T, registry *LiveRunnerRegistry, runnerID, secret string) (*LiveRunnerTunnel, *yamux.Session) {
	t.Helper()
	orchSide, runnerSide := net.Pipe()
	done := make(chan *yamux.Session, 1)
	go func() {
		session, err := yamux.Client(runnerSide, tunnelconn.YamuxConfig(io.Discard))
		if err != nil {
			done <- nil
			return
		}
		done <- session
	}()
	tunnel, err := registry.AttachTunnel(runnerID, secret, orchSide)
	require.NoError(t, err)
	session := <-done
	require.NotNil(t, session)
	t.Cleanup(func() { _ = session.Close() })
	return tunnel, session
}

func TestTunnelRegistrationAssignsSyntheticRunnerURLAndWaitsForTheTunnel(t *testing.T) {
	registry := newLiveRunnerTestRegistry()
	resp := tunnelHeartbeat(t, registry, "runner-t")
	require.Equal(t, "runner-t", resp.RunnerID)
	require.NotEmpty(t, resp.HeartbeatSecret)

	runner, unlock, err := registry.lockLiveRunner("runner-t")
	require.NoError(t, err)
	require.Equal(t, "tunnel://runner-t", runner.RunnerURL)
	require.True(t, runner.Tunnel)
	unlock()

	// Registered but not connected: out of discovery, no sessions.
	require.Empty(t, registry.Runners())
	_, _, err = registry.ReserveSession("runner-t")
	require.Error(t, err)
	_, ok := registry.TunnelDialer("runner-t")
	require.True(t, ok, "a tunneled runner has a dialer even while disconnected")

	tunnel, session := attachPipeTunnel(t, registry, "runner-t", resp.HeartbeatSecret)
	require.Len(t, registry.Runners(), 1)
	require.Equal(t, "http://localhost:1234/apps/runner-t/app", registry.Runners()[0].URL, "discovery shows the proxy URL, never the tunnel")
	endpoint, err := registry.RunnerEndpointForSession("runner-t", mustReserve(t, registry, "runner-t"))
	require.NoError(t, err)
	require.Equal(t, "tunnel://runner-t", endpoint)

	// A dial through the dialer reaches the runner side as a stream.
	go func() {
		stream, err := session.Accept()
		if err != nil {
			return
		}
		defer stream.Close()
		_, _ = io.Copy(stream, stream)
	}()
	dial, ok := registry.TunnelDialer("runner-t")
	require.True(t, ok)
	conn, err := dial(context.Background(), "tcp", "ignored")
	require.NoError(t, err)
	_, err = conn.Write([]byte("ping"))
	require.NoError(t, err)
	buf := make([]byte, 4)
	_, err = io.ReadFull(conn, buf)
	require.NoError(t, err)
	require.Equal(t, "ping", string(buf))
	require.NoError(t, conn.Close())
	require.Equal(t, "", tunnel.Reason())
}

func mustReserve(t *testing.T, registry *LiveRunnerRegistry, runnerID string) string {
	t.Helper()
	sessionID, _, err := registry.ReserveSession(runnerID)
	require.NoError(t, err)
	return sessionID
}

func TestTunnelRegistrationRejectsRunnerURLAndStaticEntriesAndFlips(t *testing.T) {
	registry := newLiveRunnerTestRegistry()
	_, err := registry.Heartbeat(LiveRunnerHeartbeatRequest{
		RunnerID: "runner-x", RunnerURL: "http://runner:8080", App: "test/app", Tunnel: true,
	}, liveRunnerTestBootstrapSecret)
	require.Error(t, err)
	var runnerErr *RunnerError
	require.ErrorAs(t, err, &runnerErr)
	require.Equal(t, http.StatusBadRequest, runnerErr.StatusCode)
	require.Equal(t, "runner_url and tunnel are exclusive", runnerErr.Message)

	_, err = ParseStaticLiveRunnerConfig([]byte(`{"runners":[{"app":"test/app","runner_url":"http://r:1","health_url":"/h","tunnel":true}]}`))
	require.ErrorContains(t, err, "tunnel is not supported for static runners")

	resp := tunnelHeartbeat(t, registry, "runner-t")
	_, err = registry.Heartbeat(LiveRunnerHeartbeatRequest{
		RunnerID: "runner-t", RunnerURL: "http://runner:8080", App: "test/app",
	}, resp.HeartbeatSecret)
	require.ErrorAs(t, err, &runnerErr)
	require.Equal(t, "tunnel cannot change after registration", runnerErr.Message)
}

func TestTunnelAttachAuthorization(t *testing.T) {
	registry := newLiveRunnerTestRegistry()
	resp := tunnelHeartbeat(t, registry, "runner-t")
	var runnerErr *RunnerError

	err := registry.AuthorizeTunnel("runner-t", "wrong")
	require.ErrorAs(t, err, &runnerErr)
	require.Equal(t, http.StatusUnauthorized, runnerErr.StatusCode)

	err = registry.AuthorizeTunnel("nobody", resp.HeartbeatSecret)
	require.ErrorAs(t, err, &runnerErr)
	require.Equal(t, http.StatusNotFound, runnerErr.StatusCode)

	plain, err := registry.Heartbeat(LiveRunnerHeartbeatRequest{
		RunnerID: "runner-plain", RunnerURL: "http://runner:8080", App: "test/app",
	}, liveRunnerTestBootstrapSecret)
	require.NoError(t, err)
	err = registry.AuthorizeTunnel("runner-plain", plain.HeartbeatSecret)
	require.ErrorAs(t, err, &runnerErr)
	require.Equal(t, http.StatusNotFound, runnerErr.StatusCode)
	_, ok := registry.TunnelDialer("runner-plain")
	require.False(t, ok)

	require.NoError(t, registry.AuthorizeTunnel("runner-t", resp.HeartbeatSecret))
	orchSide, _ := net.Pipe()
	_, err = registry.AttachTunnel("runner-t", "wrong", orchSide)
	require.ErrorAs(t, err, &runnerErr)
	require.Equal(t, http.StatusUnauthorized, runnerErr.StatusCode)
}

func TestTunnelReplaceAndRemoveCloseTheOldSession(t *testing.T) {
	registry := newLiveRunnerTestRegistry()
	resp := tunnelHeartbeat(t, registry, "runner-t")
	first, firstSession := attachPipeTunnel(t, registry, "runner-t", resp.HeartbeatSecret)
	require.Len(t, registry.Runners(), 1)

	second, _ := attachPipeTunnel(t, registry, "runner-t", resp.HeartbeatSecret)
	select {
	case <-first.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("replaced tunnel did not finish")
	}
	require.Equal(t, "replaced", first.Reason())
	<-firstSession.CloseChan()
	require.Len(t, registry.Runners(), 1, "the runner stays ready on the new tunnel")

	// A drop from the runner side takes the runner out of discovery until it reconnects.
	third, thirdSession := attachPipeTunnel(t, registry, "runner-t", resp.HeartbeatSecret)
	<-second.Done()
	require.NoError(t, thirdSession.Close())
	select {
	case <-third.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("dropped tunnel did not finish")
	}
	require.Equal(t, "closed", third.Reason())
	require.Empty(t, registry.Runners())
	dial, _ := registry.TunnelDialer("runner-t")
	_, err := dial(context.Background(), "tcp", "ignored")
	require.ErrorContains(t, err, "runner tunnel not connected")

	fourth, _ := attachPipeTunnel(t, registry, "runner-t", resp.HeartbeatSecret)
	require.Len(t, registry.Runners(), 1)
	require.NoError(t, registry.Unregister("runner-t", resp.HeartbeatSecret))
	select {
	case <-fourth.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("removed runner's tunnel did not finish")
	}
	require.Equal(t, "removed", fourth.Reason())
}

func TestTunnelHeartbeatRequestJSONShape(t *testing.T) {
	// The field name the runner-side client sends.
	var req LiveRunnerHeartbeatRequest
	require.NoError(t, json.Unmarshal([]byte(`{"app":"a","capacity":1,"price_info":{"price":"1"},"tunnel":true}`), &req))
	require.True(t, req.Tunnel)
}
