package server

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/livepeer/go-livepeer/ai/runner"
	"github.com/livepeer/go-livepeer/ai/runner/tunnelclient"
	"github.com/livepeer/go-livepeer/core"
)

const poolTestSecret = "pool-secret"

// poolWebhook is a stand-in for the pool service's admission endpoint.
type poolWebhook struct {
	srv *httptest.Server

	mu     sync.Mutex
	calls  []runner.LiveRunnerAdmissionRequest
	auths  []string
	answer runner.LiveRunnerAdmissionResponse
}

func newPoolWebhook(t *testing.T, answer runner.LiveRunnerAdmissionResponse) *poolWebhook {
	t.Helper()
	w := &poolWebhook{answer: answer}
	w.srv = httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		var req runner.LiveRunnerAdmissionRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		w.mu.Lock()
		w.calls = append(w.calls, req)
		w.auths = append(w.auths, r.Header.Get("Authorization"))
		answer := w.answer
		w.mu.Unlock()
		_ = json.NewEncoder(rw).Encode(answer)
	}))
	t.Cleanup(w.srv.Close)
	return w
}

func (w *poolWebhook) seen() ([]runner.LiveRunnerAdmissionRequest, []string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]runner.LiveRunnerAdmissionRequest(nil), w.calls...), append([]string(nil), w.auths...)
}

// newPoolLiveRunnerHTTP is an orchestrator in pool mode, served over HTTP.
func newPoolLiveRunnerHTTP(t *testing.T, webhook *poolWebhook) (*lphttp, *httptest.Server) {
	t.Helper()
	node, err := core.NewLivepeerNode(nil, t.TempDir(), nil)
	require.NoError(t, err)
	orch := newStubOrchestrator()
	orch.secret = "live-runner-secret"
	lp := &lphttp{
		orchestrator: orch,
		node:         node,
		transRPC:     http.NewServeMux(),
	}
	admitter, err := runner.NewLiveRunnerWebhookAdmitter(webhook.srv.URL, poolTestSecret, 2*time.Second)
	require.NoError(t, err)
	manager := runner.NewLiveRunnerRegistry(runner.LiveRunnerRegistryConfig{
		Host:        testLiveRunnerHost{orchestrator: lp.orchestrator},
		Admitter:    admitter,
		OrchAddress: "0x00000000000000000000000000000000000000AA",
	})
	t.Cleanup(manager.Stop)
	node.LiveRunnerManager = manager
	require.NoError(t, startAIServer(lp))
	srv := httptest.NewServer(lp)
	t.Cleanup(srv.Close)
	return lp, srv
}

// startPoolRunner runs the runner command's client against the orchestrator,
// in front of an app that answers every request with "app".
func startPoolRunner(t *testing.T, orchURL, credential string) *tunnelclient.Client {
	t.Helper()
	app := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "app")
	}))
	t.Cleanup(app.Close)
	client, err := tunnelclient.New(tunnelclient.Config{
		Orchestrator: orchURL,
		OrchSecret:   credential,
		Target:       app.URL,
		App:          "vllm/qwen2.5-7b-instruct",
		Mode:         "single-shot",
		Capacity:     2,
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = client.Run(ctx); close(done) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
		}
	})
	return client
}

func discoveredRunnerURLs(t *testing.T, orchURL string) []string {
	t.Helper()
	res, err := http.Get(orchURL + "/discovery")
	require.NoError(t, err)
	defer res.Body.Close()
	var entries []liveRunnerDiscoveryEntry
	require.NoError(t, json.NewDecoder(res.Body).Decode(&entries))
	urls := []string{}
	for _, entry := range entries {
		for _, r := range entry.Runners {
			urls = append(urls, r.URL)
		}
	}
	return urls
}

func postHeartbeat(t *testing.T, orchURL, auth, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, orchURL+"/runners/heartbeat", strings.NewReader(body))
	require.NoError(t, err)
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer res.Body.Close()
	out, _ := io.ReadAll(res.Body)
	return res.StatusCode, strings.TrimSpace(string(out))
}

func TestPoolMemberRunnerJoinsThroughTheWebhookAndTakesWork(t *testing.T) {
	webhook := newPoolWebhook(t, runner.LiveRunnerAdmissionResponse{
		Allow: true, MemberID: "m1", MaxRunners: 2, State: runner.LiveRunnerStateActive,
	})
	_, orch := newPoolLiveRunnerHTTP(t, webhook)
	client := startPoolRunner(t, orch.URL, "member-credential")

	require.Eventually(t, func() bool { return len(discoveredRunnerURLs(t, orch.URL)) == 1 }, 15*time.Second, 50*time.Millisecond)
	runnerID := client.RunnerID()
	require.Regexp(t, `^runner_[a-z2-7]{8}$`, runnerID)

	calls, auths := webhook.seen()
	require.Len(t, calls, 1)
	require.Equal(t, "Bearer "+poolTestSecret, auths[0], "the orchestrator identifies itself to the pool service")
	require.Equal(t, "register", calls[0].Event)
	require.Equal(t, "member-credential", calls[0].Credential)
	require.Equal(t, "127.0.0.1", calls[0].RemoteAddr)
	require.Equal(t, "0x00000000000000000000000000000000000000AA", calls[0].OrchAddress)
	require.Equal(t, runnerID, calls[0].Runner.RunnerID)
	require.Equal(t, "vllm/qwen2.5-7b-instruct", calls[0].Runner.App)
	require.True(t, calls[0].Runner.Tunnel)

	res, err := http.Get(orch.URL + "/apps/" + runnerID + "/app/v1/models")
	require.NoError(t, err)
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	require.Equal(t, http.StatusOK, res.StatusCode)
	require.Equal(t, "app", string(body))
}

func TestPoolRunnerOnProbationIsInvisibleToCustomers(t *testing.T) {
	webhook := newPoolWebhook(t, runner.LiveRunnerAdmissionResponse{Allow: true, MemberID: "m1"})
	lp, orch := newPoolLiveRunnerHTTP(t, webhook)
	client := startPoolRunner(t, orch.URL, "member-credential")

	// Wait until the runner is registered with its tunnel up, which is all that
	// would be needed for it to be listed were it active.
	manager, ok := lp.liveRunnerManager()
	require.True(t, ok)
	require.Eventually(t, func() bool {
		id := client.RunnerID()
		if id == "" {
			return false
		}
		dial, tunneled := manager.TunnelDialer(id)
		if !tunneled {
			return false
		}
		conn, err := dial(context.Background(), "", "")
		if err != nil {
			return false
		}
		_ = conn.Close()
		return true
	}, 15*time.Second, 50*time.Millisecond)

	require.Empty(t, discoveredRunnerURLs(t, orch.URL))
	res, err := http.Get(orch.URL + "/apps/" + client.RunnerID() + "/app/v1/models")
	require.NoError(t, err)
	res.Body.Close()
	require.Equal(t, http.StatusNotFound, res.StatusCode)
	res, err = http.Post(orch.URL+"/apps/"+client.RunnerID()+"/session", "application/json", nil)
	require.NoError(t, err)
	res.Body.Close()
	require.Equal(t, http.StatusNotFound, res.StatusCode)
}

func TestPoolRegistrationStatusCodes(t *testing.T) {
	webhook := newPoolWebhook(t, runner.LiveRunnerAdmissionResponse{Allow: false, Reason: "unknown credential"})
	_, orch := newPoolLiveRunnerHTTP(t, webhook)
	registration := `{"app":"echo","mode":"single-shot","capacity":1,"tunnel":true}`

	status, body := postHeartbeat(t, orch.URL, "bad-credential", registration)
	require.Equal(t, http.StatusForbidden, status)
	require.Equal(t, "unknown credential", body)

	status, body = postHeartbeat(t, orch.URL, "", registration)
	require.Equal(t, http.StatusUnauthorized, status)
	require.Equal(t, "registration requires a credential", body)

	// The orchestrator's own registration secret registers nothing in pool mode.
	status, _ = postHeartbeat(t, orch.URL, "live-runner-secret", registration)
	require.Equal(t, http.StatusForbidden, status)

	status, body = postHeartbeat(t, orch.URL, "c1", `{"runner_id":"echo","app":"echo","tunnel":true}`)
	require.Equal(t, http.StatusNotFound, status)
	require.Equal(t, "runner not found", body)

	status, body = postHeartbeat(t, orch.URL, "c1", `{"app":"echo","runner_url":"http://127.0.0.1:7935"}`)
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "pool runners must register with a tunnel", body)

	webhook.srv.Close()
	status, body = postHeartbeat(t, orch.URL, "c1", registration)
	require.Equal(t, http.StatusServiceUnavailable, status)
	require.Equal(t, "admission unavailable", body)
}
