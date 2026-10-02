package server

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"math/big"
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
	"github.com/livepeer/go-livepeer/eth"
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

// newPoolLiveRunnerHTTP is an off-chain orchestrator in pool mode, served over HTTP.
func newPoolLiveRunnerHTTP(t *testing.T, webhook *poolWebhook) (*lphttp, *httptest.Server) {
	t.Helper()
	return newPoolLiveRunnerHTTPOn(t, webhook, false)
}

func newPoolLiveRunnerHTTPOn(t *testing.T, webhook *poolWebhook, onchain bool) (*lphttp, *httptest.Server) {
	t.Helper()
	node, err := core.NewLivepeerNode(nil, t.TempDir(), nil)
	require.NoError(t, err)
	node.LiveRunnerPoolSecret = poolTestSecret
	orch := newStubOrchestrator()
	orch.secret = "live-runner-secret"
	if onchain {
		prevWatcher := core.PriceFeedWatcher
		core.PriceFeedWatcher = stubPriceFeedWatcher{price: eth.PriceData{Price: big.NewRat(2000, 1)}}
		t.Cleanup(func() { core.PriceFeedWatcher = prevWatcher })
		node.LivePaymentInterval = 5 * time.Second
		orch.ticketParams = defaultTicketParams()
	}
	lp := &lphttp{
		orchestrator: orch,
		node:         node,
		transRPC:     http.NewServeMux(),
	}
	admitter, err := runner.NewLiveRunnerWebhookAdmitter(webhook.srv.URL, poolTestSecret, 2*time.Second)
	require.NoError(t, err)
	manager := runner.NewLiveRunnerRegistry(runner.LiveRunnerRegistryConfig{
		Host:        testLiveRunnerHost{orchestrator: lp.orchestrator},
		Onchain:     onchain,
		Admitter:    admitter,
		OrchAddress: "0x00000000000000000000000000000000000000AA",
		Prices: runner.LiveRunnerPriceTable{Apps: map[string]runner.LiveRunnerPriceInfo{
			"vllm/qwen2.5-7b-instruct": {Price: "10", Currency: "usd", Unit: "hour"},
			"echo":                     {Price: "0.001", Currency: "usd", Unit: "fixed"},
		}},
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
	return startPoolRunnerFor(t, orchURL, credential, "single-shot", 2, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "app")
	}))
}

func startPoolRunnerFor(t *testing.T, orchURL, credential, mode string, capacity int, appHandler http.Handler) *tunnelclient.Client {
	t.Helper()
	return startPoolRunnerLoggingTo(t, io.Discard, orchURL, credential, mode, capacity, appHandler)
}

// startPoolRunnerLoggingTo runs the command's client the way a pool member
// does: with a credential and no price of its own.
func startPoolRunnerLoggingTo(t *testing.T, log io.Writer, orchURL, credential, mode string, capacity int, appHandler http.Handler) *tunnelclient.Client {
	t.Helper()
	app := httptest.NewServer(appHandler)
	t.Cleanup(app.Close)
	client, err := tunnelclient.New(tunnelclient.Config{
		Orchestrator: orchURL,
		Credential:   credential,
		Target:       app.URL,
		App:          "vllm/qwen2.5-7b-instruct",
		Mode:         mode,
		Capacity:     capacity,
		Logger:       slog.New(slog.NewTextHandler(log, nil)),
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

// waitPoolRunnerConnected waits until the runner is registered and its
// tunnel carries streams, whatever state the runner is in.
func waitPoolRunnerConnected(t *testing.T, lp *lphttp, client *tunnelclient.Client) string {
	t.Helper()
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
	return client.RunnerID()
}

// probeRequest is a request carrying the pool secret the way the pool service sends it.
func probeRequest(t *testing.T, method, url, secret string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(method, url, nil)
	require.NoError(t, err)
	req.Header.Set("Livepeer-Probe-Secret", secret)
	return req
}

func doRequest(t *testing.T, req *http.Request) (int, string) {
	t.Helper()
	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	return res.StatusCode, strings.TrimSpace(string(body))
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

	// Registered with its tunnel up is all it would take to be listed were it active.
	waitPoolRunnerConnected(t, lp, client)

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

	status, body = postHeartbeat(t, orch.URL, "c1", `{"app":"flux/schnell","tunnel":true}`)
	require.Equal(t, http.StatusForbidden, status, "the webhook is asked before the app is checked")

	webhook.srv.Close()
	status, body = postHeartbeat(t, orch.URL, "c1", registration)
	require.Equal(t, http.StatusServiceUnavailable, status)
	require.Equal(t, "admission unavailable", body)
}

// headerRecorder is an app that remembers the headers of the last request.
type headerRecorder struct {
	mu     sync.Mutex
	header http.Header
}

func (h *headerRecorder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	h.header = r.Header.Clone()
	h.mu.Unlock()
	_, _ = io.WriteString(w, "app")
}

func (h *headerRecorder) last() http.Header {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.header.Clone()
}

func TestPoolProbeTestsARunnerOnProbationWithoutPayment(t *testing.T) {
	webhook := newPoolWebhook(t, runner.LiveRunnerAdmissionResponse{Allow: true, MemberID: "m1"})
	// On-chain, so that a customer would be asked to pay.
	lp, orch := newPoolLiveRunnerHTTPOn(t, webhook, true)
	app := &headerRecorder{}
	client := startPoolRunnerFor(t, orch.URL, "member-credential", "single-shot", 2, app)
	runnerID := waitPoolRunnerConnected(t, lp, client)
	appURL := orch.URL + "/apps/" + runnerID + "/app/v1/models"

	// A customer cannot see the runner at all.
	status, _ := doRequest(t, probeRequestWithout(t, appURL))
	require.Equal(t, http.StatusNotFound, status)

	// The pool service reaches it, with no payment challenge.
	req := probeRequest(t, http.MethodGet, appURL, poolTestSecret)
	req.Header.Set("Authorization", "Bearer sk-app-key")
	status, body := doRequest(t, req)
	require.Equal(t, http.StatusOK, status, body)
	require.Equal(t, "app", body)

	// The runner sees an ordinary session, never the pool secret; what the
	// customer sends the app still arrives.
	seen := app.last()
	require.Empty(t, seen.Values("Livepeer-Probe-Secret"))
	require.Equal(t, "Bearer sk-app-key", seen.Get("Authorization"))
	require.Equal(t, runnerID, seen.Get("Livepeer-Runner-Route"))
	require.NotEmpty(t, seen.Get("Livepeer-Session-Token"))
	require.NotContains(t, seen.Get("Livepeer-Session-Id"), "session_", "on-chain a probe's session id looks like a paid one")

	// The session is released when the request ends.
	manager, _ := lp.liveRunnerManager()
	_, err := manager.RunnerEndpointForSession(runnerID, seen.Get("Livepeer-Session-Id"))
	require.Error(t, err)
}

func probeRequestWithout(t *testing.T, url string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	require.NoError(t, err)
	return req
}

func TestPoolProbeNeedsThePoolSecret(t *testing.T) {
	webhook := newPoolWebhook(t, runner.LiveRunnerAdmissionResponse{Allow: true, MemberID: "m1", State: runner.LiveRunnerStateActive})
	lp, orch := newPoolLiveRunnerHTTP(t, webhook)
	client := startPoolRunner(t, orch.URL, "member-credential")
	runnerID := waitPoolRunnerConnected(t, lp, client)
	appURL := orch.URL + "/apps/" + runnerID + "/app/v1/models"
	sessionURL := orch.URL + "/apps/" + runnerID + "/session"

	// A wrong secret is refused even though the runner is active and off-chain,
	// where the same request without the header would be served.
	for _, secret := range []string{"wrong", "", "live-runner-secret"} {
		status, body := doRequest(t, probeRequest(t, http.MethodGet, appURL, secret))
		require.Equal(t, http.StatusUnauthorized, status, secret)
		require.Equal(t, "invalid probe authorization", body)
		status, _ = doRequest(t, probeRequest(t, http.MethodPost, sessionURL, secret))
		require.Equal(t, http.StatusUnauthorized, status, secret)
	}
	status, _ := doRequest(t, probeRequestWithout(t, appURL))
	require.Equal(t, http.StatusOK, status)

	// An orchestrator with no pool secret accepts no probe at all.
	lp.node.LiveRunnerPoolSecret = ""
	status, _ = doRequest(t, probeRequest(t, http.MethodGet, appURL, poolTestSecret))
	require.Equal(t, http.StatusUnauthorized, status)
	status, _ = doRequest(t, probeRequest(t, http.MethodGet, appURL, ""))
	require.Equal(t, http.StatusUnauthorized, status)
}

func TestPoolProbeCountsAgainstCapacityAndReachesEveryState(t *testing.T) {
	webhook := newPoolWebhook(t, runner.LiveRunnerAdmissionResponse{Allow: true, MemberID: "m1", State: runner.LiveRunnerStateSuspended})
	lp, orch := newPoolLiveRunnerHTTP(t, webhook)
	entered := make(chan struct{}, 4)
	release := make(chan struct{})
	client := startPoolRunnerFor(t, orch.URL, "member-credential", "single-shot", 1, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		<-release
		_, _ = io.WriteString(w, "app")
	}))
	runnerID := waitPoolRunnerConnected(t, lp, client)
	appURL := orch.URL + "/apps/" + runnerID + "/app/slow"

	// Suspended: customers are refused, the probe gets through and holds the one slot.
	status, body := doRequest(t, probeRequestWithout(t, appURL))
	require.Equal(t, http.StatusConflict, status)
	require.Equal(t, "runner suspended", body)

	first := make(chan int, 1)
	go func() {
		status, _ := doRequest(t, probeRequest(t, http.MethodGet, appURL, poolTestSecret))
		first <- status
	}()
	<-entered
	status, body = doRequest(t, probeRequest(t, http.MethodGet, appURL, poolTestSecret))
	require.Equal(t, http.StatusServiceUnavailable, status)
	require.Equal(t, "no capacity available for runner", body)
	close(release)
	require.Equal(t, http.StatusOK, <-first)
}

func TestPoolProbeOpensAPersistentSession(t *testing.T) {
	webhook := newPoolWebhook(t, runner.LiveRunnerAdmissionResponse{Allow: true, MemberID: "m1"})
	lp, orch := newPoolLiveRunnerHTTP(t, webhook)
	app := &headerRecorder{}
	client := startPoolRunnerFor(t, orch.URL, "member-credential", "persistent", 1, app)
	runnerID := waitPoolRunnerConnected(t, lp, client)
	sessionURL := orch.URL + "/apps/" + runnerID + "/session"

	res, err := http.Post(sessionURL, "application/json", nil)
	require.NoError(t, err)
	res.Body.Close()
	require.Equal(t, http.StatusNotFound, res.StatusCode, "a customer cannot open a session on a runner on probation")

	status, body := doRequest(t, probeRequest(t, http.MethodPost, sessionURL, poolTestSecret))
	require.Equal(t, http.StatusOK, status, body)
	var session liveRunnerSessionResponse
	require.NoError(t, json.Unmarshal([]byte(body), &session))
	require.NotEmpty(t, session.SessionID)

	// The session is used and stopped like any other, by its id.
	base := orch.URL + "/apps/" + runnerID + "/session/" + session.SessionID
	status, body = doRequest(t, probeRequestWithout(t, base+"/app/echo"))
	require.Equal(t, http.StatusOK, status, body)
	require.Equal(t, session.SessionID, app.last().Get("Livepeer-Session-Id"))

	// It holds the runner's only slot until it is stopped.
	status, _ = doRequest(t, probeRequest(t, http.MethodPost, sessionURL, poolTestSecret))
	require.Equal(t, http.StatusServiceUnavailable, status)
	stop, err := http.NewRequest(http.MethodPost, base+"/stop", nil)
	require.NoError(t, err)
	status, _ = doRequest(t, stop)
	require.Equal(t, http.StatusNoContent, status)
	status, _ = doRequest(t, probeRequest(t, http.MethodPost, sessionURL, poolTestSecret))
	require.Equal(t, http.StatusOK, status)
}

func TestPoolRunnerNeverSeesPaymentHeaders(t *testing.T) {
	webhook := newPoolWebhook(t, runner.LiveRunnerAdmissionResponse{Allow: true, MemberID: "m1", State: runner.LiveRunnerStateActive})
	lp, orch := newPoolLiveRunnerHTTP(t, webhook)
	app := &headerRecorder{}
	client := startPoolRunnerFor(t, orch.URL, "member-credential", "single-shot", 2, app)
	runnerID := waitPoolRunnerConnected(t, lp, client)

	req := probeRequestWithout(t, orch.URL+"/apps/"+runnerID+"/app/v1/models")
	req.Header.Set("Livepeer-Payment", "payment")
	req.Header.Set("Livepeer-Segment", "segment")
	req.Header.Set("Livepeer-Payer-Address", "0x00000000000000000000000000000000000000c1")
	req.Header.Set("X-Request-Id", "abc")
	status, body := doRequest(t, req)
	require.Equal(t, http.StatusOK, status, body)

	seen := app.last()
	require.Empty(t, seen.Values("Livepeer-Payment"))
	require.Empty(t, seen.Values("Livepeer-Segment"))
	require.Empty(t, seen.Values("Livepeer-Payer-Address"))
	require.Equal(t, "abc", seen.Get("X-Request-Id"))
}

func TestWithoutPoolModeRunnersStillSeeEveryHeader(t *testing.T) {
	app := &headerRecorder{}
	f := newTunnelFixture(t, app, 2)
	runnerID := f.waitListed(t, "")

	req := probeRequestWithout(t, f.appURL(runnerID, "/v1/models"))
	req.Header.Set("Livepeer-Payment", "payment")
	req.Header.Set("Livepeer-Segment", "segment")
	status, body := doRequest(t, req)
	require.Equal(t, http.StatusOK, status, body)
	require.Equal(t, "payment", app.last().Get("Livepeer-Payment"))
	require.Equal(t, "segment", app.last().Get("Livepeer-Segment"))

	// Outside pool mode nobody holds a pool secret, so a probe is refused.
	status, _ = doRequest(t, probeRequest(t, http.MethodGet, f.appURL(runnerID, "/v1/models"), "anything"))
	require.Equal(t, http.StatusUnauthorized, status)
}

func TestPoolProbeSkipsThePaymentChallenge(t *testing.T) {
	webhook := newPoolWebhook(t, runner.LiveRunnerAdmissionResponse{Allow: true, MemberID: "m1", State: runner.LiveRunnerStateActive})
	lp, orch := newPoolLiveRunnerHTTPOn(t, webhook, true)
	client := startPoolRunner(t, orch.URL, "member-credential")
	runnerID := waitPoolRunnerConnected(t, lp, client)
	appURL := orch.URL + "/apps/" + runnerID + "/app/v1/models"

	// The same active, priced runner: a customer is asked to pay, the probe is not.
	status, _ := doRequest(t, probeRequestWithout(t, appURL))
	require.Equal(t, http.StatusPaymentRequired, status)
	status, body := doRequest(t, probeRequest(t, http.MethodGet, appURL, poolTestSecret))
	require.Equal(t, http.StatusOK, status, body)
	require.Equal(t, "app", body)
}

// syncBuffer is a log destination that can be read while it is written to.
type syncBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestRunnerCommandJoinsAPoolWithACredentialAndNoPrice(t *testing.T) {
	webhook := newPoolWebhook(t, runner.LiveRunnerAdmissionResponse{Allow: true, MemberID: "m1", State: runner.LiveRunnerStateActive})
	// On-chain, where a runner outside a pool has to name its price.
	lp, orch := newPoolLiveRunnerHTTPOn(t, webhook, true)
	log := &syncBuffer{}
	client := startPoolRunnerLoggingTo(t, log, orch.URL, "member-credential", "single-shot", 2, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "app")
	}))
	runnerID := waitPoolRunnerConnected(t, lp, client)

	// It is told what the operator pays for the app, and says so in its log.
	require.Contains(t, log.String(), "the orchestrator sets this app's price")
	require.Contains(t, log.String(), "price=10 currency=usd unit=hour")
	manager := lp.node.LiveRunnerManager.(*runner.LiveRunnerRegistry)
	runners := manager.PoolRunners()
	require.Len(t, runners, 1)
	require.Equal(t, runnerID, runners[0].RunnerID)
	require.Equal(t, json.Number("10"), runners[0].PriceInfo.Price)

	// Its later heartbeats are accepted: nothing it reports has changed.
	require.Never(t, func() bool { return client.RunnerID() != runnerID }, 2*time.Second, 100*time.Millisecond)
}

func TestRunnerCommandSaysWhyItWasRefusedAndKeepsTrying(t *testing.T) {
	webhook := newPoolWebhook(t, runner.LiveRunnerAdmissionResponse{Allow: false, Reason: "credential revoked"})
	lp, orch := newPoolLiveRunnerHTTP(t, webhook)
	log := &syncBuffer{}
	client := startPoolRunnerLoggingTo(t, log, orch.URL, "member-credential", "single-shot", 1, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

	require.Eventually(t, func() bool {
		calls, _ := webhook.seen()
		return len(calls) >= 2
	}, 15*time.Second, 50*time.Millisecond, "a refused runner retries")
	require.Contains(t, log.String(), "orchestrator refused the registration")
	require.Contains(t, log.String(), "403 credential revoked")
	require.Empty(t, client.RunnerID())

	// Once the pool service lets it in, it joins without being restarted.
	webhook.mu.Lock()
	webhook.answer = runner.LiveRunnerAdmissionResponse{Allow: true, MemberID: "m1"}
	webhook.mu.Unlock()
	waitPoolRunnerConnected(t, lp, client)
}
