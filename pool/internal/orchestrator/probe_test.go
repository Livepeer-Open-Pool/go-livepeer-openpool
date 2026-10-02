package orchestrator_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Livepeer-Open-Pool/pool/internal/core"
	"github.com/Livepeer-Open-Pool/pool/internal/orchestrator"
)

const probeSecret = "the-pool-secret"

type probeRequest struct {
	method, path, query, secret, body, custom string
}

// fakeOrchestrator is the orchestrator's public port as a probe sees it:
// app requests go to handle, and sessions can be opened and stopped.
type fakeOrchestrator struct {
	mu       sync.Mutex
	requests []probeRequest
	handle   http.HandlerFunc
	// sessionStatus is what opening a session answers; 0 means 200.
	sessionStatus int
}

func newFakeOrchestrator(t *testing.T, handle http.HandlerFunc) (*fakeOrchestrator, *orchestrator.Prober) {
	t.Helper()
	fake := &fakeOrchestrator{handle: handle}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		fake.mu.Lock()
		fake.requests = append(fake.requests, probeRequest{r.Method, r.URL.Path, r.URL.RawQuery, r.Header.Get("Livepeer-Probe-Secret"), string(body), r.Header.Get("X-Test")})
		sessionStatus := fake.sessionStatus
		fake.mu.Unlock()
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/session"):
			if sessionStatus != 0 {
				w.WriteHeader(sessionStatus)
				_, _ = io.WriteString(w, "runner not found")
				return
			}
			_, _ = io.WriteString(w, `{"session_id":"sess123","app_url":"https://orch.example/apps/runner_a/session/sess123/app","control_url":"https://orch.example/apps/runner_a/session/sess123"}`)
		case strings.HasSuffix(r.URL.Path, "/stop"):
			w.WriteHeader(http.StatusNoContent)
		default:
			fake.handle(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	prober, err := orchestrator.NewProber(srv.URL, probeSecret, false)
	require.NoError(t, err)
	return fake, prober
}

func (f *fakeOrchestrator) seen() []probeRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]probeRequest(nil), f.requests...)
}

var chatTest = core.Test{
	Method: "POST", Path: "/v1/chat/completions?stream=false", Headers: map[string]string{"X-Test": "yes", "Content-Type": "application/json"},
	Body: `{"model":"m","messages":[]}`, ExpectStatus: 200, ExpectContains: []string{"choices", "content"}, TimeoutMs: 2000,
}

func TestProbeSendsTheTestThroughTheProbeRoute(t *testing.T) {
	fake, prober := newFakeOrchestrator(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"choices":[{"message":{"content":"hello"}}]}`)
	})
	outcome := prober.Probe(context.Background(), core.OrchRunner{RunnerID: "runner_a", Mode: core.ModeSingleShot}, chatTest)
	require.True(t, outcome.OK, outcome.Detail)
	require.Empty(t, outcome.Detail)
	require.GreaterOrEqual(t, outcome.LatencyMs, int64(0))

	// One request, to the runner's own route, carrying the pool secret that
	// marks it as an unpaid probe.
	require.Equal(t, []probeRequest{{
		method: "POST", path: "/apps/runner_a/app/v1/chat/completions", query: "stream=false",
		secret: probeSecret, body: `{"model":"m","messages":[]}`, custom: "yes",
	}}, fake.seen())
}

func TestProbeSaysWhatWasWrongWithABadAnswer(t *testing.T) {
	runner := core.OrchRunner{RunnerID: "runner_a", Mode: core.ModeSingleShot}
	cases := map[string]struct {
		handle http.HandlerFunc
		detail string
	}{
		"wrong status": {func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(w, "CUDA out of memory")
		}, "answered 500, expected 200: CUDA out of memory"},
		"missing content": {func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.WriteString(w, `{"choices":[]}`)
		}, `answer does not contain "content": {"choices":[]}`},
		"payment asked": {func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusPaymentRequired)
		}, "answered 402, expected 200: "},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, prober := newFakeOrchestrator(t, tc.handle)
			outcome := prober.Probe(context.Background(), runner, chatTest)
			require.False(t, outcome.OK)
			require.Equal(t, tc.detail, outcome.Detail)
		})
	}
}

func TestProbeEnforcesTheTimeLimit(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	_, prober := newFakeOrchestrator(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})
	slow := chatTest
	slow.TimeoutMs = 100
	started := time.Now()
	outcome := prober.Probe(context.Background(), core.OrchRunner{RunnerID: "runner_a", Mode: core.ModeSingleShot}, slow)
	require.False(t, outcome.OK)
	require.Equal(t, "no answer within 100ms", outcome.Detail)
	require.Less(t, time.Since(started), 2*time.Second)
	require.GreaterOrEqual(t, outcome.LatencyMs, int64(100))
}

func TestProbeOfAPersistentRunnerOpensAndStopsASession(t *testing.T) {
	fake, prober := newFakeOrchestrator(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"choices":[{"message":{"content":"hello"}}]}`)
	})
	runner := core.OrchRunner{RunnerID: "runner_a", Mode: core.ModePersistent}
	outcome := prober.Probe(context.Background(), runner, chatTest)
	require.True(t, outcome.OK, outcome.Detail)

	seen := fake.seen()
	require.Len(t, seen, 3)
	require.Equal(t, probeRequest{method: "POST", path: "/apps/runner_a/session", secret: probeSecret}, seen[0])
	require.Equal(t, "/apps/runner_a/session/sess123/app/v1/chat/completions", seen[1].path)
	require.Empty(t, seen[1].secret, "inside the session the request is an ordinary one")
	require.Equal(t, probeRequest{method: "POST", path: "/apps/runner_a/session/sess123/stop"}, seen[2])

	// The session is stopped when the test fails too.
	fake.handle = func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusBadGateway) }
	outcome = prober.Probe(context.Background(), runner, chatTest)
	require.False(t, outcome.OK)
	require.Equal(t, "/apps/runner_a/session/sess123/stop", fake.seen()[5].path)

	// A runner that cannot be reached at all.
	fake.mu.Lock()
	fake.sessionStatus = http.StatusNotFound
	fake.mu.Unlock()
	outcome = prober.Probe(context.Background(), runner, chatTest)
	require.False(t, outcome.OK)
	require.Equal(t, "could not open a session: orchestrator answered 404: runner not found", outcome.Detail)
}

func TestProbeReportsAnOrchestratorThatIsDown(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	prober, err := orchestrator.NewProber(srv.URL, probeSecret, false)
	require.NoError(t, err)
	srv.Close()
	outcome := prober.Probe(context.Background(), core.OrchRunner{RunnerID: "runner_a", Mode: core.ModeSingleShot}, chatTest)
	require.False(t, outcome.OK)
	require.Contains(t, outcome.Detail, "request failed")
}

func TestNewProberNeedsAURLAndTheSecret(t *testing.T) {
	_, err := orchestrator.NewProber("orch.example:8935", probeSecret, false)
	require.Error(t, err)
	_, err = orchestrator.NewProber("https://orch.example:8935", "", false)
	require.Error(t, err)
}
