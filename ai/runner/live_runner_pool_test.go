package runner

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeAdmitter records the admission requests it sees and answers them with
// answer, or admits everything as member m1 on probation when answer is nil.
type fakeAdmitter struct {
	mu     sync.Mutex
	calls  []LiveRunnerAdmissionRequest
	answer func(LiveRunnerAdmissionRequest) (*LiveRunnerAdmissionResponse, error)
}

func (a *fakeAdmitter) Admit(req LiveRunnerAdmissionRequest) (*LiveRunnerAdmissionResponse, error) {
	a.mu.Lock()
	a.calls = append(a.calls, req)
	answer := a.answer
	a.mu.Unlock()
	if answer == nil {
		return &LiveRunnerAdmissionResponse{Allow: true, MemberID: "m1", MaxRunners: 10}, nil
	}
	return answer(req)
}

func (a *fakeAdmitter) seen() []LiveRunnerAdmissionRequest {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]LiveRunnerAdmissionRequest(nil), a.calls...)
}

func admitAs(memberID, state string, maxRunners int) func(LiveRunnerAdmissionRequest) (*LiveRunnerAdmissionResponse, error) {
	return func(LiveRunnerAdmissionRequest) (*LiveRunnerAdmissionResponse, error) {
		return &LiveRunnerAdmissionResponse{Allow: true, MemberID: memberID, State: state, MaxRunners: maxRunners}, nil
	}
}

func newPoolTestRegistry(t *testing.T, admitter LiveRunnerAdmitter) *LiveRunnerRegistry {
	t.Helper()
	registry := newLiveRunnerTestRegistryWithConfig(LiveRunnerRegistryConfig{
		Host:        liveRunnerTestHost{},
		Admitter:    admitter,
		OrchAddress: "0x00000000000000000000000000000000000000AA",
	})
	t.Cleanup(registry.Stop)
	return registry
}

func poolTestHeartbeat() LiveRunnerHeartbeatRequest {
	return LiveRunnerHeartbeatRequest{
		App:      "vllm/qwen2.5-7b-instruct",
		Mode:     LiveRunnerModeSingleShot,
		Version:  "vllm-app:1.4.2",
		Status:   "ready",
		Capacity: 2,
		Tunnel:   true,
	}
}

// poolTestRegister registers a pool runner and connects its tunnel, so that
// only its state decides whether it takes work.
func poolTestRegister(t *testing.T, registry *LiveRunnerRegistry, credential string) *LiveRunnerHeartbeatResponse {
	t.Helper()
	resp, err := registry.HeartbeatFrom(poolTestHeartbeat(), credential, "203.0.113.7")
	require.NoError(t, err)
	require.NotEmpty(t, resp.HeartbeatSecret)
	attachPipeTunnel(t, registry, resp.RunnerID, resp.HeartbeatSecret)
	return resp
}

func requireRunnerError(t *testing.T, err error, status int, message string) {
	t.Helper()
	var runnerErr *RunnerError
	require.ErrorAs(t, err, &runnerErr)
	require.Equal(t, status, runnerErr.StatusCode, runnerErr.Message)
	if message != "" {
		require.Equal(t, message, runnerErr.Message)
	}
}

func setPoolTestState(t *testing.T, registry *LiveRunnerRegistry, runnerID, state string) {
	t.Helper()
	runner, unlock, err := registry.lockLiveRunner(runnerID)
	require.NoError(t, err)
	runner.state = state
	unlock()
}

func TestPoolRegistrationAsksTheAdmitterAndStartsOnProbation(t *testing.T) {
	admitter := &fakeAdmitter{answer: func(LiveRunnerAdmissionRequest) (*LiveRunnerAdmissionResponse, error) {
		return &LiveRunnerAdmissionResponse{
			Allow: true, MemberID: "m1", PayoutAddress: "0x00000000000000000000000000000000000000b1",
			MaxRunners: 2, State: LiveRunnerStateProbation,
		}, nil
	}}
	registry := newPoolTestRegistry(t, admitter)

	resp := poolTestRegister(t, registry, "c1")
	require.Regexp(t, liveRunnerTestGeneratedRunnerIDPattern, resp.RunnerID)

	calls := admitter.seen()
	require.Len(t, calls, 1)
	require.Equal(t, LiveRunnerAdmissionRequest{
		Event:       "register",
		OrchAddress: "0x00000000000000000000000000000000000000AA",
		RemoteAddr:  "203.0.113.7",
		Credential:  "c1",
		Runner: LiveRunnerAdmissionRunner{
			RunnerID: resp.RunnerID,
			Tunnel:   true,
			Version:  "vllm-app:1.4.2",
			Status:   "ready",
			Mode:     LiveRunnerModeSingleShot,
			App:      "vllm/qwen2.5-7b-instruct",
			Capacity: 2,
		},
	}, calls[0])

	runner, unlock, err := registry.lockLiveRunner(resp.RunnerID)
	require.NoError(t, err)
	require.Equal(t, "m1", runner.member.id)
	require.Equal(t, "0x00000000000000000000000000000000000000b1", runner.member.payoutAddress)
	require.Equal(t, LiveRunnerStateProbation, runner.state)
	require.Equal(t, "203.0.113.7", runner.remoteAddr)
	unlock()

	// On probation the runner is connected and ready, and customers cannot see or use it.
	require.Empty(t, registry.Runners())
	_, _, err = registry.ReserveSession(resp.RunnerID)
	requireRunnerError(t, err, http.StatusNotFound, "runner not found")
	_, err = registry.PaymentInfo(resp.RunnerID)
	requireRunnerError(t, err, http.StatusNotFound, "runner not found")
	_, err = registry.RunnerMode(resp.RunnerID)
	requireRunnerError(t, err, http.StatusNotFound, "runner not found")

	// Later heartbeats use the heartbeat secret and never reach the admitter.
	next := poolTestHeartbeat()
	next.RunnerID = resp.RunnerID
	_, err = registry.HeartbeatFrom(next, resp.HeartbeatSecret, "203.0.113.7")
	require.NoError(t, err)
	require.Len(t, admitter.seen(), 1)
}

func TestPoolRegistrationDefaultsToProbationAndOneRunner(t *testing.T) {
	admitter := &fakeAdmitter{answer: func(LiveRunnerAdmissionRequest) (*LiveRunnerAdmissionResponse, error) {
		return &LiveRunnerAdmissionResponse{Allow: true, MemberID: "m1"}, nil
	}}
	registry := newPoolTestRegistry(t, admitter)

	resp := poolTestRegister(t, registry, "c1")
	runner, unlock, err := registry.lockLiveRunner(resp.RunnerID)
	require.NoError(t, err)
	require.Equal(t, LiveRunnerStateProbation, runner.state)
	unlock()

	_, err = registry.HeartbeatFrom(poolTestHeartbeat(), "c1", "")
	requireRunnerError(t, err, http.StatusTooManyRequests, "member runner limit")
}

func TestPoolRegistrationRefusals(t *testing.T) {
	tests := []struct {
		name    string
		answer  func(LiveRunnerAdmissionRequest) (*LiveRunnerAdmissionResponse, error)
		status  int
		message string
	}{
		{"denied with a reason", func(LiveRunnerAdmissionRequest) (*LiveRunnerAdmissionResponse, error) {
			return &LiveRunnerAdmissionResponse{Allow: false, Reason: "unknown credential"}, nil
		}, http.StatusForbidden, "unknown credential"},
		{"denied without a reason", func(LiveRunnerAdmissionRequest) (*LiveRunnerAdmissionResponse, error) {
			return &LiveRunnerAdmissionResponse{}, nil
		}, http.StatusForbidden, "registration denied"},
		{"admitter unreachable", func(LiveRunnerAdmissionRequest) (*LiveRunnerAdmissionResponse, error) {
			return nil, errors.New("connection refused")
		}, http.StatusServiceUnavailable, "admission unavailable"},
		{"no member id", func(LiveRunnerAdmissionRequest) (*LiveRunnerAdmissionResponse, error) {
			return &LiveRunnerAdmissionResponse{Allow: true}, nil
		}, http.StatusServiceUnavailable, "admission unavailable"},
		{"unknown state", func(LiveRunnerAdmissionRequest) (*LiveRunnerAdmissionResponse, error) {
			return &LiveRunnerAdmissionResponse{Allow: true, MemberID: "m1", State: "trusted"}, nil
		}, http.StatusServiceUnavailable, "admission unavailable"},
		{"payout address that is not an address", func(LiveRunnerAdmissionRequest) (*LiveRunnerAdmissionResponse, error) {
			return &LiveRunnerAdmissionResponse{Allow: true, MemberID: "m1", PayoutAddress: "alice"}, nil
		}, http.StatusServiceUnavailable, "admission unavailable"},
		{"negative runner limit", func(LiveRunnerAdmissionRequest) (*LiveRunnerAdmissionResponse, error) {
			return &LiveRunnerAdmissionResponse{Allow: true, MemberID: "m1", MaxRunners: -1}, nil
		}, http.StatusServiceUnavailable, "admission unavailable"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			admitter := &fakeAdmitter{answer: tt.answer}
			registry := newPoolTestRegistry(t, admitter)
			_, err := registry.HeartbeatFrom(poolTestHeartbeat(), "c1", "")
			requireRunnerError(t, err, tt.status, tt.message)
			require.Len(t, admitter.seen(), 1)
			registry.mu.Lock()
			require.Empty(t, registry.runners, "a refused registration leaves no runner")
			registry.mu.Unlock()
		})
	}
}

func TestPoolRegistrationShape(t *testing.T) {
	admitter := &fakeAdmitter{}
	registry := newPoolTestRegistry(t, admitter)

	// No credential: refused before the admitter is asked.
	_, err := registry.HeartbeatFrom(poolTestHeartbeat(), "", "")
	requireRunnerError(t, err, http.StatusUnauthorized, "registration requires a credential")

	// A heartbeat that names a runner is never a registration, so a runner
	// cannot pick its id and a stale runner is told to register again.
	named := poolTestHeartbeat()
	named.RunnerID = "vllm"
	_, err = registry.HeartbeatFrom(named, "c1", "")
	requireRunnerError(t, err, http.StatusNotFound, "runner not found")

	// A runner_url instead of a tunnel is refused.
	dialed := poolTestHeartbeat()
	dialed.Tunnel = false
	dialed.RunnerURL = "http://127.0.0.1:7935"
	_, err = registry.HeartbeatFrom(dialed, "c1", "")
	requireRunnerError(t, err, http.StatusBadRequest, "pool runners must register with a tunnel")

	require.Empty(t, admitter.seen(), "none of these reach the admitter")

	// The registration secret is a credential like any other: it is forwarded,
	// and registers only if the admitter allows it.
	admitter.answer = func(req LiveRunnerAdmissionRequest) (*LiveRunnerAdmissionResponse, error) {
		return &LiveRunnerAdmissionResponse{Allow: false, Reason: "unknown credential"}, nil
	}
	_, err = registry.HeartbeatFrom(poolTestHeartbeat(), liveRunnerTestBootstrapSecret, "")
	requireRunnerError(t, err, http.StatusForbidden, "unknown credential")
	require.Len(t, admitter.seen(), 1)
	require.Equal(t, liveRunnerTestBootstrapSecret, admitter.seen()[0].Credential)
}

func TestPoolRunnerLimit(t *testing.T) {
	registry := newPoolTestRegistry(t, &fakeAdmitter{answer: admitAs("m1", LiveRunnerStateActive, 2)})

	first := poolTestRegister(t, registry, "c1")
	poolTestRegister(t, registry, "c1")
	_, err := registry.HeartbeatFrom(poolTestHeartbeat(), "c1", "")
	requireRunnerError(t, err, http.StatusTooManyRequests, "member runner limit")

	// A runner that leaves frees its place.
	require.NoError(t, registry.Unregister(first.RunnerID, first.HeartbeatSecret))
	poolTestRegister(t, registry, "c1")
}

func TestPoolRunnerLimitIsPerMember(t *testing.T) {
	registry := newPoolTestRegistry(t, &fakeAdmitter{answer: func(req LiveRunnerAdmissionRequest) (*LiveRunnerAdmissionResponse, error) {
		return &LiveRunnerAdmissionResponse{Allow: true, MemberID: req.Credential, MaxRunners: 1}, nil
	}})
	poolTestRegister(t, registry, "alice")
	poolTestRegister(t, registry, "bob")
	_, err := registry.HeartbeatFrom(poolTestHeartbeat(), "alice", "")
	requireRunnerError(t, err, http.StatusTooManyRequests, "member runner limit")
}

func TestPoolRunnerLimitHoldsUnderConcurrentRegistrations(t *testing.T) {
	registry := newPoolTestRegistry(t, &fakeAdmitter{answer: admitAs("m1", LiveRunnerStateActive, 1)})

	const attempts = 16
	var wg sync.WaitGroup
	results := make(chan error, attempts)
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := registry.HeartbeatFrom(poolTestHeartbeat(), "c1", "")
			results <- err
		}()
	}
	wg.Wait()
	close(results)

	admitted := 0
	for err := range results {
		if err == nil {
			admitted++
			continue
		}
		requireRunnerError(t, err, http.StatusTooManyRequests, "member runner limit")
	}
	require.Equal(t, 1, admitted)
}

func TestPoolRunnerCannotChangeWhatItWasAdmittedAs(t *testing.T) {
	changes := map[string]func(*LiveRunnerHeartbeatRequest){
		"app":      func(req *LiveRunnerHeartbeatRequest) { req.App = "flux/schnell" },
		"mode":     func(req *LiveRunnerHeartbeatRequest) { req.Mode = LiveRunnerModePersistent },
		"version":  func(req *LiveRunnerHeartbeatRequest) { req.Version = "vllm-app:9.9.9" },
		"label":    func(req *LiveRunnerHeartbeatRequest) { req.Label = "other" },
		"proxy":    func(req *LiveRunnerHeartbeatRequest) { req.Proxy = true },
		"metadata": func(req *LiveRunnerHeartbeatRequest) { req.Metadata = `{"fast":true}` },
		"gpu":      func(req *LiveRunnerHeartbeatRequest) { req.GPU = &LiveRunnerGPU{Name: "NVIDIA RTX 5090"} },
	}
	for field, change := range changes {
		t.Run(field, func(t *testing.T) {
			registry := newPoolTestRegistry(t, &fakeAdmitter{})
			resp := poolTestRegister(t, registry, "c1")

			req := poolTestHeartbeat()
			req.RunnerID = resp.RunnerID
			change(&req)
			_, err := registry.HeartbeatFrom(req, resp.HeartbeatSecret, "")
			requireRunnerError(t, err, http.StatusBadRequest, field+" cannot change after registration")

			runner, unlock, err := registry.lockLiveRunner(resp.RunnerID)
			require.NoError(t, err)
			require.Equal(t, poolTestHeartbeat().App, runner.App)
			require.Equal(t, poolTestHeartbeat().Mode, runner.Mode)
			require.Equal(t, poolTestHeartbeat().Version, runner.Version)
			unlock()
		})
	}
}

func TestPoolRunnerMayLowerCapacityAndReportStatus(t *testing.T) {
	registry := newPoolTestRegistry(t, &fakeAdmitter{answer: admitAs("m1", LiveRunnerStateActive, 1)})
	resp := poolTestRegister(t, registry, "c1")

	heartbeat := func(capacity int, status string) error {
		req := poolTestHeartbeat()
		req.RunnerID = resp.RunnerID
		req.Capacity = capacity
		req.Status = status
		_, err := registry.HeartbeatFrom(req, resp.HeartbeatSecret, "")
		return err
	}

	require.NoError(t, heartbeat(1, "ready"))
	require.Equal(t, 1, registry.Runners()[0].Capacity)
	require.NoError(t, heartbeat(2, "ready"), "back up to the registered capacity")
	requireRunnerError(t, heartbeat(3, "ready"), http.StatusBadRequest, "capacity cannot exceed the registered capacity")
	require.Equal(t, 2, registry.Runners()[0].Capacity)

	require.NoError(t, heartbeat(2, "unavailable"))
	require.Empty(t, registry.Runners())
	require.NoError(t, heartbeat(2, "ready"))
	require.Len(t, registry.Runners(), 1)
}

func TestPoolRunnerStates(t *testing.T) {
	registry := newPoolTestRegistry(t, &fakeAdmitter{answer: admitAs("m1", LiveRunnerStateActive, 1)})
	resp := poolTestRegister(t, registry, "c1")

	// Active: listed and usable.
	require.Len(t, registry.Runners(), 1)
	sessionID, _, err := registry.ReserveSession(resp.RunnerID)
	require.NoError(t, err)

	// Suspended: hidden, refuses new work, and lets the running session finish.
	setPoolTestState(t, registry, resp.RunnerID, LiveRunnerStateSuspended)
	require.Empty(t, registry.Runners())
	_, _, err = registry.ReserveSession(resp.RunnerID)
	requireRunnerError(t, err, http.StatusConflict, "runner suspended")
	_, err = registry.PaymentInfo(resp.RunnerID)
	requireRunnerError(t, err, http.StatusConflict, "runner suspended")
	_, err = registry.RunnerEndpointForSession(resp.RunnerID, sessionID)
	require.NoError(t, err)
	_, err = registry.SessionTokenForSession(resp.RunnerID, sessionID)
	require.NoError(t, err)
	require.NoError(t, registry.ReleaseSession(resp.RunnerID, sessionID))

	// A suspended runner keeps heartbeating without leaving the registry.
	req := poolTestHeartbeat()
	req.RunnerID = resp.RunnerID
	_, err = registry.HeartbeatFrom(req, resp.HeartbeatSecret, "")
	require.NoError(t, err)

	setPoolTestState(t, registry, resp.RunnerID, LiveRunnerStateActive)
	require.Len(t, registry.Runners(), 1)
}

func TestWithoutAnAdmitterRegistrationIsUnchanged(t *testing.T) {
	registry := newLiveRunnerTestRegistry()
	t.Cleanup(registry.Stop)

	// The registration secret registers, a runner picks its id and a runner_url,
	// and any field may change on a later heartbeat.
	req := liveRunnerTestHeartbeat("my-runner")
	resp := liveRunnerTestRegister(t, registry, req)
	require.Equal(t, "my-runner", resp.RunnerID)
	require.Len(t, registry.Runners(), 1)

	req.App = "another/app"
	req.Version = "2.0.0"
	req.Capacity = 9
	_, err := registry.Heartbeat(req, resp.HeartbeatSecret)
	require.NoError(t, err)
	require.Equal(t, "another/app", registry.Runners()[0].App)
	require.Equal(t, 9, registry.Runners()[0].Capacity)
}

func TestWebhookAdmitter(t *testing.T) {
	var got struct {
		method, contentType, auth string
		body                      LiveRunnerAdmissionRequest
	}
	answer := `{"allow":true,"member_id":"m1","payout_address":"0x00000000000000000000000000000000000000b1","max_runners":4,"state":"probation"}`
	status := http.StatusOK
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.method, got.contentType, got.auth = r.Method, r.Header.Get("Content-Type"), r.Header.Get("Authorization")
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &got.body)
		w.WriteHeader(status)
		_, _ = w.Write([]byte(answer))
	}))
	defer srv.Close()

	admitter, err := NewLiveRunnerWebhookAdmitter(srv.URL+"/admit", "pool-secret", time.Second)
	require.NoError(t, err)
	req := LiveRunnerAdmissionRequest{
		Event: "register", OrchAddress: "0xAA", RemoteAddr: "203.0.113.7", Credential: "c1",
		Runner: LiveRunnerAdmissionRunner{RunnerID: "runner_abc", Tunnel: true, App: "echo", Mode: LiveRunnerModeSingleShot, Capacity: 1},
	}

	resp, err := admitter.Admit(req)
	require.NoError(t, err)
	require.Equal(t, &LiveRunnerAdmissionResponse{
		Allow: true, MemberID: "m1", PayoutAddress: "0x00000000000000000000000000000000000000b1", MaxRunners: 4, State: "probation",
	}, resp)
	require.Equal(t, http.MethodPost, got.method)
	require.Equal(t, "application/json", got.contentType)
	require.Equal(t, "Bearer pool-secret", got.auth)
	require.Equal(t, req, got.body)

	answer = `{"allow":false,"reason":"banned"}`
	resp, err = admitter.Admit(req)
	require.NoError(t, err)
	require.False(t, resp.Allow)
	require.Equal(t, "banned", resp.Reason)

	// Anything but a 200 with a JSON body is no decision at all.
	status = http.StatusInternalServerError
	_, err = admitter.Admit(req)
	require.Error(t, err)
	status, answer = http.StatusOK, `not json`
	_, err = admitter.Admit(req)
	require.Error(t, err)
}

func TestWebhookAdmitterTimesOut(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
	}))
	defer srv.Close()
	defer close(release)

	admitter, err := NewLiveRunnerWebhookAdmitter(srv.URL, "", 50*time.Millisecond)
	require.NoError(t, err)
	registry := newPoolTestRegistry(t, admitter)
	_, err = registry.HeartbeatFrom(poolTestHeartbeat(), "c1", "")
	requireRunnerError(t, err, http.StatusServiceUnavailable, "admission unavailable")
}

func TestWebhookAdmitterRejectsBadURLs(t *testing.T) {
	for _, raw := range []string{"", "pool.example.com/admit", "ftp://pool.example.com", "/admit"} {
		_, err := NewLiveRunnerWebhookAdmitter(raw, "", time.Second)
		require.Error(t, err, raw)
	}
}
