package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/livepeer/go-livepeer/ai/runner"
	"github.com/livepeer/go-livepeer/core"
	"github.com/livepeer/go-livepeer/trickle"
)

// admitEveryone admits each registration as the member named by its credential.
type admitEveryone struct{}

func (admitEveryone) Admit(req runner.LiveRunnerAdmissionRequest) (*runner.LiveRunnerAdmissionResponse, error) {
	return &runner.LiveRunnerAdmissionResponse{Allow: true, MemberID: req.Credential, MaxRunners: 4}, nil
}

func newLiveRunnerAdminServer(t *testing.T) (*runner.LiveRunnerRegistry, *httptest.Server) {
	t.Helper()
	n, err := core.NewLivepeerNode(nil, t.TempDir(), nil)
	require.NoError(t, err)
	manager := runner.NewLiveRunnerRegistry(runner.LiveRunnerRegistryConfig{
		Host:     newStubOrchestrator(),
		Admitter: admitEveryone{},
		Prices: runner.LiveRunnerPriceTable{Apps: map[string]runner.LiveRunnerPriceInfo{
			"echo": {Price: "0.001", Currency: "usd", Unit: "fixed"},
		}},
	})
	t.Cleanup(manager.Stop)
	// Registering a runner creates its bootstrap channel on the trickle server.
	trickleSrv := trickle.ConfigureServer(trickle.TrickleServerConfig{Mux: http.NewServeMux(), BasePath: TrickleHTTPPath})
	manager.SetTrickleServer(trickleSrv, "http://localhost"+TrickleHTTPPath, "http://localhost"+TrickleHTTPPath)
	n.LiveRunnerManager = manager
	s := &LivepeerServer{LivepeerNode: n}
	srv := httptest.NewServer(s.cliWebServerHandlers("addr"))
	t.Cleanup(srv.Close)
	return manager, srv
}

func registerAdminTestRunner(t *testing.T, manager *runner.LiveRunnerRegistry, credential string) string {
	t.Helper()
	resp, err := manager.HeartbeatFrom(runner.LiveRunnerHeartbeatRequest{
		App: "echo", Mode: runner.LiveRunnerModeSingleShot, Capacity: 1, Tunnel: true,
	}, credential, "203.0.113.7")
	require.NoError(t, err)
	return resp.RunnerID
}

func postAdminForm(t *testing.T, target string, form url.Values) (int, string) {
	t.Helper()
	res, err := http.PostForm(target, form)
	require.NoError(t, err)
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	return res.StatusCode, strings.TrimSpace(string(body))
}

func listAdminRunners(t *testing.T, srvURL string) []runner.LiveRunnerPoolRunner {
	t.Helper()
	res, err := http.Get(srvURL + "/liveRunner/runners")
	require.NoError(t, err)
	defer res.Body.Close()
	require.Equal(t, http.StatusOK, res.StatusCode)
	require.Equal(t, "application/json", res.Header.Get("Content-Type"))
	var out liveRunnerPoolRunnersResponse
	require.NoError(t, json.NewDecoder(res.Body).Decode(&out))
	return out.Runners
}

func TestLiveRunnerAdminListAndSetState(t *testing.T) {
	manager, srv := newLiveRunnerAdminServer(t)
	require.Empty(t, listAdminRunners(t, srv.URL))
	runnerID := registerAdminTestRunner(t, manager, "alice")

	runners := listAdminRunners(t, srv.URL)
	require.Len(t, runners, 1)
	require.Equal(t, runnerID, runners[0].RunnerID)
	require.Equal(t, "alice", runners[0].MemberID)
	require.Equal(t, runner.LiveRunnerStateProbation, runners[0].State)
	require.Equal(t, "203.0.113.7", runners[0].RemoteAddr)
	require.True(t, runners[0].Tunnel)
	require.False(t, runners[0].TunnelConnected)

	status, body := postAdminForm(t, srv.URL+"/liveRunner/setState", url.Values{"runner_id": {runnerID}, "state": {"active"}, "reason": {"probe passed"}})
	require.Equal(t, http.StatusNoContent, status, body)
	require.Equal(t, runner.LiveRunnerStateActive, listAdminRunners(t, srv.URL)[0].State)

	status, body = postAdminForm(t, srv.URL+"/liveRunner/setState", url.Values{"runner_id": {runnerID}, "state": {"trusted"}})
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "state must be probation, active, or suspended", body)
	status, body = postAdminForm(t, srv.URL+"/liveRunner/setState", url.Values{"runner_id": {"runner_missing"}, "state": {"active"}})
	require.Equal(t, http.StatusNotFound, status)
	require.Equal(t, "runner not found", body)
	status, _ = postAdminForm(t, srv.URL+"/liveRunner/setState", url.Values{"runner_id": {runnerID}})
	require.Equal(t, http.StatusBadRequest, status)

	res, err := http.Get(srv.URL + "/liveRunner/setState")
	require.NoError(t, err)
	res.Body.Close()
	require.Equal(t, http.StatusMethodNotAllowed, res.StatusCode)
}

func TestLiveRunnerAdminEvict(t *testing.T) {
	manager, srv := newLiveRunnerAdminServer(t)
	first := registerAdminTestRunner(t, manager, "alice")
	registerAdminTestRunner(t, manager, "alice")
	bob := registerAdminTestRunner(t, manager, "bob")

	status, body := postAdminForm(t, srv.URL+"/liveRunner/evict", url.Values{})
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "give runner_id or member_id, not both", body)
	status, _ = postAdminForm(t, srv.URL+"/liveRunner/evict", url.Values{"runner_id": {first}, "member_id": {"alice"}})
	require.Equal(t, http.StatusBadRequest, status)
	require.Len(t, listAdminRunners(t, srv.URL), 3)

	status, body = postAdminForm(t, srv.URL+"/liveRunner/evict", url.Values{"runner_id": {first}, "reason": {"returned garbage"}})
	require.Equal(t, http.StatusNoContent, status, body)
	require.Len(t, listAdminRunners(t, srv.URL), 2)
	status, _ = postAdminForm(t, srv.URL+"/liveRunner/evict", url.Values{"runner_id": {first}})
	require.Equal(t, http.StatusNotFound, status)

	status, body = postAdminForm(t, srv.URL+"/liveRunner/evict", url.Values{"member_id": {"alice"}, "reason": {"banned"}})
	require.Equal(t, http.StatusNoContent, status, body)
	remaining := listAdminRunners(t, srv.URL)
	require.Len(t, remaining, 1)
	require.Equal(t, bob, remaining[0].RunnerID)
	status, body = postAdminForm(t, srv.URL+"/liveRunner/evict", url.Values{"member_id": {"alice"}})
	require.Equal(t, http.StatusNotFound, status)
	require.Equal(t, "member has no runners", body)
}

func TestLiveRunnerAdminPrices(t *testing.T) {
	manager, srv := newLiveRunnerAdminServer(t)
	before := manager.Prices()

	res, err := http.Post(srv.URL+"/liveRunner/prices", "application/json", strings.NewReader(`{"apps":{"echo":{"price":"1","unit":"minute"}}}`))
	require.NoError(t, err)
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()
	require.Equal(t, http.StatusBadRequest, res.StatusCode)
	require.Contains(t, string(body), `apps["echo"]`)
	require.Equal(t, before, manager.Prices())

	res, err = http.Post(srv.URL+"/liveRunner/prices", "application/json", strings.NewReader(`{"apps":{"vllm/qwen2.5-7b-instruct":{"price":"0.50","currency":"usd","unit":"hour"}}}`))
	require.NoError(t, err)
	res.Body.Close()
	require.Equal(t, http.StatusNoContent, res.StatusCode)
	require.Equal(t, runner.LiveRunnerPriceTable{Apps: map[string]runner.LiveRunnerPriceInfo{
		"vllm/qwen2.5-7b-instruct": {Price: "0.50", Currency: "usd", Unit: "hour"},
	}}, manager.Prices())
}

func TestLiveRunnerAdminWithoutLiveRunners(t *testing.T) {
	n, err := core.NewLivepeerNode(nil, t.TempDir(), nil)
	require.NoError(t, err)
	s := &LivepeerServer{LivepeerNode: n}
	srv := httptest.NewServer(s.cliWebServerHandlers("addr"))
	defer srv.Close()

	res, err := http.Get(srv.URL + "/liveRunner/runners")
	require.NoError(t, err)
	res.Body.Close()
	require.Equal(t, http.StatusNotFound, res.StatusCode)
	status, _ := postAdminForm(t, srv.URL+"/liveRunner/evict", url.Values{"member_id": {"alice"}})
	require.Equal(t, http.StatusNotFound, status)
}
