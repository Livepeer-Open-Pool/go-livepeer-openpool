package server

import (
	"encoding/json"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"testing/synctest"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"

	"github.com/livepeer/go-livepeer/ai/runner"
	"github.com/livepeer/go-livepeer/core"
	"github.com/livepeer/go-livepeer/eth"
	"github.com/livepeer/go-tools/drivers"
)

const poolRouteTestApp = "vllm/qwen2.5-7b-instruct"

var poolRouteTestSegment = runner.EncodeLiveRunnerApp(poolRouteTestApp)

// namedApp is an app that answers every request with its name, so a test can
// tell which runner served it.
func namedApp(name string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, name)
	})
}

func discoveredApps(t *testing.T, orchURL string) []runner.LiveRunnerDiscoveryApp {
	t.Helper()
	res, err := http.Get(orchURL + "/discovery")
	require.NoError(t, err)
	defer res.Body.Close()
	var entries []liveRunnerDiscoveryEntry
	require.NoError(t, json.NewDecoder(res.Body).Decode(&entries))
	require.Len(t, entries, 1)
	return entries[0].Apps
}

func TestPoolRouteServesAnAppFromWhicheverRunnerIsFree(t *testing.T) {
	webhook := newPoolWebhook(t, runner.LiveRunnerAdmissionResponse{Allow: true, MemberID: "m1", MaxRunners: 4, State: runner.LiveRunnerStateActive})
	lp, orch := newPoolLiveRunnerHTTP(t, webhook)
	first := startPoolRunnerFor(t, orch.URL, "member-credential", "single-shot", 2, namedApp("first"))
	second := startPoolRunnerFor(t, orch.URL, "member-credential", "single-shot", 2, namedApp("second"))
	waitPoolRunnerConnected(t, lp, first)
	waitPoolRunnerConnected(t, lp, second)

	// Discovery tells the customer where to ask for the app.
	apps := discoveredApps(t, orch.URL)
	require.Len(t, apps, 1)
	require.Equal(t, poolRouteTestApp, apps[0].App)
	require.Equal(t, runner.LiveRunnerModeSingleShot, apps[0].Mode)
	require.Equal(t, 2, apps[0].Runners)
	require.Equal(t, 4, apps[0].Capacity)
	appURL, err := url.Parse(apps[0].URL)
	require.NoError(t, err)
	require.Equal(t, "/pool/"+poolRouteTestSegment+"/app", appURL.Path)

	// Each request is routed on its own, so both runners get work.
	served := map[string]int{}
	for i := 0; i < 40; i++ {
		res, err := http.Get(orch.URL + appURL.Path + "/v1/models?x=1")
		require.NoError(t, err)
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
		served[string(body)]++
	}
	require.Len(t, served, 2, "served: %v", served)
	require.Equal(t, 40, served["first"]+served["second"])
}

func TestPoolRouteRefusals(t *testing.T) {
	webhook := newPoolWebhook(t, runner.LiveRunnerAdmissionResponse{Allow: true, MemberID: "m1", State: runner.LiveRunnerStateSuspended})
	lp, orch := newPoolLiveRunnerHTTP(t, webhook)
	client := startPoolRunner(t, orch.URL, "member-credential")
	waitPoolRunnerConnected(t, lp, client)
	base := orch.URL + "/pool/" + poolRouteTestSegment

	status, body := doRequest(t, probeRequestWithout(t, orch.URL+"/pool/not base64!/app/x"))
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "app must be base64url without padding", body)

	status, body = doRequest(t, probeRequestWithout(t, orch.URL+"/pool/"+runner.EncodeLiveRunnerApp("flux/schnell")+"/app/x"))
	require.Equal(t, http.StatusNotFound, status)
	require.Equal(t, "app not offered", body)

	// The only runner is suspended, and nothing serves the app in the other mode.
	status, body = doRequest(t, probeRequestWithout(t, base+"/app/x"))
	require.Equal(t, http.StatusServiceUnavailable, status)
	require.Equal(t, "no capacity for app", body)
	res, err := http.Post(base+"/session", "application/json", nil)
	require.NoError(t, err)
	res.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, res.StatusCode)
	require.Empty(t, discoveredApps(t, orch.URL))

	// A probe tests one runner, so it cannot be sent to an app.
	status, body = doRequest(t, probeRequest(t, http.MethodGet, base+"/app/x", poolTestSecret))
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "a probe names its runner", body)
}

func TestPoolRouteOpensAPersistentSessionOnAChosenRunner(t *testing.T) {
	webhook := newPoolWebhook(t, runner.LiveRunnerAdmissionResponse{Allow: true, MemberID: "m1", MaxRunners: 4, State: runner.LiveRunnerStateActive})
	lp, orch := newPoolLiveRunnerHTTP(t, webhook)
	app := &headerRecorder{}
	client := startPoolRunnerFor(t, orch.URL, "member-credential", "persistent", 1, app)
	runnerID := waitPoolRunnerConnected(t, lp, client)
	sessionURL := orch.URL + "/pool/" + poolRouteTestSegment + "/session"

	res, err := http.Post(sessionURL, "application/json", nil)
	require.NoError(t, err)
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()
	require.Equal(t, http.StatusOK, res.StatusCode, string(body))
	var session liveRunnerSessionResponse
	require.NoError(t, json.Unmarshal(body, &session))
	require.Equal(t, runnerID, session.RunnerID, "the response says which runner was chosen")

	// What follows the reservation addresses that runner, as it always did.
	appURL, err := url.Parse(session.AppURL)
	require.NoError(t, err)
	require.Equal(t, "/apps/"+runnerID+"/session/"+session.SessionID+"/app", appURL.Path)
	controlURL, err := url.Parse(session.ControlURL)
	require.NoError(t, err)
	require.Equal(t, "/apps/"+runnerID+"/session/"+session.SessionID, controlURL.Path)

	status, _ := doRequest(t, probeRequestWithout(t, orch.URL+appURL.Path+"/echo"))
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, session.SessionID, app.last().Get("Livepeer-Session-Id"))

	// The runner's one slot is taken until the session is stopped.
	res, err = http.Post(sessionURL, "application/json", nil)
	require.NoError(t, err)
	res.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, res.StatusCode)
	stop, err := http.NewRequest(http.MethodPost, orch.URL+controlURL.Path+"/stop", nil)
	require.NoError(t, err)
	status, _ = doRequest(t, stop)
	require.Equal(t, http.StatusNoContent, status)
	res, err = http.Post(sessionURL, "application/json", nil)
	require.NoError(t, err)
	res.Body.Close()
	require.Equal(t, http.StatusOK, res.StatusCode)
}

// newPoolRouteHTTPOnchain is an on-chain orchestrator whose price table lists
// the app registerLiveRunnerForSession registers, so the app is routable.
func newPoolRouteHTTPOnchain(t *testing.T) (*lphttp, *liveRunnerEventLog) {
	t.Helper()
	prevWatcher := core.PriceFeedWatcher
	core.PriceFeedWatcher = stubPriceFeedWatcher{price: eth.PriceData{Price: big.NewRat(2000, 1)}}
	t.Cleanup(func() { core.PriceFeedWatcher = prevWatcher })

	node, err := core.NewLivepeerNode(nil, t.TempDir(), nil)
	require.NoError(t, err)
	node.LivePaymentInterval = 5 * time.Second
	orch := newStubOrchestrator()
	orch.secret = "live-runner-secret"
	orch.ticketParams = defaultTicketParams()
	orch.balances = make(map[ethcommon.Address]map[core.ManifestID]*big.Rat)
	lp := &lphttp{orchestrator: orch, node: node, transRPC: http.NewServeMux()}
	log := &liveRunnerEventLog{}
	manager := runner.NewLiveRunnerRegistry(runner.LiveRunnerRegistryConfig{
		Host:    testLiveRunnerHost{orchestrator: lp.orchestrator},
		Onchain: true,
		Prices: runner.LiveRunnerPriceTable{Apps: map[string]runner.LiveRunnerPriceInfo{
			"live-video-to-video/scope": {Price: "0.001990656", Currency: "usd", Unit: "720p"},
		}},
		Events: log.sink,
	})
	t.Cleanup(manager.Stop)
	node.LiveRunnerManager = manager
	require.NoError(t, startAIServer(lp))
	return lp, log
}

func TestPoolRoutePaidSessionAndItsFollowUpPayments(t *testing.T) {
	oldStorage := drivers.NodeStorage
	drivers.NodeStorage = drivers.NewMemoryDriver(nil)
	t.Cleanup(func() { drivers.NodeStorage = oldStorage })

	// In a bubble, so the session's payment monitor runs on virtual time and
	// is gone when the test ends.
	synctest.Test(t, func(t *testing.T) {
		lp, log := newPoolRouteHTTPOnchain(t)
		registerLiveRunnerForSession(t, lp, &liveRunnerRegistrationOptions{RunnerID: "runner-1"})
		registerLiveRunnerForSession(t, lp, &liveRunnerRegistrationOptions{RunnerID: "runner-2"})
		orch := lp.orchestrator.(*stubOrchestrator)
		orch.paymentCredit = big.NewRat(100, 1)
		segment := runner.EncodeLiveRunnerApp("live-video-to-video/scope")

		// Asked for the app without payment, the customer is told the app's price
		// and where to pay; no runner is involved yet.
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/pool/"+segment+"/session", nil)
		setRequestHeaders(req, liveRunnerSenderHeaders(orch))
		lp.ServeHTTP(w, req)
		require.Equal(t, http.StatusPaymentRequired, w.Code, w.Body.String())
		challenge, oInfo := decodeLiveRunnerPaymentChallenge(t, w.Body.Bytes())
		paymentPath := "/pool/" + segment + "/session/" + challenge.ManifestID + "/payment"
		require.Equal(t, orch.ServiceURI().String()+paymentPath, challenge.PaymentURL)
		headers := liveRunnerReservationPaymentHeaders(t, orch, oInfo.GetAuthToken(), challenge.ManifestID)

		// The paid retry reserves the session on a runner the orchestrator chose.
		w = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodPost, "/pool/"+segment+"/session", nil)
		setRequestHeaders(req, headers)
		lp.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		var session liveRunnerSessionResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &session))
		require.Equal(t, challenge.ManifestID, session.SessionID)
		require.Contains(t, []string{"runner-1", "runner-2"}, session.RunnerID)
		require.Equal(t, orch.ServiceURI().String()+"/apps/"+session.RunnerID+"/session/"+session.SessionID, session.ControlURL)
		manager := lp.node.LiveRunnerManager.(*runner.LiveRunnerRegistry)
		_, err := manager.RunnerEndpointForSession(session.RunnerID, session.SessionID)
		require.NoError(t, err)

		// A follow-up payment sent where the challenge said reaches that session.
		w = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodPost, paymentPath, nil)
		setRequestHeaders(req, headers)
		lp.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		paymentResponse, _ := decodeLiveRunnerPaymentChallenge(t, w.Body.Bytes())
		require.Equal(t, challenge.ManifestID, paymentResponse.ManifestID)
		balance := orch.Balance(orch.Address(), core.ManifestID(challenge.ManifestID))
		require.Equal(t, "200", balance.FloatString(0))

		// A payment for a session the app does not hold is refused.
		w = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodPost, "/pool/"+segment+"/session/unknown/payment", nil)
		setRequestHeaders(req, headers)
		lp.ServeHTTP(w, req)
		require.Equal(t, http.StatusNotFound, w.Code)

		// The session's events name the runner that was chosen.
		synctest.Wait()
		require.Len(t, log.ofType(runner.LiveRunnerEventSessionReserved), 1)
		reserved := log.ofType(runner.LiveRunnerEventSessionReserved)[0]
		require.Equal(t, session.RunnerID, reserved["runner_id"])
		require.Equal(t, runner.LiveRunnerViaPool, reserved["via"])
		require.Equal(t, true, reserved["paid"])

		// Stop the session and let its payment monitor wind down.
		require.NoError(t, manager.ReleaseSession(session.RunnerID, session.SessionID))
		synctest.Sleep(lp.node.LivePaymentInterval)
		manager.Stop()
	})
}

func TestRunnerRouteSessionResponseDoesNotNameTheRunner(t *testing.T) {
	lp := newLiveRunnerHTTP(t, true)
	registerLiveRunnerForSession(t, lp, nil)
	w := httptest.NewRecorder()
	lp.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/apps/runner-1/session", nil))
	require.Equal(t, http.StatusOK, w.Code)
	require.NotContains(t, w.Body.String(), "runner_id", "unchanged for a session asked for by runner")

	// And discovery lists no apps when the operator set no prices.
	w = httptest.NewRecorder()
	lp.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/discovery", nil))
	require.NotContains(t, w.Body.String(), `"apps"`)
}
