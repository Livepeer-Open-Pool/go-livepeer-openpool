package runner

import (
	"net/http"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

const routingTestApp = "vllm/qwen2.5-7b-instruct"

// routingTestRunner registers a connected pool runner of the test app.
func routingTestRunner(t *testing.T, registry *LiveRunnerRegistry, mode string, capacity int) string {
	t.Helper()
	req := poolTestHeartbeat()
	req.Mode = mode
	req.Capacity = capacity
	resp, err := registry.HeartbeatFrom(req, "c1", "")
	require.NoError(t, err)
	attachPipeTunnel(t, registry, resp.RunnerID, resp.HeartbeatSecret)
	return resp.RunnerID
}

func sessionsOn(t *testing.T, registry *LiveRunnerRegistry) map[string]int {
	t.Helper()
	used := map[string]int{}
	for _, r := range registry.PoolRunners() {
		used[r.RunnerID] = r.CapacityUsed
	}
	return used
}

func TestPoolRoutingPicksTheLeastLoadedRunner(t *testing.T) {
	registry, recorder := newEventTestRegistry(t, false, &fakeAdmitter{answer: admitAs("m1", LiveRunnerStateActive, 10)})
	small := routingTestRunner(t, registry, LiveRunnerModePersistent, 1)
	mediumA := routingTestRunner(t, registry, LiveRunnerModePersistent, 2)
	mediumB := routingTestRunner(t, registry, LiveRunnerModePersistent, 2)
	// A fourth runner that is not taking work must never be chosen.
	probation := routingTestRunner(t, registry, LiveRunnerModePersistent, 5)
	require.NoError(t, registry.SetRunnerState(probation, LiveRunnerStateProbation, "retest"))

	// Three idle runners: the first three sessions land one on each.
	chosen := map[string]bool{}
	for i := 0; i < 3; i++ {
		runnerID, sessionID, appURL, err := registry.ReservePoolSession(routingTestApp, LiveRunnerModePersistent)
		require.NoError(t, err)
		require.NotEmpty(t, sessionID)
		require.Equal(t, "http://localhost:1234/apps/"+runnerID+"/session/"+sessionID+"/app", appURL)
		chosen[runnerID] = true
	}
	require.Equal(t, map[string]bool{small: true, mediumA: true, mediumB: true}, chosen)

	// The next two go to the two runners that still have room.
	for i := 0; i < 2; i++ {
		runnerID, _, _, err := registry.ReservePoolSession(routingTestApp, LiveRunnerModePersistent)
		require.NoError(t, err)
		require.Contains(t, []string{mediumA, mediumB}, runnerID)
	}
	require.Equal(t, map[string]int{small: 1, mediumA: 2, mediumB: 2, probation: 0}, sessionsOn(t, registry))

	_, _, _, err := registry.ReservePoolSession(routingTestApp, LiveRunnerModePersistent)
	requireRunnerError(t, err, http.StatusServiceUnavailable, "no capacity for app")

	// Sessions reached by app say so in their events.
	for _, event := range recorder.wait(t, 4+1+5) {
		if event.eventType == LiveRunnerEventSessionReserved {
			require.Equal(t, LiveRunnerViaPool, event.fields["via"])
		}
	}
}

func TestPoolRoutingSpreadsWorkAcrossIdleRunners(t *testing.T) {
	registry := newPoolTestRegistry(t, &fakeAdmitter{answer: admitAs("m1", LiveRunnerStateActive, 10)})
	for i := 0; i < 3; i++ {
		routingTestRunner(t, registry, LiveRunnerModeSingleShot, 1)
	}
	chosen := map[string]int{}
	for i := 0; i < 90; i++ {
		runnerID, sessionID, _, err := registry.ReservePoolSession(routingTestApp, LiveRunnerModeSingleShot)
		require.NoError(t, err)
		require.NoError(t, registry.ReleaseSession(runnerID, sessionID))
		chosen[runnerID]++
	}
	require.Len(t, chosen, 3, "ties are broken at random, so every idle runner gets work: %v", chosen)
}

func TestPoolRoutingOnlyUsesRunnersThatCanTakeTheRequest(t *testing.T) {
	registry := newPoolTestRegistry(t, &fakeAdmitter{answer: admitAs("m1", LiveRunnerStateActive, 10)})

	_, _, _, err := registry.ReservePoolSession("flux/schnell", LiveRunnerModeSingleShot)
	requireRunnerError(t, err, http.StatusNotFound, "app not offered")
	_, err = registry.PoolPaymentInfo("flux/schnell", LiveRunnerModeSingleShot)
	requireRunnerError(t, err, http.StatusNotFound, "app not offered")

	// Offered, but nobody serves it.
	_, _, _, err = registry.ReservePoolSession(routingTestApp, LiveRunnerModeSingleShot)
	requireRunnerError(t, err, http.StatusServiceUnavailable, "no capacity for app")

	// A runner of the other mode does not count.
	persistent := routingTestRunner(t, registry, LiveRunnerModePersistent, 1)
	_, _, _, err = registry.ReservePoolSession(routingTestApp, LiveRunnerModeSingleShot)
	requireRunnerError(t, err, http.StatusServiceUnavailable, "no capacity for app")
	runnerID, _, _, err := registry.ReservePoolSession(routingTestApp, LiveRunnerModePersistent)
	require.NoError(t, err)
	require.Equal(t, persistent, runnerID)

	// Neither does a suspended one, nor one that reports itself unavailable,
	// nor one whose tunnel is down.
	suspended := routingTestRunner(t, registry, LiveRunnerModeSingleShot, 1)
	require.NoError(t, registry.SetRunnerState(suspended, LiveRunnerStateSuspended, ""))
	disconnected := poolTestHeartbeat()
	_, err = registry.HeartbeatFrom(disconnected, "c1", "")
	require.NoError(t, err)
	_, _, _, err = registry.ReservePoolSession(routingTestApp, LiveRunnerModeSingleShot)
	requireRunnerError(t, err, http.StatusServiceUnavailable, "no capacity for app")

	require.NoError(t, registry.SetRunnerState(suspended, LiveRunnerStateActive, ""))
	runnerID, _, _, err = registry.ReservePoolSession(routingTestApp, LiveRunnerModeSingleShot)
	require.NoError(t, err)
	require.Equal(t, suspended, runnerID)
}

func TestPoolRoutingNeverOverbooksUnderConcurrency(t *testing.T) {
	registry := newPoolTestRegistry(t, &fakeAdmitter{answer: admitAs("m1", LiveRunnerStateActive, 10)})
	for _, capacity := range []int{1, 2, 3} {
		routingTestRunner(t, registry, LiveRunnerModePersistent, capacity)
	}

	const attempts = 40
	var wg sync.WaitGroup
	results := make(chan error, attempts)
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, _, err := registry.ReservePoolSession(routingTestApp, LiveRunnerModePersistent)
			results <- err
		}()
	}
	wg.Wait()
	close(results)
	reserved := 0
	for err := range results {
		if err == nil {
			reserved++
			continue
		}
		requireRunnerError(t, err, http.StatusServiceUnavailable, "no capacity for app")
	}
	require.Equal(t, 6, reserved, "exactly the pool's capacity")
	for _, r := range registry.PoolRunners() {
		require.Equal(t, r.Capacity, r.CapacityUsed)
	}
}

func TestPoolSessionIsFoundByItsApp(t *testing.T) {
	registry := newPoolTestRegistry(t, &fakeAdmitter{answer: admitAs("m1", LiveRunnerStateActive, 10)})
	routingTestRunner(t, registry, LiveRunnerModePersistent, 1)
	routingTestRunner(t, registry, LiveRunnerModePersistent, 1)

	runnerID, sessionID, _, err := registry.ReservePaidPoolSession(routingTestApp, LiveRunnerModePersistent, "manifest-1", "0xc1")
	require.NoError(t, err)
	require.Equal(t, "manifest-1", sessionID)

	found, err := registry.RunnerForPoolSession(routingTestApp, "manifest-1")
	require.NoError(t, err)
	require.Equal(t, runnerID, found)
	_, err = registry.RunnerForPoolSession("echo", "manifest-1")
	requireRunnerError(t, err, http.StatusNotFound, "runner session not found")
	_, err = registry.RunnerForPoolSession(routingTestApp, "manifest-2")
	requireRunnerError(t, err, http.StatusNotFound, "runner session not found")
}

func TestPoolAppsDescribesWhatCustomersCanAskFor(t *testing.T) {
	registry := newOnchainPoolTestRegistry(t, &fakeAdmitter{answer: admitAs("m1", LiveRunnerStateActive, 10)})
	require.Empty(t, registry.PoolApps(), "an offered app nobody serves is not listed")

	routingTestRunner(t, registry, LiveRunnerModePersistent, 1)
	routingTestRunner(t, registry, LiveRunnerModePersistent, 2)
	routingTestRunner(t, registry, LiveRunnerModePersistent, 2)
	probation := routingTestRunner(t, registry, LiveRunnerModePersistent, 5)
	require.NoError(t, registry.SetRunnerState(probation, LiveRunnerStateProbation, ""))
	routingTestRunner(t, registry, LiveRunnerModeSingleShot, 4)

	apps := registry.PoolApps()
	require.Len(t, apps, 2, "one entry per app and mode")
	persistent, singleShot := apps[0], apps[1]
	require.Equal(t, LiveRunnerDiscoveryApp{
		App:  routingTestApp,
		URL:  "http://localhost:1234/pool/dmxsbS9xd2VuMi41LTdiLWluc3RydWN0/session",
		Mode: LiveRunnerModePersistent,
		PriceInfo: &LiveRunnerPriceInfo{
			Price: persistent.PriceInfo.Price, Currency: "wei", Unit: "seconds",
		},
		Runners: 3, Capacity: 5, CapacityAvailable: 5,
	}, persistent)
	require.Equal(t, weiPerSecond(t, 10), persistent.PriceInfo.Price.String())
	require.Equal(t, "http://localhost:1234/pool/dmxsbS9xd2VuMi41LTdiLWluc3RydWN0/app", singleShot.URL)
	require.Equal(t, 1, singleShot.Runners)
	require.Equal(t, 4, singleShot.Capacity)

	for want := 4; want >= 0; want-- {
		_, _, _, err := registry.ReservePaidPoolSession(routingTestApp, LiveRunnerModePersistent, "", "0xc1")
		require.NoError(t, err)
		require.Equal(t, want, registry.PoolApps()[0].CapacityAvailable)
	}

	// The price a customer is challenged with is the app's.
	priceInfo, err := registry.PoolPaymentInfo(routingTestApp, LiveRunnerModeSingleShot)
	require.NoError(t, err)
	require.Equal(t, weiPerSecond(t, 10), priceInfo.Price.String())
	_, err = registry.PoolPaymentInfo(routingTestApp, LiveRunnerModePersistent)
	requireRunnerError(t, err, http.StatusServiceUnavailable, "no capacity for app")
}

func TestPoolAppsIsEmptyWithoutAPriceTable(t *testing.T) {
	registry := newLiveRunnerTestRegistry()
	t.Cleanup(registry.Stop)
	liveRunnerTestRegister(t, registry, liveRunnerTestHeartbeat("runner-1"))
	require.Len(t, registry.Runners(), 1)
	require.Nil(t, registry.PoolApps())
	_, _, _, err := registry.ReservePoolSession("new-ai-pipeline/model-a", LiveRunnerModePersistent)
	requireRunnerError(t, err, http.StatusNotFound, "app not offered")
}

func TestLiveRunnerAppEncoding(t *testing.T) {
	require.Equal(t, "dmxsbS9xd2VuMi41LTdiLWluc3RydWN0", EncodeLiveRunnerApp(routingTestApp))
	for _, app := range []string{routingTestApp, "echo", "a/b/c", "über/模型", "a?b#c d"} {
		decoded, err := DecodeLiveRunnerApp(EncodeLiveRunnerApp(app))
		require.NoError(t, err)
		require.Equal(t, app, decoded)
		require.NotContains(t, EncodeLiveRunnerApp(app), "/")
	}
	for _, segment := range []string{"", "ZWNobw==", "not base64!", "vllm/qwen", "ZWNobw=", "ZWNo bw"} {
		_, err := DecodeLiveRunnerApp(segment)
		requireRunnerError(t, err, http.StatusBadRequest, "app must be base64url without padding")
	}
}
