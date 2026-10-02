package runner

import (
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type recordedEvent struct {
	eventType string
	key       string
	fields    map[string]interface{}
}

// eventRecorder is an event sink that keeps what it is given.
type eventRecorder struct {
	mu     sync.Mutex
	events []recordedEvent
	// block, when set, is received from before each event is recorded.
	block chan struct{}
}

func (r *eventRecorder) sink(eventType, key string, fields map[string]interface{}) error {
	if r.block != nil {
		<-r.block
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, recordedEvent{eventType, key, fields})
	return nil
}

// wait returns the events once n of them have been published.
func (r *eventRecorder) wait(t *testing.T, n int) []recordedEvent {
	t.Helper()
	var events []recordedEvent
	require.Eventually(t, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		events = append([]recordedEvent(nil), r.events...)
		return len(events) >= n
	}, 5*time.Second, 5*time.Millisecond, "expected %d events", n)
	require.Len(t, events, n)
	return events
}

func eventTypes(events []recordedEvent) []string {
	types := make([]string, 0, len(events))
	for _, event := range events {
		types = append(types, event.eventType)
	}
	return types
}

func newEventTestRegistry(t *testing.T, onchain bool, admitter LiveRunnerAdmitter) (*LiveRunnerRegistry, *eventRecorder) {
	t.Helper()
	recorder := &eventRecorder{}
	config := LiveRunnerRegistryConfig{
		Host:        liveRunnerTestHost{},
		Onchain:     onchain,
		Admitter:    admitter,
		OrchAddress: "0x00000000000000000000000000000000000000AA",
		Prices:      poolTestPrices(),
		Events:      recorder.sink,
	}
	registry := newLiveRunnerTestRegistryWithConfig(config)
	t.Cleanup(registry.Stop)
	return registry, recorder
}

func TestEventsFollowAPoolRunnerThroughItsLife(t *testing.T) {
	registry, recorder := newEventTestRegistry(t, false, &fakeAdmitter{answer: func(LiveRunnerAdmissionRequest) (*LiveRunnerAdmissionResponse, error) {
		return &LiveRunnerAdmissionResponse{Allow: true, MemberID: "m1", PayoutAddress: "0x00000000000000000000000000000000000000b1", MaxRunners: 2}, nil
	}})
	started := time.Now().UnixMilli()

	resp := poolTestRegister(t, registry, "c1")
	probeID, _, err := registry.ReserveProbeSession(resp.RunnerID)
	require.NoError(t, err)
	require.NoError(t, registry.ReleaseSession(resp.RunnerID, probeID))
	require.NoError(t, registry.SetRunnerState(resp.RunnerID, LiveRunnerStateActive, "probe passed"))

	const payer = "0x00000000000000000000000000000000000000c1"
	paidID, _, err := registry.ReservePaidSession(resp.RunnerID, "manifest-1", payer)
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		require.NoError(t, registry.RecordSessionFee(resp.RunnerID, paidID, payer, 5, big.NewRat(100, 1), big.NewRat(int64(1000-100*(i+1)), 1)))
	}
	require.NoError(t, registry.ReleaseSessionWithReason(resp.RunnerID, paidID, LiveRunnerReleaseCompleted))
	lastID, _, err := registry.ReserveSession(resp.RunnerID)
	require.NoError(t, err)
	require.NoError(t, registry.EvictRunner(resp.RunnerID, "returned garbage"))

	events := recorder.wait(t, 12)
	require.Equal(t, []string{
		LiveRunnerEventRegistered,
		LiveRunnerEventSessionReserved, LiveRunnerEventSessionReleased,
		LiveRunnerEventStateChanged,
		LiveRunnerEventSessionReserved,
		LiveRunnerEventFeeDebited, LiveRunnerEventFeeDebited, LiveRunnerEventFeeDebited,
		LiveRunnerEventSessionReleased,
		LiveRunnerEventSessionReserved, LiveRunnerEventSessionReleased,
		LiveRunnerEventRemoved,
	}, eventTypes(events), "in the order they happened")

	// Every event says whose runner it is about and when.
	for _, event := range events {
		require.Equal(t, resp.RunnerID, event.key)
		require.Equal(t, "0x00000000000000000000000000000000000000AA", event.fields["orch_address"], event.eventType)
		require.Equal(t, resp.RunnerID, event.fields["runner_id"], event.eventType)
		require.Equal(t, "m1", event.fields["member_id"], event.eventType)
		require.Equal(t, "vllm/qwen2.5-7b-instruct", event.fields["app"], event.eventType)
		require.GreaterOrEqual(t, event.fields["ts_ms"], started, event.eventType)
	}
	only := func(event recordedEvent) map[string]interface{} {
		fields := map[string]interface{}{}
		for k, v := range event.fields {
			switch k {
			case "orch_address", "runner_id", "member_id", "app", "ts_ms", "duration_ms":
			default:
				fields[k] = v
			}
		}
		return fields
	}

	require.Equal(t, map[string]interface{}{
		"payout_address": "0x00000000000000000000000000000000000000b1", "state": LiveRunnerStateProbation,
		"mode": LiveRunnerModeSingleShot, "capacity": 2, "version": "vllm-app:1.4.2",
		"remote_addr": "203.0.113.7", "tunnel": true, "static": false,
	}, only(events[0]))

	// The pool service's test is an unpaid session with no payer.
	require.Equal(t, map[string]interface{}{"session_id": probeID, "sender": "", "paid": false, "via": "runner"}, only(events[1]))
	require.Equal(t, map[string]interface{}{
		"session_id": probeID, "sender": "", "paid": false, "reason": "stopped", "units_total": int64(0), "fee_wei_total": "0",
	}, only(events[2]))
	require.Contains(t, events[2].fields, "duration_ms")

	require.Equal(t, map[string]interface{}{"from": LiveRunnerStateProbation, "to": LiveRunnerStateActive, "reason": "probe passed"}, only(events[3]))

	// A paid session: each debit, then totals that are their sum.
	require.Equal(t, map[string]interface{}{"session_id": "manifest-1", "sender": payer, "paid": true, "via": "runner"}, only(events[4]))
	for i, event := range events[5:8] {
		require.Equal(t, map[string]interface{}{
			"session_id": "manifest-1", "sender": payer, "units": int64(5), "fee_wei": "100", "balance_wei": big.NewRat(int64(1000-100*(i+1)), 1).FloatString(0),
		}, only(event))
	}
	require.Equal(t, map[string]interface{}{
		"session_id": "manifest-1", "sender": payer, "paid": true, "reason": "completed", "units_total": int64(15), "fee_wei_total": "300",
	}, only(events[8]))

	// Eviction releases what was running, then removes the runner.
	require.Equal(t, lastID, events[10].fields["session_id"])
	require.Equal(t, "runner_removed", events[10].fields["reason"])
	require.Equal(t, map[string]interface{}{"reason": "evicted", "evict_reason": "returned garbage"}, only(events[11]))
}

func TestRemovedEventSaysWhy(t *testing.T) {
	registry, recorder := newEventTestRegistry(t, false, &fakeAdmitter{answer: admitAs("m1", LiveRunnerStateActive, 4)})

	left := poolTestRegister(t, registry, "c1")
	require.NoError(t, registry.Unregister(left.RunnerID, left.HeartbeatSecret))
	lapsed := poolTestRegister(t, registry, "c1")
	registry.removeExpiredRunners(time.Now().Add(time.Hour))

	events := recorder.wait(t, 4)
	require.Equal(t, []string{LiveRunnerEventRegistered, LiveRunnerEventRemoved, LiveRunnerEventRegistered, LiveRunnerEventRemoved}, eventTypes(events))
	require.Equal(t, left.RunnerID, events[1].fields["runner_id"])
	require.Equal(t, "unregistered", events[1].fields["reason"])
	require.NotContains(t, events[1].fields, "evict_reason")
	require.Equal(t, lapsed.RunnerID, events[3].fields["runner_id"])
	require.Equal(t, "expired", events[3].fields["reason"])
}

func TestEventsReportRunnersOutsideThePoolUnderTheOperator(t *testing.T) {
	healthSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer healthSrv.Close()
	registry, recorder := newEventTestRegistry(t, false, nil)

	liveRunnerTestRegister(t, registry, liveRunnerTestHeartbeat("own-runner"))
	static, err := registry.RegisterStaticRunners(StaticLiveRunnerConfig{Runners: []StaticLiveRunnerConfigEntry{{
		Label: "own-box", RunnerURL: "https://runner.example.com", App: "echo", HealthURL: healthSrv.URL,
	}}})
	require.NoError(t, err)
	// Re-registering the same static config is not a new registration.
	_, err = registry.RegisterStaticRunners(StaticLiveRunnerConfig{Runners: []StaticLiveRunnerConfigEntry{{
		Label: "own-box", RunnerURL: "https://runner.example.com", App: "echo", HealthURL: healthSrv.URL,
	}}})
	require.NoError(t, err)
	_, _, err = registry.ReserveSession("own-runner")
	require.NoError(t, err)

	events := recorder.wait(t, 3)
	require.Equal(t, []string{LiveRunnerEventRegistered, LiveRunnerEventRegistered, LiveRunnerEventSessionReserved}, eventTypes(events))
	require.Equal(t, "own-runner", events[0].fields["runner_id"])
	require.Equal(t, "operator", events[0].fields["member_id"])
	require.Equal(t, LiveRunnerStateActive, events[0].fields["state"])
	require.Equal(t, false, events[0].fields["static"])
	require.Equal(t, static.Runners[0].RunnerID, events[1].fields["runner_id"])
	require.Equal(t, "operator", events[1].fields["member_id"])
	require.Equal(t, true, events[1].fields["static"])
	require.Equal(t, "operator", events[2].fields["member_id"])
}

func TestEventsCarryThePriceASessionWasReservedAt(t *testing.T) {
	registry, recorder := newEventTestRegistry(t, true, &fakeAdmitter{answer: admitAs("m1", LiveRunnerStateActive, 1)})
	resp := poolTestRegister(t, registry, "c1")
	_, _, err := registry.ReservePaidSession(resp.RunnerID, "manifest-1", "0x00000000000000000000000000000000000000c1")
	require.NoError(t, err)

	events := recorder.wait(t, 2)
	price := &LiveRunnerPriceInfo{Price: "10", Currency: "usd", Unit: "hour"}
	require.Equal(t, price, events[0].fields["price_info"])
	require.Equal(t, price, events[1].fields["price_info"])
}

func TestSessionFeeTotalsAreSummedExactlyAndRoundedOnce(t *testing.T) {
	registry, recorder := newEventTestRegistry(t, false, &fakeAdmitter{answer: admitAs("m1", LiveRunnerStateActive, 1)})
	resp := poolTestRegister(t, registry, "c1")
	sessionID, _, err := registry.ReservePaidSession(resp.RunnerID, "manifest-1", "0xc1")
	require.NoError(t, err)

	// Three debits of a third of a wei are one wei, not three times zero.
	for i := 0; i < 3; i++ {
		require.NoError(t, registry.RecordSessionFee(resp.RunnerID, sessionID, "0xc1", 1, big.NewRat(1, 3), nil))
	}
	require.NoError(t, registry.ReleaseSession(resp.RunnerID, sessionID))

	events := recorder.wait(t, 6)
	require.Equal(t, "0", events[2].fields["fee_wei"])
	require.Equal(t, "0", events[2].fields["balance_wei"], "an unknown balance is reported as zero")
	require.Equal(t, "1", events[5].fields["fee_wei_total"])
	require.Equal(t, int64(3), events[5].fields["units_total"])
}

func TestFeeDebitedAfterItsSessionWasReleasedIsStillReported(t *testing.T) {
	registry, recorder := newEventTestRegistry(t, false, &fakeAdmitter{answer: admitAs("m1", LiveRunnerStateActive, 1)})
	resp := poolTestRegister(t, registry, "c1")
	sessionID, _, err := registry.ReservePaidSession(resp.RunnerID, "manifest-1", "0xc1")
	require.NoError(t, err)
	require.NoError(t, registry.ReleaseSession(resp.RunnerID, sessionID))

	require.NoError(t, registry.RecordSessionFee(resp.RunnerID, sessionID, "0xc1", 2, big.NewRat(7, 1), big.NewRat(1, 1)))
	events := recorder.wait(t, 4)
	require.Equal(t, LiveRunnerEventFeeDebited, events[3].eventType)
	require.Equal(t, "7", events[3].fields["fee_wei"])
	require.Equal(t, "m1", events[3].fields["member_id"])

	// A runner that is gone cannot be attributed; the caller logs the debit.
	require.Error(t, registry.RecordSessionFee("runner_missing", sessionID, "0xc1", 2, big.NewRat(7, 1), nil))
}

func TestEmittingNeverBlocksTheRegistry(t *testing.T) {
	recorder := &eventRecorder{block: make(chan struct{})}
	registry := newLiveRunnerTestRegistryWithConfig(LiveRunnerRegistryConfig{
		Host: liveRunnerTestHost{}, Admitter: &fakeAdmitter{}, Prices: poolTestPrices(), Events: recorder.sink,
	})
	t.Cleanup(registry.Stop)
	resp := poolTestRegister(t, registry, "c1")

	// The sink is stuck. Far more events than the queue holds must not stall
	// the registry, which emits under its locks.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < liveRunnerEventQueueSize+100; i++ {
			state := LiveRunnerStateActive
			if i%2 == 0 {
				state = LiveRunnerStateSuspended
			}
			_ = registry.SetRunnerState(resp.RunnerID, state, "")
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("emitting blocked on a stuck sink")
	}
	require.Len(t, registry.PoolRunners(), 1)
	close(recorder.block)
}

func TestWithoutASinkNoEventsAreQueued(t *testing.T) {
	registry := newPoolTestRegistry(t, &fakeAdmitter{})
	require.Nil(t, registry.events)
	resp := poolTestRegister(t, registry, "c1")
	require.NoError(t, registry.SetRunnerState(resp.RunnerID, LiveRunnerStateActive, ""))
	sessionID, _, err := registry.ReservePaidSession(resp.RunnerID, "manifest-1", "0xc1")
	require.NoError(t, err)
	require.NoError(t, registry.RecordSessionFee(resp.RunnerID, sessionID, "0xc1", 1, big.NewRat(1, 1), nil))
	require.NoError(t, registry.EvictRunner(resp.RunnerID, ""))
}

func TestEventsAreStampedWithTheCurrentRound(t *testing.T) {
	recorder := &eventRecorder{}
	round := int64(4100)
	var roundMu sync.Mutex
	registry := newLiveRunnerTestRegistryWithConfig(LiveRunnerRegistryConfig{
		Host: liveRunnerTestHost{}, Admitter: &fakeAdmitter{answer: admitAs("m1", LiveRunnerStateActive, 4)},
		Prices: poolTestPrices(), Events: recorder.sink,
		Round: func() int64 {
			roundMu.Lock()
			defer roundMu.Unlock()
			return round
		},
	})
	t.Cleanup(registry.Stop)

	resp := poolTestRegister(t, registry, "c1")
	sessionID, _, err := registry.ReservePaidSession(resp.RunnerID, "manifest-1", "0xc1")
	require.NoError(t, err)
	require.NoError(t, registry.RecordSessionFee(resp.RunnerID, sessionID, "0xc1", 1, big.NewRat(5, 1), nil))
	// The round turns over while the session runs: each event carries the
	// round it happened in, which is what a pool counts its windows by.
	roundMu.Lock()
	round = 4101
	roundMu.Unlock()
	require.NoError(t, registry.RecordSessionFee(resp.RunnerID, sessionID, "0xc1", 1, big.NewRat(5, 1), nil))
	require.NoError(t, registry.ReleaseSession(resp.RunnerID, sessionID))

	events := recorder.wait(t, 5)
	rounds := []interface{}{}
	for _, event := range events {
		rounds = append(rounds, event.fields["round"])
	}
	require.Equal(t, []interface{}{int64(4100), int64(4100), int64(4100), int64(4101), int64(4101)}, rounds)
}

func TestEventsCarryNoRoundOffchain(t *testing.T) {
	registry, recorder := newEventTestRegistry(t, false, &fakeAdmitter{})
	poolTestRegister(t, registry, "c1")
	require.NotContains(t, recorder.wait(t, 1)[0].fields, "round")
}

func TestRefusedRequestsAreCountedPerApp(t *testing.T) {
	registry, recorder := newEventTestRegistry(t, false, &fakeAdmitter{answer: admitAs("m1", LiveRunnerStateActive, 4)})
	req := poolTestHeartbeat()
	req.Capacity = 1
	resp, err := registry.HeartbeatFrom(req, "c1", "")
	require.NoError(t, err)
	attachPipeTunnel(t, registry, resp.RunnerID, resp.HeartbeatSecret)
	_, _, err = registry.ReserveSession(resp.RunnerID)
	require.NoError(t, err)

	// The runner is full. Customers asking for it by id, or for its app, are turned away.
	_, _, err = registry.ReserveSession(resp.RunnerID)
	requireRunnerError(t, err, http.StatusServiceUnavailable, "no capacity available for runner")
	_, _, err = registry.ReservePaidSession(resp.RunnerID, "manifest-1", "0xc1")
	requireRunnerError(t, err, http.StatusServiceUnavailable, "no capacity available for runner")
	_, _, _, err = registry.ReservePoolSession(routingTestApp, LiveRunnerModeSingleShot)
	requireRunnerError(t, err, http.StatusServiceUnavailable, "no capacity for app")
	_, err = registry.PoolPaymentInfo(routingTestApp, LiveRunnerModeSingleShot)
	requireRunnerError(t, err, http.StatusServiceUnavailable, "no capacity for app")
	// The pool service's own probe is not a customer and is not counted.
	_, _, err = registry.ReserveProbeSession(resp.RunnerID)
	requireRunnerError(t, err, http.StatusServiceUnavailable, "no capacity available for runner")
	// Neither is a request for an app that is not offered at all.
	_, _, _, err = registry.ReservePoolSession("flux/schnell", LiveRunnerModeSingleShot)
	requireRunnerError(t, err, http.StatusNotFound, "app not offered")

	registry.reportRefusals()
	events := recorder.wait(t, 3)
	refused := events[2]
	require.Equal(t, LiveRunnerEventRequestsRefused, refused.eventType)
	require.Equal(t, routingTestApp, refused.key)
	require.Equal(t, routingTestApp, refused.fields["app"])
	require.Equal(t, 4, refused.fields["count"])
	require.NotContains(t, refused.fields, "runner_id", "the count is about an app, not a runner")

	// Nothing refused since: nothing reported.
	registry.reportRefusals()
	require.NoError(t, registry.SetRunnerState(resp.RunnerID, LiveRunnerStateSuspended, ""))
	require.Equal(t, LiveRunnerEventStateChanged, recorder.wait(t, 4)[3].eventType)
}
