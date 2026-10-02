package server

import (
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"

	"github.com/livepeer/go-livepeer/ai/runner"
	"github.com/livepeer/go-livepeer/core"
	"github.com/livepeer/go-livepeer/eth"
)

type liveRunnerTestEvent struct {
	eventType string
	fields    map[string]interface{}
}

// liveRunnerEventLog is the registry's event sink in these tests.
type liveRunnerEventLog struct {
	mu     sync.Mutex
	events []liveRunnerTestEvent
}

func (l *liveRunnerEventLog) sink(eventType, key string, fields map[string]interface{}) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, liveRunnerTestEvent{eventType, fields})
	return nil
}

func (l *liveRunnerEventLog) all() []liveRunnerTestEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]liveRunnerTestEvent(nil), l.events...)
}

func (l *liveRunnerEventLog) ofType(eventType string) []map[string]interface{} {
	out := []map[string]interface{}{}
	for _, event := range l.all() {
		if event.eventType == eventType {
			out = append(out, event.fields)
		}
	}
	return out
}

// newLiveRunnerHTTPOnchainWithEvents is newLiveRunnerHTTPOnchain with the
// registry's events recorded.
func newLiveRunnerHTTPOnchainWithEvents(t *testing.T) (*lphttp, *liveRunnerEventLog) {
	t.Helper()
	prevWatcher := core.PriceFeedWatcher
	core.PriceFeedWatcher = stubPriceFeedWatcher{price: eth.PriceData{Price: big.NewRat(2000, 1)}}
	t.Cleanup(func() { core.PriceFeedWatcher = prevWatcher })

	node, err := core.NewLivepeerNode(nil, t.TempDir(), nil)
	require.NoError(t, err)
	node.LivePaymentInterval = time.Second
	orch := newStubOrchestrator()
	orch.secret = "live-runner-secret"
	orch.ticketParams = defaultTicketParams()
	orch.balances = make(map[ethcommon.Address]map[core.ManifestID]*big.Rat)
	lp := &lphttp{
		orchestrator: orch,
		node:         node,
		transRPC:     http.NewServeMux(),
	}
	log := &liveRunnerEventLog{}
	manager := runner.NewLiveRunnerRegistry(runner.LiveRunnerRegistryConfig{
		Host:        testLiveRunnerHost{orchestrator: lp.orchestrator},
		Onchain:     true,
		OrchAddress: "0x00000000000000000000000000000000000000AA",
		Events:      log.sink,
	})
	t.Cleanup(manager.Stop)
	node.LiveRunnerManager = manager
	require.NoError(t, startAIServer(lp))
	return lp, log
}

func TestLiveRunnerPaidSessionEmitsEveryFeeAndItsTotal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lp, log := newLiveRunnerHTTPOnchainWithEvents(t)
		registerLiveRunnerForSession(t, lp, nil)
		manager := lp.node.LiveRunnerManager.(*runner.LiveRunnerRegistry)
		defer manager.Stop()
		orch := lp.orchestrator.(*stubOrchestrator)
		orch.paymentCredit = big.NewRat(5, 1)
		priceInfo := liveRunnerTestPricePerSecond(1)

		sessionID := reservePaidLiveRunnerSession(t, lp, "runner-1", priceInfo)
		synctest.Sleep(3 * time.Second)
		synctest.Wait()
		balance := orch.Balance(orch.Address(), core.ManifestID(sessionID))
		require.Equal(t, "2", balance.FloatString(0), "three accounting ticks debited one wei each")
		require.NoError(t, manager.ReleaseSession("runner-1", sessionID))
		synctest.Sleep(time.Second)
		synctest.Wait()

		events := log.all()
		types := []string{}
		for _, event := range events {
			types = append(types, event.eventType)
			require.Equal(t, "0x00000000000000000000000000000000000000AA", event.fields["orch_address"])
			require.Equal(t, "runner-1", event.fields["runner_id"])
			require.Equal(t, "operator", event.fields["member_id"])
		}
		require.Equal(t, []string{
			runner.LiveRunnerEventRegistered,
			runner.LiveRunnerEventSessionReserved,
			runner.LiveRunnerEventFeeDebited, runner.LiveRunnerEventFeeDebited, runner.LiveRunnerEventFeeDebited,
			runner.LiveRunnerEventSessionReleased,
		}, types)

		sender := orch.Address().Hex()
		reserved := events[1].fields
		require.Equal(t, sessionID, reserved["session_id"])
		require.Equal(t, sender, reserved["sender"])
		require.Equal(t, true, reserved["paid"])
		require.NotNil(t, reserved["price_info"])

		var unitsTotal int64
		for i, fee := range log.ofType(runner.LiveRunnerEventFeeDebited) {
			require.Equal(t, sessionID, fee["session_id"])
			require.Equal(t, sender, fee["sender"])
			require.Equal(t, "1", fee["fee_wei"], "the fee the balance was debited by")
			require.Equal(t, big.NewRat(int64(4-i), 1).FloatString(0), fee["balance_wei"])
			require.Equal(t, priceInfo.PixelsPerUnit, fee["units"])
			unitsTotal += fee["units"].(int64)
		}

		released := events[5].fields
		require.Equal(t, sessionID, released["session_id"])
		require.Equal(t, "stopped", released["reason"])
		require.Equal(t, "3", released["fee_wei_total"], "the sum of the fee events")
		require.Equal(t, unitsTotal, released["units_total"])
	})
}

func TestLiveRunnerFixedPriceSessionEmitsOneFee(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lp, log := newLiveRunnerHTTPOnchainWithEvents(t)
		registerLiveRunnerForSession(t, lp, &liveRunnerRegistrationOptions{
			PriceInfo: &runner.LiveRunnerPriceInfo{Price: json.Number("0.001"), Unit: "fixed"},
		})
		manager := lp.node.LiveRunnerManager.(*runner.LiveRunnerRegistry)
		defer manager.Stop()
		orch := lp.orchestrator.(*stubOrchestrator)

		challenge, oInfo := requestLiveRunnerPaymentChallenge(t, lp, "runner-1")
		price := oInfo.GetPriceInfo().GetPricePerUnit()
		orch.paymentCredit = big.NewRat(price, 1)
		headers := liveRunnerReservationPaymentHeadersWithPrice(t, orch, oInfo.GetAuthToken(), challenge.ManifestID, oInfo.GetPriceInfo())
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/apps/runner-1/session", nil)
		setRequestHeaders(req, headers)
		lp.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())

		synctest.Sleep(3 * time.Second)
		synctest.Wait()
		fees := log.ofType(runner.LiveRunnerEventFeeDebited)
		require.Len(t, fees, 1, "a fixed-price session is debited once, however long it runs")
		require.Equal(t, challenge.ManifestID, fees[0]["session_id"])
		require.Equal(t, int64(1), fees[0]["units"])
		require.Equal(t, big.NewRat(price, 1).FloatString(0), fees[0]["fee_wei"])
		require.Equal(t, "0", fees[0]["balance_wei"])
	})
}

func TestLiveRunnerSessionReleasedForNonPaymentSaysSo(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lp, log := newLiveRunnerHTTPOnchainWithEvents(t)
		registerLiveRunnerForSession(t, lp, nil)
		manager := lp.node.LiveRunnerManager.(*runner.LiveRunnerRegistry)
		defer manager.Stop()
		orch := lp.orchestrator.(*stubOrchestrator)
		orch.paymentCredit = big.NewRat(0, 1)

		sessionID := reservePaidLiveRunnerSession(t, lp, "runner-1", liveRunnerTestPricePerSecond(1))
		synctest.Sleep(2 * time.Second)
		synctest.Wait()

		require.Empty(t, log.ofType(runner.LiveRunnerEventFeeDebited), "nothing was debited")
		released := log.ofType(runner.LiveRunnerEventSessionReleased)
		require.Len(t, released, 1)
		require.Equal(t, sessionID, released[0]["session_id"])
		require.Equal(t, "payment_failed", released[0]["reason"])
		require.Equal(t, "0", released[0]["fee_wei_total"])
	})
}
