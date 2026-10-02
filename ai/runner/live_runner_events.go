package runner

import (
	"errors"
	"log/slog"
	"math/big"
	"net/http"
	"time"

	"github.com/livepeer/go-livepeer/monitor"
)

// The registry's events are the record a pool service settles from: which
// runner registered under which member, which sessions it served, and every
// fee it earned. They are emitted at the moment the thing happens, often
// under a lock, so emitting never blocks: events wait in a queue that one
// goroutine drains, in order, into the sink.

const (
	LiveRunnerEventRegistered      = "live_runner_registered"
	LiveRunnerEventStateChanged    = "live_runner_state_changed"
	LiveRunnerEventRemoved         = "live_runner_removed"
	LiveRunnerEventSessionReserved = "live_runner_session_reserved"
	LiveRunnerEventSessionReleased = "live_runner_session_released"
	LiveRunnerEventFeeDebited      = "live_runner_fee_debited"
	// LiveRunnerEventRequestsRefused counts, per app, the customer requests
	// answered 503 because no runner had a free slot: demand the pool could
	// not serve.
	LiveRunnerEventRequestsRefused = "live_runner_requests_refused"
)

// liveRunnerRefusalInterval is how often refused requests are reported. They
// are counted and reported together because an overloaded app refuses many
// requests a second.
const liveRunnerRefusalInterval = 10 * time.Second

// Why a session was released, as its released event reports it.
const (
	LiveRunnerReleaseCompleted     = "completed"        // a single-shot request finished
	LiveRunnerReleaseStopped       = "stopped"          // the customer or the runner stopped it
	LiveRunnerReleasePaymentFailed = "payment_failed"   // a payment was refused or the balance ran out
	LiveRunnerReleaseRunnerRemoved = "runner_removed"   // the runner left the registry
	LiveRunnerReleaseUnhealthy     = "runner_unhealthy" // a static runner failed its health check
	LiveRunnerReleaseExpired       = "expired"          // a probe session nobody stopped
)

// How a session was reached, as its reserved event reports it.
const (
	LiveRunnerViaRunner = "runner" // by the runner's id
	LiveRunnerViaPool   = "pool"   // by the app, the registry choosing the runner
)

const liveRunnerEventQueueSize = 4096

// LiveRunnerEventSink publishes one event. key orders events that share it.
// It is called from one goroutine, in the order the events happened, and may
// block; an error means the event was not published.
type LiveRunnerEventSink func(eventType, key string, fields map[string]interface{}) error

type liveRunnerEvent struct {
	eventType string
	key       string
	fields    map[string]interface{}
}

// liveRunnerSessionOptions says what kind of session is being reserved.
type liveRunnerSessionOptions struct {
	probe  bool   // unpaid, by the pool service, in any runner state
	sender string // the payer's address when the session is paid for
	via    string // how the customer reached the runner; LiveRunnerViaRunner when empty
}

func (r *LiveRunnerRegistry) eventLoop() {
	for {
		select {
		case event := <-r.events:
			r.publish(event)
		case <-r.stopRegistry:
			// Publish what is already queued, then stop.
			for {
				select {
				case event := <-r.events:
					r.publish(event)
				default:
					return
				}
			}
		}
	}
}

func (r *LiveRunnerRegistry) publish(event liveRunnerEvent) {
	if err := r.eventSink(event.eventType, event.key, event.fields); err != nil {
		// The event is part of the pool's accounting record: leave it in the log.
		slog.Error("live runner event not published", "event_type", event.eventType, "err", err, "event", event.fields)
	}
}

// emitLocked queues an event about the runner. It requires runner.mu and
// never blocks.
func (runner *liveRunner) emitLocked(eventType string, fields map[string]interface{}) {
	r := runner.registry
	if r == nil || r.events == nil {
		return
	}
	event := map[string]interface{}{
		"runner_id": runner.RunnerID,
		"member_id": runner.memberID(),
		"app":       runner.App,
	}
	for k, v := range fields {
		event[k] = v
	}
	r.emit(eventType, runner.RunnerID, event)
}

// emit queues an event, adding who emitted it and when: the orchestrator's
// address, the time, and the current round. It never blocks.
func (r *LiveRunnerRegistry) emit(eventType, key string, event map[string]interface{}) {
	if r.events == nil {
		return
	}
	event["orch_address"] = r.orchAddress
	event["ts_ms"] = time.Now().UnixMilli()
	if r.round != nil {
		if round := r.round(); round > 0 {
			event["round"] = round
		}
	}
	select {
	case r.events <- liveRunnerEvent{eventType: eventType, key: key, fields: event}:
	default:
		monitor.OrchestratorEventDropped(eventType)
		slog.Error("live runner event dropped, event queue full", "event_type", eventType, "event", event)
	}
}

// noteRefusal counts a customer request for the app that was refused for
// lack of capacity.
func (r *LiveRunnerRegistry) noteRefusal(app string) {
	if r.refusals == nil || app == "" {
		return
	}
	r.refusalsMu.Lock()
	r.refusals[app]++
	r.refusalsMu.Unlock()
}

// noteRunnerRefusal counts err against the runner's app when err says the
// runner had no free slot.
func (r *LiveRunnerRegistry) noteRunnerRefusal(runnerID string, err error) {
	if r.refusals == nil || !isNoCapacity(err) {
		return
	}
	runner, unlock, lockErr := r.lockLiveRunner(runnerID)
	if lockErr != nil {
		return
	}
	app := runner.App
	unlock()
	r.noteRefusal(app)
}

func isNoCapacity(err error) bool {
	var runnerErr *RunnerError
	return errors.As(err, &runnerErr) && runnerErr.StatusCode == http.StatusServiceUnavailable &&
		(runnerErr.Message == "no capacity available for runner" || runnerErr.Message == "no capacity for app")
}

func (r *LiveRunnerRegistry) refusalLoop() {
	ticker := time.NewTicker(liveRunnerRefusalInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			r.reportRefusals()
		case <-r.stopRegistry:
			r.reportRefusals()
			return
		}
	}
}

// reportRefusals emits one event per app that refused requests since the
// last report.
func (r *LiveRunnerRegistry) reportRefusals() {
	r.refusalsMu.Lock()
	refusals := r.refusals
	r.refusals = make(map[string]int)
	r.refusalsMu.Unlock()
	for app, count := range refusals {
		r.emit(LiveRunnerEventRequestsRefused, app, map[string]interface{}{"app": app, "count": count})
	}
}

func (runner *liveRunner) emitRegisteredLocked() {
	fields := map[string]interface{}{
		"payout_address": "",
		"state":          runner.stateLocked(),
		"mode":           runner.Mode,
		"capacity":       runner.Capacity,
		"version":        runner.Version,
		"remote_addr":    runner.remoteAddr,
		"tunnel":         runner.Tunnel,
		"static":         runner.static,
	}
	if runner.member != nil {
		fields["payout_address"] = runner.member.payoutAddress
	}
	if price := runner.eventPriceLocked(runner.PriceInfo); price != nil {
		fields["price_info"] = price
	}
	runner.emitLocked(LiveRunnerEventRegistered, fields)
}

// emitRemovedLocked reports the runner leaving the registry. reason is the
// registry's internal one.
func (runner *liveRunner) emitRemovedLocked(reason string) {
	fields := map[string]interface{}{"reason": reason}
	switch reason {
	case "unregister":
		fields["reason"] = "unregistered"
	case "heartbeat timeout":
		fields["reason"] = "expired"
	case "evicted":
		fields["evict_reason"] = runner.evictReason
	}
	runner.emitLocked(LiveRunnerEventRemoved, fields)
}

func (runner *liveRunner) emitSessionReservedLocked(sessionID string, session *liveRunnerSession) {
	fields := map[string]interface{}{
		"session_id": sessionID,
		"sender":     session.sender,
		"paid":       session.sender != "",
		"via":        session.via,
	}
	if price := runner.eventPriceLocked(session.priceInfo); price != nil {
		fields["price_info"] = price
	}
	runner.emitLocked(LiveRunnerEventSessionReserved, fields)
}

func (runner *liveRunner) emitSessionReleasedLocked(sessionID string, session *liveRunnerSession, reason string) {
	runner.emitLocked(LiveRunnerEventSessionReleased, map[string]interface{}{
		"session_id":    sessionID,
		"sender":        session.sender,
		"paid":          session.sender != "",
		"reason":        reason,
		"duration_ms":   time.Since(session.createdAt).Milliseconds(),
		"units_total":   session.units,
		"fee_wei_total": weiString(session.fee),
	})
}

// eventPriceLocked is the price an event reports: the operator's or the
// runner's USD price, and nothing when the runner is not priced.
func (runner *liveRunner) eventPriceLocked(priceInfo LiveRunnerPriceInfo) *LiveRunnerPriceInfo {
	if runner.offchain || priceInfo == (LiveRunnerPriceInfo{}) {
		return nil
	}
	return &priceInfo
}

// weiString renders an amount of wei as a whole number; a nil amount is zero.
func weiString(amount *big.Rat) string {
	if amount == nil {
		return "0"
	}
	return amount.FloatString(0)
}

// ReservePaidSession is ReserveSession for a session a customer pays for;
// sender is the payer's address, which the session's events carry.
func (r *LiveRunnerRegistry) ReservePaidSession(runnerID, sessionID, sender string) (string, string, error) {
	sessionID, appURL, err := r.reserveSession(runnerID, liveRunnerSessionOptions{sender: sender}, sessionID)
	r.noteRunnerRefusal(runnerID, err)
	return sessionID, appURL, err
}

// RecordSessionFee notes a fee debited for a session: it adds to the totals
// the session's released event reports and emits the fee event. balance is
// what the payer has left with the orchestrator for the session.
func (r *LiveRunnerRegistry) RecordSessionFee(runnerID, sessionID, sender string, units int64, fee, balance *big.Rat) error {
	runner, unlock, err := r.lockLiveRunner(runnerID)
	if err != nil {
		return err
	}
	defer unlock()
	if fee == nil {
		fee = new(big.Rat)
	}
	// A session released while its last debit was being accounted has already
	// reported its totals; the fee was still earned, so the event still goes out.
	if session := runner.sessions[sessionID]; session != nil {
		session.units += units
		if session.fee == nil {
			session.fee = new(big.Rat)
		}
		session.fee.Add(session.fee, fee)
	}
	runner.emitLocked(LiveRunnerEventFeeDebited, map[string]interface{}{
		"session_id":  sessionID,
		"sender":      sender,
		"units":       units,
		"fee_wei":     weiString(fee),
		"balance_wei": weiString(balance),
	})
	return nil
}
