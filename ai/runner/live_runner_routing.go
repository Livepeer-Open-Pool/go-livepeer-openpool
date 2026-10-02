package runner

import (
	"encoding/base64"
	"errors"
	"math/rand"
	"net/http"
	"sort"
	"time"
)

// App-level routing: a customer asks for an app and the registry picks the
// runner. It exists for pools, where many members' runners serve the same
// app at the price the operator set, so only apps in the price table are
// routable.

// LiveRunnerPoolRoute is the path prefix of the app-level routes; the
// segment after it is the app name, base64url-encoded without padding
// because app names contain slashes.
const LiveRunnerPoolRoute = "pool"

// LiveRunnerDiscoveryApp is an app customers can ask for by name: the active,
// ready runners of one app and mode taken together.
type LiveRunnerDiscoveryApp struct {
	App               string               `json:"app"`
	URL               string               `json:"url"`
	Mode              string               `json:"mode"`
	PriceInfo         *LiveRunnerPriceInfo `json:"price_info,omitempty"`
	Runners           int                  `json:"runners"`
	Capacity          int                  `json:"capacity"`
	CapacityAvailable int                  `json:"capacity_available"`
}

var liveRunnerAppEncoding = base64.RawURLEncoding

type poolCandidate struct {
	route  string
	runner *liveRunner
	used   int
}

func (r *LiveRunnerRegistry) appOffered(app string) error {
	if _, ok := r.configuredPrice(app); !ok {
		return &RunnerError{StatusCode: http.StatusNotFound, Message: "app not offered"}
	}
	return nil
}

// poolCandidates are the runners that could take a new session for the app
// right now: active, ready, of the route's mode, with a free slot.
func (r *LiveRunnerRegistry) poolCandidates(app, mode string, exclude map[*liveRunner]bool) []poolCandidate {
	r.mu.Lock()
	heartbeatTTL := r.heartbeatTTL
	entries := make([]poolCandidate, 0, len(r.runners))
	for route, runner := range r.runners {
		if !exclude[runner] {
			entries = append(entries, poolCandidate{route: route, runner: runner})
		}
	}
	r.mu.Unlock()

	candidates := entries[:0]
	now := time.Now()
	for _, c := range entries {
		c.runner.mu.Lock()
		eligible := !c.runner.removed && !liveRunnerExpiredLocked(c.runner, now, heartbeatTTL) &&
			c.runner.readyLocked() && c.runner.stateLocked() == LiveRunnerStateActive &&
			c.runner.App == app && c.runner.Mode == mode && len(c.runner.sessions) < c.runner.Capacity
		c.used = len(c.runner.sessions)
		c.runner.mu.Unlock()
		if eligible {
			candidates = append(candidates, c)
		}
	}
	return candidates
}

// pickPoolCandidate takes the runner with the fewest sessions, choosing
// uniformly among equals so that idle runners share the work.
func pickPoolCandidate(candidates []poolCandidate) poolCandidate {
	least := candidates[0].used
	for _, c := range candidates[1:] {
		if c.used < least {
			least = c.used
		}
	}
	idlest := make([]poolCandidate, 0, len(candidates))
	for _, c := range candidates {
		if c.used == least {
			idlest = append(idlest, c)
		}
	}
	return idlest[rand.Intn(len(idlest))]
}

// reservePoolSession picks a runner of the app and reserves a session on it.
// The reservation itself re-checks the runner under its lock, so two requests
// cannot take the same last slot: the loser moves on to the next runner.
func (r *LiveRunnerRegistry) reservePoolSession(app, mode string, opts liveRunnerSessionOptions, optSessionID ...string) (string, string, string, error) {
	if err := r.appOffered(app); err != nil {
		return "", "", "", err
	}
	opts.via = LiveRunnerViaPool
	exclude := map[*liveRunner]bool{}
	for {
		candidates := r.poolCandidates(app, mode, exclude)
		if len(candidates) == 0 {
			r.noteRefusal(app)
			return "", "", "", &RunnerError{StatusCode: http.StatusServiceUnavailable, Message: "no capacity for app"}
		}
		chosen := pickPoolCandidate(candidates)
		sessionID, appURL, err := r.reserveSession(chosen.route, opts, optSessionID...)
		if err == nil {
			return chosen.route, sessionID, appURL, nil
		}
		// The runner filled up, left, or was suspended since it was picked.
		var runnerErr *RunnerError
		if errors.As(err, &runnerErr) && (runnerErr.StatusCode == http.StatusServiceUnavailable ||
			runnerErr.StatusCode == http.StatusNotFound || runnerErr.Message == "runner suspended") {
			exclude[chosen.runner] = true
			continue
		}
		return "", "", "", err
	}
}

// ReservePoolSession reserves an unpaid session on a runner of the app and
// returns the runner chosen, the session id, and the URL for the app request.
func (r *LiveRunnerRegistry) ReservePoolSession(app, mode string) (string, string, string, error) {
	return r.reservePoolSession(app, mode, liveRunnerSessionOptions{})
}

// ReservePaidPoolSession is ReservePoolSession for a session a customer pays for.
func (r *LiveRunnerRegistry) ReservePaidPoolSession(app, mode, sessionID, sender string) (string, string, string, error) {
	return r.reservePoolSession(app, mode, liveRunnerSessionOptions{sender: sender}, sessionID)
}

// PoolPaymentInfo is the price a customer is challenged with for the app,
// nil when sessions are not paid for. Every runner of an offered app has the
// operator's price, so any runner that could serve the request answers it.
func (r *LiveRunnerRegistry) PoolPaymentInfo(app, mode string) (*LiveRunnerPriceInfo, error) {
	if err := r.appOffered(app); err != nil {
		return nil, err
	}
	for _, c := range r.poolCandidates(app, mode, nil) {
		priceInfo, err := r.PaymentInfo(c.route)
		if err == nil {
			return priceInfo, nil
		}
	}
	r.noteRefusal(app)
	return nil, &RunnerError{StatusCode: http.StatusServiceUnavailable, Message: "no capacity for app"}
}

// RunnerForPoolSession finds the runner of the app that holds the session,
// for the requests that follow a reservation made by app name.
func (r *LiveRunnerRegistry) RunnerForPoolSession(app, sessionID string) (string, error) {
	r.mu.Lock()
	entries := make([]poolCandidate, 0, len(r.runners))
	for route, runner := range r.runners {
		entries = append(entries, poolCandidate{route: route, runner: runner})
	}
	r.mu.Unlock()
	for _, c := range entries {
		c.runner.mu.Lock()
		_, holds := c.runner.sessions[sessionID]
		found := holds && !c.runner.removed && c.runner.App == app
		c.runner.mu.Unlock()
		if found {
			return c.route, nil
		}
	}
	return "", &RunnerError{StatusCode: http.StatusNotFound, Message: "runner session not found"}
}

// PoolAppURL is the public URL customers use to ask for the app.
func (r *LiveRunnerRegistry) PoolAppURL(app, mode string) string {
	last := "session"
	if mode == LiveRunnerModeSingleShot {
		last = "app"
	}
	return r.host.ServiceURI().JoinPath(LiveRunnerPoolRoute, EncodeLiveRunnerApp(app), last).String()
}

// PoolApps lists the offered apps that have at least one runner taking work,
// one entry per app and mode.
func (r *LiveRunnerRegistry) PoolApps() []LiveRunnerDiscoveryApp {
	offered := r.Prices().Apps
	if len(offered) == 0 {
		return nil
	}
	r.mu.Lock()
	heartbeatTTL := r.heartbeatTTL
	snapshot := make([]*liveRunner, 0, len(r.runners))
	for _, runner := range r.runners {
		snapshot = append(snapshot, runner)
	}
	r.mu.Unlock()

	type appMode struct{ app, mode string }
	apps := map[appMode]*LiveRunnerDiscoveryApp{}
	now := time.Now()
	for _, runner := range snapshot {
		runner.mu.Lock()
		if _, ok := offered[runner.App]; ok && !runner.removed && !liveRunnerExpiredLocked(runner, now, heartbeatTTL) &&
			runner.readyLocked() && runner.stateLocked() == LiveRunnerStateActive {
			key := appMode{runner.App, runner.Mode}
			entry := apps[key]
			if entry == nil {
				entry = &LiveRunnerDiscoveryApp{App: runner.App, Mode: runner.Mode, URL: r.PoolAppURL(runner.App, runner.Mode)}
				apps[key] = entry
			}
			discovered := runner.discoveryRunner()
			if entry.PriceInfo == nil {
				entry.PriceInfo = discovered.PriceInfo
			}
			entry.Runners++
			entry.Capacity += discovered.Capacity
			entry.CapacityAvailable += discovered.CapacityAvailable
		}
		runner.mu.Unlock()
	}
	out := make([]LiveRunnerDiscoveryApp, 0, len(apps))
	for _, entry := range apps {
		out = append(out, *entry)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].App != out[j].App {
			return out[i].App < out[j].App
		}
		return out[i].Mode < out[j].Mode
	})
	return out
}

// EncodeLiveRunnerApp is the app name as it appears in a pool route.
func EncodeLiveRunnerApp(app string) string {
	return liveRunnerAppEncoding.EncodeToString([]byte(app))
}

// DecodeLiveRunnerApp reads the app name out of a pool route's segment.
func DecodeLiveRunnerApp(segment string) (string, error) {
	app, err := liveRunnerAppEncoding.DecodeString(segment)
	if err != nil || len(app) == 0 {
		return "", &RunnerError{StatusCode: http.StatusBadRequest, Message: "app must be base64url without padding"}
	}
	return string(app), nil
}
