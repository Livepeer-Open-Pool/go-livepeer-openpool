package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/livepeer/go-livepeer/ai/runner"
	"github.com/livepeer/go-livepeer/clog"
)

// App-level routes. A customer asks for an app under /pool/{app}/… and the
// orchestrator picks the runner; {app} is the app name, base64url-encoded
// without padding. Everything after a reservation uses the chosen runner's
// /apps/{runner_id}/… routes, except follow-up payments, which arrive at the
// payment URL the challenge named before any runner was chosen.

func (lp *lphttp) registerLiveRunnerPoolRoutes() {
	prefix := "/" + runner.LiveRunnerPoolRoute + "/{app}"
	lp.transRPC.HandleFunc("POST "+prefix+"/session", lp.ReservePoolLiveRunnerSession)
	lp.transRPC.HandleFunc("POST "+prefix+"/session/{session_id}/payment", lp.PaymentForPoolLiveRunnerSession)
	lp.transRPC.HandleFunc(prefix+"/app/{app_path...}", lp.ProxyPoolLiveRunnerSingleShot)
}

// liveRunnerPoolTarget says a request asks for an app rather than a runner.
type liveRunnerPoolTarget struct {
	app  string // the app name
	mode string // the runner mode the route serves
}

type liveRunnerPoolTargetKey struct{}

func withLiveRunnerPoolTarget(r *http.Request, target liveRunnerPoolTarget) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), liveRunnerPoolTargetKey{}, target))
}

func liveRunnerPoolTargetOf(r *http.Request) (liveRunnerPoolTarget, bool) {
	target, ok := r.Context().Value(liveRunnerPoolTargetKey{}).(liveRunnerPoolTarget)
	return target, ok
}

// reservePaidLiveRunnerSessionOn reserves the paid session on the runner the
// request names, or on a runner the registry picks when the request asks for
// an app. It returns the runner the session is on.
func reservePaidLiveRunnerSessionOn(r *http.Request, manager liveRunnerManager, runnerID, sessionID, sender string) (string, string, string, error) {
	if target, ok := liveRunnerPoolTargetOf(r); ok {
		return manager.ReservePaidPoolSession(target.app, target.mode, sessionID, sender)
	}
	sessionID, appURL, err := manager.ReservePaidSession(runnerID, sessionID, sender)
	return runnerID, sessionID, appURL, err
}

// liveRunnerPaymentURL is where a customer sends follow-up payments for the
// session a challenge is about. For a request by app no runner is chosen yet,
// so the URL names the app and the session.
func (h *lphttp) liveRunnerPaymentURL(r *http.Request, sessionID string) string {
	if _, ok := liveRunnerPoolTargetOf(r); ok {
		return h.orchestrator.ServiceURI().JoinPath(runner.LiveRunnerPoolRoute, r.PathValue("app"), "session", sessionID, "payment").String()
	}
	return h.orchestrator.ServiceURI().JoinPath("apps", r.PathValue("runner_id"), "session", sessionID, "payment").String()
}

// poolLiveRunnerRequest reads what a pool route was asked for. A probe is
// refused here: the pool service tests one runner, which it names.
func (h *lphttp) poolLiveRunnerRequest(w http.ResponseWriter, r *http.Request, mode string) (liveRunnerManager, *http.Request, liveRunnerPoolTarget, bool) {
	manager, ok := h.liveRunnerManager()
	if !ok {
		respondWithError(w, "live runners are not supported", http.StatusNotFound)
		return nil, nil, liveRunnerPoolTarget{}, false
	}
	if _, probe := r.Header[liveRunnerProbeHeader]; probe {
		respondWithError(w, "a probe names its runner", http.StatusBadRequest)
		return nil, nil, liveRunnerPoolTarget{}, false
	}
	app, err := runner.DecodeLiveRunnerApp(r.PathValue("app"))
	if err != nil {
		respondWithLiveRunnerError(w, err)
		return nil, nil, liveRunnerPoolTarget{}, false
	}
	target := liveRunnerPoolTarget{app: app, mode: mode}
	return manager, withLiveRunnerPoolTarget(r, target), target, true
}

// ReservePoolLiveRunnerSession reserves a persistent session on a runner of
// the app, chosen by the orchestrator.
func (h *lphttp) ReservePoolLiveRunnerSession(w http.ResponseWriter, r *http.Request) {
	manager, r, target, ok := h.poolLiveRunnerRequest(w, r, runner.LiveRunnerModePersistent)
	if !ok {
		return
	}
	ctx := clog.AddVal(context.Background(), "app", target.app)
	priceInfo, err := manager.PoolPaymentInfo(target.app, target.mode)
	if err != nil {
		respondWithLiveRunnerError(w, err)
		return
	}
	var runnerID, sessionID, appURL string
	if priceInfo == nil {
		runnerID, sessionID, appURL, err = manager.ReservePoolSession(target.app, target.mode)
		if err != nil {
			respondWithLiveRunnerError(w, err)
			return
		}
	} else {
		var reserved bool
		sessionID, appURL, reserved = h.reservePaidLiveRunnerSession(ctx, w, r, manager, "", priceInfo, nil)
		if !reserved {
			return
		}
		if runnerID, err = manager.RunnerForPoolSession(target.app, sessionID); err != nil {
			respondWithLiveRunnerError(w, err)
			return
		}
	}
	controlURL := h.orchestrator.ServiceURI().JoinPath("apps", runnerID, "session", sessionID).String()
	data, err := json.Marshal(liveRunnerSessionResponse{SessionID: sessionID, AppURL: appURL, ControlURL: controlURL, RunnerID: runnerID})
	if err != nil {
		respondWithError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	respondJsonOk(w, data)
}

// PaymentForPoolLiveRunnerSession takes a follow-up payment sent to the
// payment URL of a challenge issued for an app.
func (h *lphttp) PaymentForPoolLiveRunnerSession(w http.ResponseWriter, r *http.Request) {
	manager, ok := h.liveRunnerManager()
	if !ok {
		respondWithError(w, "live runners are not supported", http.StatusNotFound)
		return
	}
	app, err := runner.DecodeLiveRunnerApp(r.PathValue("app"))
	if err != nil {
		respondWithLiveRunnerError(w, err)
		return
	}
	runnerID, err := manager.RunnerForPoolSession(app, r.PathValue("session_id"))
	if err != nil {
		respondWithLiveRunnerError(w, err)
		return
	}
	r.SetPathValue("runner_id", runnerID)
	h.PaymentForLiveRunnerSession(w, r)
}

// ProxyPoolLiveRunnerSingleShot serves one request on a single-shot runner of
// the app, chosen by the orchestrator for this request.
func (h *lphttp) ProxyPoolLiveRunnerSingleShot(w http.ResponseWriter, r *http.Request) {
	manager, r, target, ok := h.poolLiveRunnerRequest(w, r, runner.LiveRunnerModeSingleShot)
	if !ok {
		return
	}
	priceInfo, err := manager.PoolPaymentInfo(target.app, target.mode)
	if err != nil {
		respondWithLiveRunnerError(w, err)
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	var runnerID, sessionID, endpoint string
	if priceInfo == nil {
		runnerID, sessionID, endpoint, err = manager.ReservePoolSession(target.app, target.mode)
		if err != nil {
			respondWithLiveRunnerError(w, err)
			return
		}
	} else {
		var reserved bool
		sessionID, endpoint, reserved = h.reservePaidLiveRunnerSession(ctx, w, r, manager, "", priceInfo, cancel)
		if !reserved {
			return
		}
		if runnerID, err = manager.RunnerForPoolSession(target.app, sessionID); err != nil {
			// The session ended between being reserved and being used, for
			// instance because its runner left.
			respondWithLiveRunnerError(w, err)
			return
		}
	}
	defer func() {
		if err := manager.ReleaseSessionWithReason(runnerID, sessionID, runner.LiveRunnerReleaseCompleted); err != nil {
			slog.Error("error releasing single-shot session", "runner_id", runnerID, "session_id", sessionID, "err", err)
		}
	}()

	sessionToken, err := manager.SessionTokenForSession(runnerID, sessionID)
	if err != nil {
		respondWithLiveRunnerError(w, err)
		return
	}
	h.proxyLiveRunner(w, r.Clone(ctx), runnerID, sessionID, sessionToken, endpoint, r.PathValue("app_path"))
}
