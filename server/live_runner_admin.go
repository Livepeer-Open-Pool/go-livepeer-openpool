package server

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/livepeer/go-livepeer/ai/runner"
)

// The pool service drives the Live Runner registry through these CLI-port
// endpoints: it lists every runner, moves runners between states after
// testing them, evicts runners and members, and sets the prices. Like the
// rest of the CLI port they carry no authentication, so the port must only
// be reachable by the operator and the pool service.

type liveRunnerAdmin interface {
	PoolRunners() []runner.LiveRunnerPoolRunner
	SetRunnerState(runnerID, state, reason string) error
	EvictRunner(runnerID, reason string) error
	EvictMember(memberID, reason string) (int, error)
	SetPricesJSON(data []byte) error
}

type liveRunnerPoolRunnersResponse struct {
	Runners []runner.LiveRunnerPoolRunner `json:"runners"`
}

func (s *LivepeerServer) registerLiveRunnerAdminRoutes(mux *http.ServeMux) {
	mux.Handle("GET /liveRunner/runners", s.liveRunnerAdminHandler(func(admin liveRunnerAdmin, w http.ResponseWriter, r *http.Request) error {
		data, err := json.Marshal(liveRunnerPoolRunnersResponse{Runners: admin.PoolRunners()})
		if err != nil {
			return err
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(data)
		return nil
	}))
	mux.Handle("POST /liveRunner/setState", mustHaveFormParams(s.liveRunnerAdminHandler(func(admin liveRunnerAdmin, w http.ResponseWriter, r *http.Request) error {
		if err := admin.SetRunnerState(r.PostFormValue("runner_id"), r.PostFormValue("state"), r.PostFormValue("reason")); err != nil {
			return err
		}
		w.WriteHeader(http.StatusNoContent)
		return nil
	}), "runner_id", "state"))
	mux.Handle("POST /liveRunner/evict", mustHaveFormParams(s.liveRunnerAdminHandler(func(admin liveRunnerAdmin, w http.ResponseWriter, r *http.Request) error {
		runnerID := strings.TrimSpace(r.PostFormValue("runner_id"))
		memberID := strings.TrimSpace(r.PostFormValue("member_id"))
		reason := r.PostFormValue("reason")
		var err error
		switch {
		case (runnerID == "") == (memberID == ""):
			err = &runner.RunnerError{StatusCode: http.StatusBadRequest, Message: "give runner_id or member_id, not both"}
		case runnerID != "":
			err = admin.EvictRunner(runnerID, reason)
		default:
			_, err = admin.EvictMember(memberID, reason)
		}
		if err != nil {
			return err
		}
		w.WriteHeader(http.StatusNoContent)
		return nil
	})))
	mux.Handle("POST /liveRunner/prices", s.liveRunnerAdminHandler(func(admin liveRunnerAdmin, w http.ResponseWriter, r *http.Request) error {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return &runner.RunnerError{StatusCode: http.StatusBadRequest, Message: err.Error()}
		}
		if err := admin.SetPricesJSON(body); err != nil {
			return err
		}
		w.WriteHeader(http.StatusNoContent)
		return nil
	}))
}

func (s *LivepeerServer) liveRunnerAdminHandler(handle func(liveRunnerAdmin, http.ResponseWriter, *http.Request) error) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		admin, ok := s.LivepeerNode.LiveRunnerManager.(liveRunnerAdmin)
		if !ok {
			http.Error(w, "live runners are not supported", http.StatusNotFound)
			return
		}
		if err := handle(admin, w, r); err != nil {
			statusCode := http.StatusInternalServerError
			if runnerErr, ok := err.(*runner.RunnerError); ok {
				statusCode = runnerErr.StatusCode
			}
			http.Error(w, err.Error(), statusCode)
		}
	})
}
