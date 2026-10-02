package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/Livepeer-Open-Pool/pool/internal/core"
	"github.com/Livepeer-Open-Pool/pool/internal/ledger"
)

// maxEventsBody bounds one upload of events.
const maxEventsBody = 32 << 20

// orchestratorEvents takes orchestrator events over HTTP: a JSON array of
// events, or one event per line, each exactly as it was published. The
// events normally arrive through Kafka; this is how events that were
// dropped there, and written whole to the orchestrator's log, are put back.
// Applying an event twice changes nothing.
func (s *Server) orchestratorEvents(w http.ResponseWriter, r *http.Request) {
	if !s.poolSecretOK(r) {
		writeJSON(w, http.StatusUnauthorized, errorBody{"the pool secret is missing or wrong"})
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxEventsBody))
	if err != nil {
		s.writeError(w, r, errBadRequest)
		return
	}
	var events []json.RawMessage
	if trimmed := bytes.TrimSpace(body); len(trimmed) > 0 && trimmed[0] == '[' {
		if err := json.Unmarshal(trimmed, &events); err != nil {
			s.writeError(w, r, errBadRequest)
			return
		}
	} else {
		for _, line := range bytes.Split(trimmed, []byte("\n")) {
			if line = bytes.TrimSpace(line); len(line) > 0 {
				events = append(events, line)
			}
		}
	}
	result := struct {
		Applied   int `json:"applied"`
		Malformed int `json:"malformed"`
	}{}
	for _, event := range events {
		switch err := s.ledger.Apply(r.Context(), event); {
		case err == nil:
			result.Applied++
		case errors.Is(err, ledger.ErrMalformed):
			result.Malformed++
			s.log.Warn("uploaded orchestrator event passed over", "err", err)
		default:
			s.writeError(w, r, err)
			return
		}
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) publicLeaderboard(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	body := struct {
		Region regionBody `json:"region"`
		// Window is "current" for the payout window still running, or "all".
		Window     string                    `json:"window"`
		FirstRound int64                     `json:"first_round,omitempty"`
		LastRound  int64                     `json:"last_round,omitempty"`
		Entries    []ledger.LeaderboardEntry `json:"entries"`
	}{Region: s.region(), Window: "all"}
	first := int64(0)
	if r.URL.Query().Get("window") == "current" {
		body.Window = "current"
		from, to, ok, err := s.payouts.OpenWindow(r.Context())
		if err != nil {
			s.writeError(w, r, err)
			return
		}
		if !ok {
			body.Entries = []ledger.LeaderboardEntry{}
			writeJSON(w, http.StatusOK, body)
			return
		}
		first, body.FirstRound, body.LastRound = from, from, to
	}
	entries, err := s.ledger.Leaderboard(r.Context(), first, limit)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	body.Entries = entries
	writeJSON(w, http.StatusOK, body)
}

func (s *Server) myEarnings(w http.ResponseWriter, r *http.Request, member *core.Member) {
	first, _ := strconv.ParseInt(r.URL.Query().Get("first_round"), 10, 64)
	last, _ := strconv.ParseInt(r.URL.Query().Get("last_round"), 10, 64)
	earnings, err := s.ledger.MemberEarnings(r.Context(), member.ID, first, last)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	body := struct {
		Earnings []ledger.Earning `json:"earnings"`
		// Window is the payout window still running, when there is one.
		Window *struct {
			FirstRound int64 `json:"first_round"`
			LastRound  int64 `json:"last_round"`
		} `json:"window,omitempty"`
	}{Earnings: earnings}
	if from, to, ok, err := s.payouts.OpenWindow(r.Context()); err != nil {
		s.writeError(w, r, err)
		return
	} else if ok {
		body.Window = &struct {
			FirstRound int64 `json:"first_round"`
			LastRound  int64 `json:"last_round"`
		}{from, to}
	}
	writeJSON(w, http.StatusOK, body)
}

func (s *Server) myPayouts(w http.ResponseWriter, r *http.Request, member *core.Member) {
	payouts, err := s.payouts.MemberPayouts(r.Context(), member.ID)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"payouts": payouts})
}

// appDemand is one line of what the operator looks at when deciding which
// app a machine should run.
type appDemand struct {
	App string `json:"app"`
	// RunnersOnline are the app's runners that are active and ready.
	RunnersOnline int `json:"runners_online"`
	SlotsBusy     int `json:"slots_busy"`
	SlotsTotal    int `json:"slots_total"`
	// Over the last 24 hours.
	Sessions int64  `json:"sessions_24h"`
	FeesWei  string `json:"fees_wei_24h"`
	Refused  int64  `json:"refused_24h"`
}

func (s *Server) adminDemand(w http.ResponseWriter, r *http.Request, actor string) {
	apps, err := s.core.Apps(r.Context(), false)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	lines := map[string]*appDemand{}
	for _, a := range apps {
		lines[a.Name] = &appDemand{App: a.Name, FeesWei: "0"}
	}
	body := struct {
		Demand []appDemand `json:"demand"`
		// OrchestratorError is set when the orchestrator did not answer, in
		// which case runners and slots are missing from the lines.
		OrchestratorError string `json:"orchestrator_error,omitempty"`
	}{}
	if runners, err := s.core.Runners(r.Context(), ""); err != nil {
		body.OrchestratorError = err.Error()
	} else {
		for _, runner := range runners {
			line := lines[runner.App]
			if line == nil || !runner.Ready || runner.State != core.StateActive {
				continue
			}
			line.RunnersOnline++
			line.SlotsBusy += runner.CapacityUsed
			line.SlotsTotal += runner.Capacity
		}
	}
	demand, err := s.ledger.Demand(r.Context(), s.now().Add(-24*time.Hour))
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	for app, d := range demand {
		if line := lines[app]; line != nil {
			line.Sessions, line.FeesWei, line.Refused = d.Sessions, d.FeesWei, d.Refused
		}
	}
	for _, line := range lines {
		body.Demand = append(body.Demand, *line)
	}
	sort.Slice(body.Demand, func(i, j int) bool { return body.Demand[i].App < body.Demand[j].App })
	if body.Demand == nil {
		body.Demand = []appDemand{}
	}
	writeJSON(w, http.StatusOK, body)
}

func (s *Server) adminBatches(w http.ResponseWriter, r *http.Request, actor string) {
	batches, err := s.payouts.Batches(r.Context())
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"batches": batches})
}

func (s *Server) adminBatch(w http.ResponseWriter, r *http.Request, actor string) {
	batch, err := s.payouts.Batch(r.Context(), r.PathValue("id"))
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, batch)
}

func (s *Server) setBatchItem(w http.ResponseWriter, r *http.Request, actor string) {
	var req struct {
		Include *bool `json:"include"`
	}
	if err := readJSON(r, &req); err != nil || req.Include == nil {
		s.writeError(w, r, errBadRequest)
		return
	}
	batch, err := s.payouts.SetItemIncluded(r.Context(), actor, r.PathValue("id"), r.PathValue("member"), *req.Include)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, batch)
}

func (s *Server) approveBatch(w http.ResponseWriter, r *http.Request, actor string) {
	batch, err := s.payouts.Approve(r.Context(), actor, r.PathValue("id"))
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, batch)
}

func (s *Server) retryBatch(w http.ResponseWriter, r *http.Request, actor string) {
	batch, err := s.payouts.Retry(r.Context(), actor, r.PathValue("id"))
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, batch)
}

func (s *Server) adminWallet(w http.ResponseWriter, r *http.Request, actor string) {
	wallet, err := s.payouts.Wallet(r.Context())
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, wallet)
}
