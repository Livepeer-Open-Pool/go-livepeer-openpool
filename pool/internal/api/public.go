package api

import (
	"net/http"
	"time"

	"github.com/Livepeer-Open-Pool/pool/internal/core"
)

type regionBody struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func (s *Server) region() regionBody {
	id, name := s.core.Region()
	return regionBody{ID: id, Name: name}
}

type publicPoolBody struct {
	Region regionBody `json:"region"`
	// TakingApplications is false until the pool has published its terms.
	TakingApplications bool       `json:"taking_applications"`
	AppsOffered        int        `json:"apps_offered"`
	SignIn             SignInInfo `json:"sign_in"`
	// Agent is how machines join, when the pool runs them with the agent.
	Agent *publicAgent `json:"agent,omitempty"`
	// CommissionBps and WindowRounds are the payout terms a member is paid
	// under: the pool's cut in basis points, and how many rounds a payout
	// window lasts.
	CommissionBps int `json:"commission_bps"`
	WindowRounds  int `json:"window_rounds"`
}

func (s *Server) publicPool(w http.ResponseWriter, r *http.Request) {
	terms, err := s.core.Terms(r.Context())
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	apps, err := s.core.Apps(r.Context(), true)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	settings, err := s.core.Settings(r.Context())
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, publicPoolBody{
		Region: s.region(), TakingApplications: terms.Global != nil, AppsOffered: len(apps), SignIn: s.signIn, Agent: s.publicAgent(),
		CommissionBps: settings.CommissionBps, WindowRounds: settings.WindowRounds,
	})
}

// publicApp is an app as anyone may see it: what it is and what it pays,
// not how it is run or tested.
type publicApp struct {
	ID           string     `json:"id"`
	Name         string     `json:"name"`
	Mode         string     `json:"mode"`
	Price        core.Price `json:"price"`
	Description  string     `json:"description"`
	EligibleGPUs []string   `json:"eligible_gpus"`
	// Runners is how many machines are connected for the app right now.
	Runners   int       `json:"runners"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (s *Server) publicApps(w http.ResponseWriter, r *http.Request) {
	apps, err := s.core.Apps(r.Context(), true)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	connected, err := s.core.ConnectedRunners(r.Context())
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	out := make([]publicApp, 0, len(apps))
	for _, a := range apps {
		out = append(out, publicApp{ID: a.ID, Name: a.Name, Mode: a.Mode, Price: a.Price, Description: a.Description,
			EligibleGPUs: a.EligibleGPUs, Runners: connected[a.Name], UpdatedAt: a.UpdatedAt})
	}
	writeJSON(w, http.StatusOK, map[string]any{"region": s.region(), "apps": out})
}

func (s *Server) publicGPUs(w http.ResponseWriter, r *http.Request) {
	gpus, err := s.core.GPUs(r.Context(), true)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"gpus": gpus})
}

func (s *Server) publicTerms(w http.ResponseWriter, r *http.Request) {
	terms, err := s.core.Terms(r.Context())
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, terms)
}
