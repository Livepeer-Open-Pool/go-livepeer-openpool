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
	writeJSON(w, http.StatusOK, publicPoolBody{
		Region: s.region(), TakingApplications: terms.Global != nil, AppsOffered: len(apps), SignIn: s.signIn,
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
	UpdatedAt    time.Time  `json:"updated_at"`
}

func (s *Server) publicApps(w http.ResponseWriter, r *http.Request) {
	apps, err := s.core.Apps(r.Context(), true)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	out := make([]publicApp, 0, len(apps))
	for _, a := range apps {
		out = append(out, publicApp{ID: a.ID, Name: a.Name, Mode: a.Mode, Price: a.Price, Description: a.Description,
			EligibleGPUs: a.EligibleGPUs, UpdatedAt: a.UpdatedAt})
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
