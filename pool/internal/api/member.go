package api

import (
	"net/http"
	"strconv"

	"github.com/Livepeer-Open-Pool/pool/internal/core"
)

func (s *Server) applicationMessage(w http.ResponseWriter, r *http.Request, address string) {
	var draft core.ApplicationDraft
	if err := readJSON(r, &draft); err != nil {
		s.writeError(w, r, err)
		return
	}
	message, err := s.core.ApplicationMessage(r.Context(), address, draft)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"message": message})
}

func (s *Server) submitApplication(w http.ResponseWriter, r *http.Request, address string) {
	var req struct {
		core.ApplicationDraft
		Signature string `json:"signature"`
	}
	if err := readJSON(r, &req); err != nil {
		s.writeError(w, r, err)
		return
	}
	application, err := s.core.SubmitApplication(r.Context(), address, req.ApplicationDraft, req.Signature)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, application)
}

type meBody struct {
	Region  regionBody   `json:"region"`
	Address string       `json:"address"`
	Member  *core.Member `json:"member"`
	// Applications are this address's applications here, newest first.
	Applications []core.Application `json:"applications"`
}

func (s *Server) me(w http.ResponseWriter, r *http.Request, address string) {
	member, err := s.core.MemberByAddress(r.Context(), address)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	applications, err := s.core.Applications(r.Context(), "", address)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, meBody{Region: s.region(), Address: address, Member: member, Applications: applications})
}

func (s *Server) myMachines(w http.ResponseWriter, r *http.Request, member *core.Member) {
	machines, err := s.core.Machines(r.Context(), member.ID, "")
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"machines": machines})
}

func (s *Server) requestMachine(w http.ResponseWriter, r *http.Request, member *core.Member) {
	var spec core.MachineSpec
	if err := readJSON(r, &spec); err != nil {
		s.writeError(w, r, err)
		return
	}
	machine, err := s.core.RequestMachine(r.Context(), member, spec)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, machine)
}

func (s *Server) removeMyMachine(w http.ResponseWriter, r *http.Request, member *core.Member) {
	if err := s.core.RemoveMachine(r.Context(), member.Address, member.ID, r.PathValue("id")); err != nil {
		s.writeError(w, r, err)
		return
	}
	noContent(w)
}

func (s *Server) createCredential(w http.ResponseWriter, r *http.Request, member *core.Member) {
	credential, err := s.core.CreateCredential(r.Context(), member, r.PathValue("id"))
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, credential)
}

func (s *Server) revokeCredential(w http.ResponseWriter, r *http.Request, member *core.Member) {
	if err := s.core.RevokeCredential(r.Context(), member, r.PathValue("id")); err != nil {
		s.writeError(w, r, err)
		return
	}
	noContent(w)
}

func (s *Server) myRunners(w http.ResponseWriter, r *http.Request, member *core.Member) {
	runners, err := s.core.Runners(r.Context(), member.ID)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"runners": runners})
}

func (s *Server) myTests(w http.ResponseWriter, r *http.Request, member *core.Member) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	results, err := s.core.TestResults(r.Context(), r.URL.Query().Get("runner"), member.ID, limit)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"tests": results})
}
