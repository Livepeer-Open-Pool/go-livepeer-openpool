package api

import (
	"net/http"
	"strconv"
	"time"

	"github.com/Livepeer-Open-Pool/pool/internal/core"
)

func (s *Server) adminStatus(w http.ResponseWriter, r *http.Request, actor string) {
	status := struct {
		Region       regionBody `json:"region"`
		Orchestrator struct {
			Reachable bool   `json:"reachable"`
			Error     string `json:"error,omitempty"`
			Runners   int    `json:"runners"`
		} `json:"orchestrator"`
		PendingApplications int `json:"pending_applications"`
		PendingMachines     int `json:"pending_machines"`
	}{Region: s.region()}
	if runners, err := s.core.Runners(r.Context(), ""); err != nil {
		status.Orchestrator.Error = err.Error()
	} else {
		status.Orchestrator.Reachable, status.Orchestrator.Runners = true, len(runners)
	}
	applications, err := s.core.Applications(r.Context(), core.ApplicationPending, "")
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	machines, err := s.core.Machines(r.Context(), "", core.MachinePending)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	status.PendingApplications, status.PendingMachines = len(applications), len(machines)
	writeJSON(w, http.StatusOK, status)
}

func (s *Server) adminApplications(w http.ResponseWriter, r *http.Request, actor string) {
	applications, err := s.core.Applications(r.Context(), r.URL.Query().Get("status"), r.URL.Query().Get("address"))
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"applications": applications})
}

func (s *Server) adminApplication(w http.ResponseWriter, r *http.Request, actor string) {
	application, err := s.core.Application(r.Context(), r.PathValue("id"))
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, application)
}

func (s *Server) approveApplication(w http.ResponseWriter, r *http.Request, actor string) {
	var req struct {
		Machines []core.MachineAssignments `json:"machines"`
	}
	if err := readJSON(r, &req); err != nil {
		s.writeError(w, r, err)
		return
	}
	member, err := s.core.ApproveApplication(r.Context(), actor, r.PathValue("id"), req.Machines)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, member)
}

type reasonBody struct {
	Reason string `json:"reason"`
}

func (s *Server) rejectApplication(w http.ResponseWriter, r *http.Request, actor string) {
	var req reasonBody
	if err := readJSON(r, &req); err != nil {
		s.writeError(w, r, err)
		return
	}
	application, err := s.core.RejectApplication(r.Context(), actor, r.PathValue("id"), req.Reason)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, application)
}

func (s *Server) adminMembers(w http.ResponseWriter, r *http.Request, actor string) {
	members, err := s.core.Members(r.Context())
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"members": members})
}

func (s *Server) adminMember(w http.ResponseWriter, r *http.Request, actor string) {
	member, err := s.core.Member(r.Context(), r.PathValue("id"))
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	machines, err := s.core.Machines(r.Context(), member.ID, "")
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	applications, err := s.core.Applications(r.Context(), "", member.Address)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"member": member, "machines": machines, "applications": applications})
}

func (s *Server) banMember(w http.ResponseWriter, r *http.Request, actor string) {
	var req reasonBody
	if err := readJSON(r, &req); err != nil {
		s.writeError(w, r, err)
		return
	}
	member, err := s.core.BanMember(r.Context(), actor, r.PathValue("id"), req.Reason)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, member)
}

func (s *Server) unbanMember(w http.ResponseWriter, r *http.Request, actor string) {
	member, err := s.core.UnbanMember(r.Context(), actor, r.PathValue("id"))
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, member)
}

func (s *Server) adminMachines(w http.ResponseWriter, r *http.Request, actor string) {
	machines, err := s.core.Machines(r.Context(), r.URL.Query().Get("member"), r.URL.Query().Get("status"))
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"machines": machines})
}

func (s *Server) approveMachine(w http.ResponseWriter, r *http.Request, actor string) {
	machine, err := s.core.ApproveMachine(r.Context(), actor, r.PathValue("id"))
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, machine)
}

func (s *Server) setAssignments(w http.ResponseWriter, r *http.Request, actor string) {
	var req struct {
		Assignments []core.AssignmentInput `json:"assignments"`
	}
	if err := readJSON(r, &req); err != nil {
		s.writeError(w, r, err)
		return
	}
	machine, err := s.core.SetAssignments(r.Context(), actor, r.PathValue("id"), req.Assignments)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, machine)
}

func (s *Server) adminRemoveMachine(w http.ResponseWriter, r *http.Request, actor string) {
	if err := s.core.RemoveMachine(r.Context(), actor, "", r.PathValue("id")); err != nil {
		s.writeError(w, r, err)
		return
	}
	noContent(w)
}

func (s *Server) adminGPUs(w http.ResponseWriter, r *http.Request, actor string) {
	gpus, err := s.core.GPUs(r.Context(), false)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"gpus": gpus})
}

func (s *Server) createGPU(w http.ResponseWriter, r *http.Request, actor string) {
	var in core.GPUInput
	if err := readJSON(r, &in); err != nil {
		s.writeError(w, r, err)
		return
	}
	gpu, err := s.core.CreateGPU(r.Context(), actor, in)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, gpu)
}

func (s *Server) updateGPU(w http.ResponseWriter, r *http.Request, actor string) {
	var in core.GPUInput
	if err := readJSON(r, &in); err != nil {
		s.writeError(w, r, err)
		return
	}
	gpu, err := s.core.UpdateGPU(r.Context(), actor, r.PathValue("id"), in)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, gpu)
}

func (s *Server) adminApps(w http.ResponseWriter, r *http.Request, actor string) {
	apps, err := s.core.Apps(r.Context(), false)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"apps": apps})
}

func (s *Server) adminApp(w http.ResponseWriter, r *http.Request, actor string) {
	app, err := s.core.App(r.Context(), r.PathValue("id"))
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, app)
}

func (s *Server) createApp(w http.ResponseWriter, r *http.Request, actor string) {
	var in core.AppInput
	if err := readJSON(r, &in); err != nil {
		s.writeError(w, r, err)
		return
	}
	app, err := s.core.CreateApp(r.Context(), actor, in)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, app)
}

func (s *Server) updateApp(w http.ResponseWriter, r *http.Request, actor string) {
	var in core.AppInput
	if err := readJSON(r, &in); err != nil {
		s.writeError(w, r, err)
		return
	}
	app, err := s.core.UpdateApp(r.Context(), actor, r.PathValue("id"), in)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, app)
}

func (s *Server) setAppVersion(w http.ResponseWriter, r *http.Request, actor string) {
	var req struct {
		Version  string     `json:"version"`
		Deadline *time.Time `json:"deadline"`
	}
	if err := readJSON(r, &req); err != nil {
		s.writeError(w, r, err)
		return
	}
	app, err := s.core.SetAppVersion(r.Context(), actor, r.PathValue("id"), req.Version, req.Deadline)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, app)
}

func (s *Server) pauseApp(w http.ResponseWriter, r *http.Request, actor string) {
	s.setOffered(w, r, actor, false)
}

func (s *Server) resumeApp(w http.ResponseWriter, r *http.Request, actor string) {
	s.setOffered(w, r, actor, true)
}

func (s *Server) setOffered(w http.ResponseWriter, r *http.Request, actor string, offered bool) {
	app, err := s.core.SetAppOffered(r.Context(), actor, r.PathValue("id"), offered)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, app)
}

func (s *Server) adminTerms(w http.ResponseWriter, r *http.Request, actor string) {
	terms, err := s.core.Terms(r.Context())
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, terms)
}

func (s *Server) publishTerms(w http.ResponseWriter, r *http.Request, actor string) {
	var req struct {
		Scope string `json:"scope"`
		Body  string `json:"body"`
	}
	if err := readJSON(r, &req); err != nil {
		s.writeError(w, r, err)
		return
	}
	terms, err := s.core.PublishTerms(r.Context(), actor, req.Scope, req.Body)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, terms)
}

func (s *Server) getSettings(w http.ResponseWriter, r *http.Request, actor string) {
	settings, err := s.core.Settings(r.Context())
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, settings)
}

func (s *Server) putSettings(w http.ResponseWriter, r *http.Request, actor string) {
	// Start from what is saved, so a body that leaves a field out keeps it.
	settings, err := s.core.Settings(r.Context())
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	if err := readJSON(r, &settings); err != nil {
		s.writeError(w, r, err)
		return
	}
	if err := s.core.SaveSettings(r.Context(), actor, settings); err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, settings)
}

func (s *Server) adminRunners(w http.ResponseWriter, r *http.Request, actor string) {
	runners, err := s.core.Runners(r.Context(), r.URL.Query().Get("member"))
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"runners": runners})
}

func (s *Server) setRunnerState(w http.ResponseWriter, r *http.Request, actor string) {
	var req struct {
		State  string `json:"state"`
		Reason string `json:"reason"`
	}
	if err := readJSON(r, &req); err != nil {
		s.writeError(w, r, err)
		return
	}
	if err := s.core.SetRunnerState(r.Context(), actor, r.PathValue("id"), req.State, req.Reason); err != nil {
		s.writeError(w, r, err)
		return
	}
	noContent(w)
}

func (s *Server) evictRunner(w http.ResponseWriter, r *http.Request, actor string) {
	var req reasonBody
	if err := readJSON(r, &req); err != nil {
		s.writeError(w, r, err)
		return
	}
	if err := s.core.EvictRunner(r.Context(), actor, r.PathValue("id"), req.Reason); err != nil {
		s.writeError(w, r, err)
		return
	}
	noContent(w)
}

func (s *Server) adminAudit(w http.ResponseWriter, r *http.Request, actor string) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	entries, err := s.core.Audit(r.Context(), limit)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"entries": entries})
}

func (s *Server) holdRunner(w http.ResponseWriter, r *http.Request, actor string) {
	req := struct {
		Hold   bool   `json:"hold"`
		Reason string `json:"reason"`
	}{Hold: true}
	if err := readJSON(r, &req); err != nil {
		s.writeError(w, r, err)
		return
	}
	if err := s.core.SetRunnerHold(r.Context(), actor, r.PathValue("id"), req.Hold, req.Reason); err != nil {
		s.writeError(w, r, err)
		return
	}
	noContent(w)
}

func (s *Server) testRunner(w http.ResponseWriter, r *http.Request, actor string) {
	result, err := s.core.TestRunnerNow(r.Context(), actor, r.PathValue("id"))
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) adminTests(w http.ResponseWriter, r *http.Request, actor string) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	results, err := s.core.TestResults(r.Context(), r.URL.Query().Get("runner"), r.URL.Query().Get("member"), limit)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"tests": results})
}
