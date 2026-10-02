package api

import (
	"crypto/ed25519"
	"net/http"

	"github.com/Livepeer-Open-Pool/pool/internal/compose"
	"github.com/Livepeer-Open-Pool/pool/internal/core"
)

// AgentOptions are what the service needs to tell member agents what to run.
type AgentOptions struct {
	// Key signs the compose files; agents trust its public half.
	Key ed25519.PrivateKey
	// Image is the agent's own image, shown in the install command.
	Image string
	// TunnelImage holds livepeer_runner_tunnel, pinned by digest.
	TunnelImage string
	// OrchestratorURL is where the tunnels register.
	OrchestratorURL string
}

// publicAgent is what anyone may know about how machines join: the agent's
// image and the key its files are signed with.
type publicAgent struct {
	Image string `json:"image"`
	Key   string `json:"key"`
}

func (s *Server) publicAgent() *publicAgent {
	if s.agent == nil {
		return nil
	}
	return &publicAgent{Image: s.agent.Image, Key: compose.EncodePublicKey(s.agent.Key.Public().(ed25519.PublicKey))}
}

// agentDesired answers a machine's agent with the compose file the machine
// is to run, signed. The machine is known by its credential.
func (s *Server) agentDesired(w http.ResponseWriter, r *http.Request) {
	if s.agent == nil {
		writeJSON(w, http.StatusServiceUnavailable, errorBody{"this pool is not set up for the agent yet"})
		return
	}
	deployment, err := s.core.Deployment(r.Context(), bearer(r))
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	spec := compose.Spec{OrchestratorURL: s.agent.OrchestratorURL, TunnelImage: s.agent.TunnelImage}
	for _, app := range deployment.Apps {
		c := app.Container
		spec.Apps = append(spec.Apps, compose.App{
			ID: app.AssignmentID, CacheID: app.AppID, Name: app.App, Mode: app.Mode, Version: app.Version, Capacity: app.Capacity,
			Image: c.Image, Command: c.Command, Env: c.Env, Port: c.Port, HealthPath: c.HealthPath,
			GPUs: c.GPUs, NoGPU: c.NoGPU, ShmSize: c.ShmSize, CachePath: c.CachePath,
		})
	}
	file, err := compose.Generate(spec)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, compose.Sign(s.agent.Key, deployment.MachineID, s.now(), file))
}

func (s *Server) agentReport(w http.ResponseWriter, r *http.Request) {
	var report core.AgentReport
	if err := readJSON(r, &report); err != nil {
		s.writeError(w, r, err)
		return
	}
	if err := s.core.RecordAgentReport(r.Context(), bearer(r), report); err != nil {
		s.writeError(w, r, err)
		return
	}
	noContent(w)
}
