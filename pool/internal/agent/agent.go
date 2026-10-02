// Package agent is the program a pool member runs on a GPU machine. It keeps
// the machine running what the pool assigned it: it fetches the machine's
// Compose file, checks that the pool signed it and that it asks for nothing
// but app containers, applies it with Docker, and reports back what is
// running and which GPUs the machine has.
package agent

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/Livepeer-Open-Pool/pool/internal/compose"
)

// Version is the agent's version, set when it is built.
var Version = "dev"

const (
	// reapplyInterval is how often an unchanged file is applied again, which
	// brings back a container someone removed by hand.
	reapplyInterval = 5 * time.Minute
	// refusedGrace is how long the pool may refuse the credential before the
	// agent stops what the pool had it start. It is long enough for the
	// sessions of a machine that was removed from the pool to finish.
	refusedGrace = time.Hour
	// gpuInterval is how often the machine's GPUs are listed again.
	gpuInterval = 10 * time.Minute
)

// Container is a container of the pool's Compose project.
type Container struct {
	Service string `json:"service"`
	State   string `json:"state"`
	Health  string `json:"health,omitempty"`
}

// GPU is a GPU of the machine.
type GPU struct {
	Name     string `json:"name"`
	MemoryMB int    `json:"memory_mb"`
}

// Report is what the agent tells the pool about the machine.
type Report struct {
	AgentVersion string      `json:"agent_version"`
	Generation   string      `json:"generation"`
	Error        string      `json:"error,omitempty"`
	Containers   []Container `json:"containers"`
	GPUs         []GPU       `json:"gpus"`
	GPUError     string      `json:"gpu_error,omitempty"`
}

// Docker is what the agent needs from Docker.
type Docker interface {
	// Up makes the machine run file, removing containers no longer in it.
	// credential is the value of the file's credential variable.
	Up(ctx context.Context, file []byte, credential string) error
	// Down stops and removes everything the agent started. Volumes stay.
	Down(ctx context.Context) error
	// Containers lists the project's containers.
	Containers(ctx context.Context) ([]Container, error)
}

// errRefused is the pool not accepting the machine's credential.
var errRefused = errors.New("the pool does not accept this machine's credential")

// Agent keeps one machine in step with the pool.
type Agent struct {
	// PoolURL is the pool service's address, e.g. https://us-east.api.open-pool.com.
	PoolURL    string
	Credential string
	// Key is the pool's Compose signing key; files signed by any other are refused.
	Key    ed25519.PublicKey
	Docker Docker
	// ListGPUs lists the machine's GPUs; nil reports none.
	ListGPUs func(ctx context.Context) ([]GPU, error)
	HTTP     *http.Client
	Log      *slog.Logger
	// Now is the clock; nil means time.Now.
	Now func() time.Time

	machineID  string // the machine the first accepted answer was for
	issuedAt   int64  // of the newest accepted answer
	generation string // of the file last applied
	appliedAt  time.Time
	applyErr   string
	downed     bool // nothing of the pool's is running because the agent took it down

	refusedSince time.Time
	gpus         []GPU
	gpuErr       string
	gpusAt       time.Time
}

func (a *Agent) now() time.Time {
	if a.Now != nil {
		return a.Now()
	}
	return time.Now()
}

// Run calls Step every interval until ctx ends. Whatever was started keeps
// running when the agent stops: the containers restart by themselves and the
// agent picks them up again when it comes back.
func (a *Agent) Run(ctx context.Context, interval time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := a.Step(ctx); err != nil && ctx.Err() == nil {
			a.Log.Warn("not in step with the pool", "err", err)
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return nil
		}
	}
}

// Step is one round: fetch, verify, apply, report.
func (a *Agent) Step(ctx context.Context) error {
	signed, err := a.fetch(ctx)
	if errors.Is(err, errRefused) {
		return a.refused(ctx)
	}
	if err != nil {
		// The pool is unreachable: keep running what it last said.
		return err
	}
	a.refusedSince = time.Time{}

	stepErr := a.apply(ctx, signed)
	if stepErr != nil {
		a.applyErr = stepErr.Error()
	} else {
		a.applyErr = ""
	}
	if err := a.report(ctx); err != nil {
		return errors.Join(stepErr, fmt.Errorf("reporting to the pool: %w", err))
	}
	return stepErr
}

// refused handles a pool that no longer accepts the credential: the machine
// was removed, the member banned, or the credential replaced. Nothing the
// agent runs can earn any more, so after a while it is stopped.
func (a *Agent) refused(ctx context.Context) error {
	now := a.now()
	if a.refusedSince.IsZero() {
		a.refusedSince = now
	}
	if !a.downed && now.Sub(a.refusedSince) >= refusedGrace {
		if err := a.Docker.Down(ctx); err != nil {
			return fmt.Errorf("%w; stopping the pool's containers: %v", errRefused, err)
		}
		a.downed, a.generation = true, ""
		a.Log.Warn("stopped the pool's containers: the credential has been refused for too long", "since", a.refusedSince)
	}
	return fmt.Errorf("%w; create a new one on the machine's page and start the agent with it", errRefused)
}

// apply verifies the pool's answer and makes the machine run it.
func (a *Agent) apply(ctx context.Context, signed compose.Signed) error {
	file, err := compose.Verify(a.Key, signed)
	if err != nil {
		return fmt.Errorf("refused the pool's compose file: %w", err)
	}
	if a.machineID != "" && signed.MachineID != a.machineID {
		return fmt.Errorf("refused the pool's compose file: it is for machine %s, not %s", signed.MachineID, a.machineID)
	}
	if signed.IssuedAt < a.issuedAt {
		return errors.New("refused the pool's compose file: it is older than one already applied")
	}
	a.machineID, a.issuedAt = signed.MachineID, signed.IssuedAt

	now := a.now()
	if signed.Generation == a.generation && a.applyErr == "" && now.Sub(a.appliedAt) < reapplyInterval {
		return nil
	}
	changed := signed.Generation != a.generation
	if len(file.Services) == 0 {
		err = a.Docker.Down(ctx)
	} else {
		err = a.Docker.Up(ctx, []byte(signed.Compose), a.Credential)
	}
	if err != nil {
		return fmt.Errorf("applying the pool's compose file: %w", err)
	}
	a.generation, a.appliedAt, a.downed = signed.Generation, now, len(file.Services) == 0
	if changed {
		a.Log.Info("applied the pool's compose file", "generation", signed.Generation, "containers", len(file.Services))
	}
	return nil
}

func (a *Agent) fetch(ctx context.Context) (compose.Signed, error) {
	var signed compose.Signed
	err := a.call(ctx, http.MethodGet, "/v1/agent/desired", nil, &signed)
	return signed, err
}

func (a *Agent) report(ctx context.Context) error {
	report := Report{AgentVersion: Version, Generation: a.generation, Error: a.applyErr, Containers: []Container{}, GPUs: []GPU{}}
	containers, err := a.Docker.Containers(ctx)
	if err != nil {
		report.Error = strings.TrimSpace(report.Error + " listing containers: " + err.Error())
	} else if containers != nil {
		report.Containers = containers
	}
	if a.ListGPUs != nil {
		if now := a.now(); a.gpusAt.IsZero() || now.Sub(a.gpusAt) >= gpuInterval {
			a.gpusAt = now
			a.gpus, a.gpuErr = nil, ""
			if gpus, err := a.ListGPUs(ctx); err != nil {
				a.gpuErr = err.Error()
			} else {
				a.gpus = gpus
			}
		}
		if a.gpus != nil {
			report.GPUs = a.gpus
		}
		report.GPUError = a.gpuErr
	}
	return a.call(ctx, http.MethodPost, "/v1/agent/report", report, nil)
}

// call makes one request to the pool with the machine's credential.
func (a *Agent) call(ctx context.Context, method, path string, in, out any) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var body io.Reader
	if in != nil {
		data, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(a.PoolURL, "/")+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+a.Credential)
	req.Header.Set("User-Agent", "openpool-agent/"+Version)
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	client := a.HTTP
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return err
	}
	switch {
	case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
		return errRefused
	case resp.StatusCode/100 != 2:
		var problem struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(data, &problem) == nil && problem.Error != "" {
			return fmt.Errorf("the pool answered %d: %s", resp.StatusCode, problem.Error)
		}
		return fmt.Errorf("the pool answered %d", resp.StatusCode)
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(data, out)
}
