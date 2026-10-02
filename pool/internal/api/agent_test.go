package api_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Livepeer-Open-Pool/pool/internal/agent"
	"github.com/Livepeer-Open-Pool/pool/internal/compose"
	"github.com/Livepeer-Open-Pool/pool/internal/core"
	"github.com/Livepeer-Open-Pool/pool/internal/testkit"
)

// join takes a new member through application and approval with one machine
// assigned app, and returns their session, the machine and its credential.
func (e *env) join(admin string, gpu core.GPU, app core.App) (token string, machine core.Machine, credential core.NewCredential) {
	e.t.Helper()
	wallet := testkit.NewWallet(e.t)
	token = e.signInWallet(wallet)
	draft := map[string]any{
		"display_name": "Alice",
		"machines":     []map[string]any{{"label": "box-1", "gpus": []map[string]any{{"gpu_id": gpu.ID, "count": 1}}}},
	}
	var toSign struct {
		Message string `json:"message"`
	}
	e.ok(http.StatusOK, "POST", "/v1/applications/message", token, draft, &toSign)
	draft["signature"] = wallet.Sign(e.t, toSign.Message)
	var application core.Application
	e.ok(http.StatusCreated, "POST", "/v1/applications", token, draft, &application)
	e.ok(http.StatusOK, "POST", "/v1/admin/applications/"+application.ID+"/approve", admin, map[string]any{
		"machines": []map[string]any{{"machine": 0, "assignments": []map[string]any{{"app_id": app.ID}}}},
	}, nil)
	var machines struct {
		Machines []core.Machine `json:"machines"`
	}
	e.ok(http.StatusOK, "GET", "/v1/me/machines", token, nil, &machines)
	machine = machines.Machines[0]
	e.ok(http.StatusCreated, "POST", "/v1/me/machines/"+machine.ID+"/credential", token, nil, &credential)
	return token, machine, credential
}

// machineAgent is a member agent pointed at the service, with a stand-in Docker.
func (e *env) machineAgent(credential string, docker *testkit.Docker) *agent.Agent {
	return &agent.Agent{
		PoolURL: e.url, Credential: credential, Key: e.composeKey, Docker: docker, Now: e.clock.Now,
		ListGPUs: func(context.Context) ([]agent.GPU, error) {
			return []agent.GPU{{Name: "NVIDIA GeForce RTX 4090", MemoryMB: 24564}}, nil
		},
		Log: slog.New(slog.DiscardHandler),
	}
}

// images are the images of the services docker runs, by service kind.
func images(t *testing.T, docker *testkit.Docker) (apps, tunnels []string) {
	t.Helper()
	file := docker.Running(t)
	if file == nil {
		return nil, nil
	}
	for name, service := range file.Services {
		if len(name) > 4 && name[:4] == "app-" {
			apps = append(apps, service.Image)
		} else {
			tunnels = append(tunnels, service.Image)
		}
	}
	return apps, tunnels
}

func TestTheAgentKeepsAMachineRunningWhatItIsAssigned(t *testing.T) {
	e := newEnv(t)
	admin := e.signInAdmin()
	gpu, vllm, flux := e.catalog(admin)
	token, machine, credential := e.join(admin, gpu, vllm)

	// The pool says how machines join, and the member is shown the one command.
	var pool struct {
		Agent struct{ Image, Key string }
	}
	e.ok(http.StatusOK, "GET", "/v1/public/pool", "", nil, &pool)
	require.Equal(t, agentImage, pool.Agent.Image)
	require.Equal(t, compose.EncodePublicKey(e.composeKey), pool.Agent.Key)
	require.Equal(t, "docker run -d --name openpool-agent-us-east --restart unless-stopped --gpus all"+
		" -v /var/run/docker.sock:/var/run/docker.sock -e OPENPOOL_CREDENTIAL="+credential.Secret+
		" "+agentImage+" --pool https://us-east.api.open-pool.com --key "+pool.Agent.Key, credential.AgentCommand)

	// One round of the agent brings the assigned app up beside its tunnel.
	docker := &testkit.Docker{}
	a := e.machineAgent(credential.Secret, docker)
	require.NoError(t, a.Step(context.Background()))
	apps, tunnels := images(t, docker)
	require.Equal(t, []string{testkit.PinnedImage}, apps)
	require.Equal(t, []string{tunnelImage}, tunnels)
	require.Equal(t, credential.Secret, docker.Credential)
	require.NotContains(t, string(docker.File), credential.Secret, "the signed file holds no secret")
	file := docker.Running(t)
	tunnel := file.Services["tunnel-"+machine.Assignments[0].ID]
	require.Contains(t, tunnel.Command, "vllm/qwen2.5-7b-instruct")
	require.Contains(t, tunnel.Command, "https://orch.us-east.open-pool.com:8935")

	// The member and the operator see what the agent found on the box.
	var machines struct {
		Machines []core.Machine `json:"machines"`
	}
	e.ok(http.StatusOK, "GET", "/v1/me/machines", token, nil, &machines)
	reported := machines.Machines[0].Reported
	require.NotNil(t, reported)
	require.Equal(t, "NVIDIA GeForce RTX 4090", reported.GPUs[0].Name)
	require.Len(t, reported.Containers, 2)
	require.Equal(t, "running", reported.Containers[0].State)
	require.NotEmpty(t, reported.Generation)
	require.Empty(t, reported.Error)

	// Nothing changed: the next round leaves Docker alone.
	require.NoError(t, a.Step(context.Background()))
	require.Equal(t, 1, docker.Ups)

	// The operator moves the machine to another app; the agent switches it.
	e.ok(http.StatusOK, "PUT", "/v1/admin/machines/"+machine.ID+"/assignments", admin,
		map[string]any{"assignments": []map[string]any{{"app_id": flux.ID}}}, nil)
	require.NoError(t, e.core.Reconcile(context.Background()))
	require.NoError(t, a.Step(context.Background()))
	require.Equal(t, 2, docker.Ups)
	file = docker.Running(t)
	require.Len(t, file.Services, 2)
	for name, service := range file.Services {
		if service.Image == tunnelImage {
			require.Contains(t, service.Command, "flux/schnell", name)
		}
	}

	// A new version with a new image reaches it the same way.
	const next = "blackforestlabs/flux@sha256:2222222222222222222222222222222222222222222222222222222222222222"
	container := flux.Container
	container.Image = next
	e.ok(http.StatusOK, "POST", "/v1/admin/apps/"+flux.ID+"/version", admin, map[string]any{"version": "2.0.0", "container": container}, nil)
	require.NoError(t, e.core.Reconcile(context.Background()))
	require.NoError(t, a.Step(context.Background()))
	apps, _ = images(t, docker)
	require.Equal(t, []string{next}, apps)
	require.Contains(t, string(docker.File), `"2.0.0"`)

	// With nothing assigned, everything the pool started is stopped.
	e.ok(http.StatusOK, "PUT", "/v1/admin/machines/"+machine.ID+"/assignments", admin, map[string]any{"assignments": []map[string]any{}}, nil)
	require.NoError(t, e.core.Reconcile(context.Background()))
	require.NoError(t, a.Step(context.Background()))
	require.Nil(t, docker.Running(t))
	require.Equal(t, 1, docker.Downs)
}

func TestTheAgentRefusesFilesNotSignedByThePoolItWasInstalledFor(t *testing.T) {
	e := newEnv(t)
	admin := e.signInAdmin()
	gpu, vllm, _ := e.catalog(admin)
	token, _, credential := e.join(admin, gpu, vllm)

	docker := &testkit.Docker{}
	a := e.machineAgent(credential.Secret, docker)
	other, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	a.Key = other
	require.ErrorContains(t, a.Step(context.Background()), "not signed by this pool's key")
	require.Zero(t, docker.Ups)

	// The pool is told, so the member reads why nothing is running.
	var machines struct {
		Machines []core.Machine `json:"machines"`
	}
	e.ok(http.StatusOK, "GET", "/v1/me/machines", token, nil, &machines)
	require.Contains(t, machines.Machines[0].Reported.Error, "not signed by this pool's key")
}

func TestTheAgentStopsItsContainersSomeTimeAfterItsCredentialIsRefused(t *testing.T) {
	e := newEnv(t)
	admin := e.signInAdmin()
	gpu, vllm, _ := e.catalog(admin)
	token, machine, credential := e.join(admin, gpu, vllm)

	docker := &testkit.Docker{}
	a := e.machineAgent(credential.Secret, docker)
	require.NoError(t, a.Step(context.Background()))
	require.NotNil(t, docker.Running(t))

	// Alice removes the machine. The agent is refused, and leaves what is
	// running alone at first, so sessions can finish.
	e.ok(http.StatusNoContent, "DELETE", "/v1/me/machines/"+machine.ID, token, nil, nil)
	require.ErrorContains(t, a.Step(context.Background()), "does not accept this machine's credential")
	require.NotNil(t, docker.Running(t))
	e.clock.Advance(59 * time.Minute)
	require.Error(t, a.Step(context.Background()))
	require.NotNil(t, docker.Running(t))

	e.clock.Advance(2 * time.Minute)
	require.Error(t, a.Step(context.Background()))
	require.Nil(t, docker.Running(t))
	require.Error(t, a.Step(context.Background()))
	require.Equal(t, 1, docker.Downs, "stopped once, not on every round")
}

func TestAgentEndpointsNeedAMachineCredential(t *testing.T) {
	e := newEnv(t)
	admin := e.signInAdmin()
	require.Equal(t, "unknown credential", e.refused(http.StatusForbidden, "GET", "/v1/agent/desired", "", nil))
	require.Equal(t, "unknown credential", e.refused(http.StatusForbidden, "GET", "/v1/agent/desired", admin, nil))
	require.Equal(t, "unknown credential", e.refused(http.StatusForbidden, "POST", "/v1/agent/report", "opc_made_up", map[string]any{}))
}
