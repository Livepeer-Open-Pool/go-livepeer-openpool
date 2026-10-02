package api_test

import (
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Livepeer-Open-Pool/pool/internal/agent"
	"github.com/Livepeer-Open-Pool/pool/internal/compose"
	"github.com/Livepeer-Open-Pool/pool/internal/core"
)

// TestAgentImageOnRealDocker runs the agent's real image against this
// service and a real Docker daemon: the one command a member is shown brings
// the machine's app up beside its tunnel, and unassigning the app takes both
// down, with nothing else done on the machine. The tunnel has no
// orchestrator to reach here; registering through one is the end-to-end
// harness's job.
//
// It starts containers, so it runs only when asked to, with the images built
// from docker/Dockerfile.agent and go-livepeer's docker/Dockerfile.runner-tunnel:
//
//	OPENPOOL_DOCKER_TEST=1 \
//	OPENPOOL_TEST_AGENT_IMAGE=openpool-agent:dev \
//	OPENPOOL_TEST_TUNNEL_IMAGE=localhost:5000/livepeer-runner-tunnel@sha256:... \
//	go test ./internal/api -run AgentImageOnRealDocker
func TestAgentImageOnRealDocker(t *testing.T) {
	agentImage, tunnel := os.Getenv("OPENPOOL_TEST_AGENT_IMAGE"), os.Getenv("OPENPOOL_TEST_TUNNEL_IMAGE")
	if os.Getenv("OPENPOOL_DOCKER_TEST") == "" || agentImage == "" || tunnel == "" {
		t.Skip("set OPENPOOL_DOCKER_TEST=1, OPENPOOL_TEST_AGENT_IMAGE and OPENPOOL_TEST_TUNNEL_IMAGE to run against a real Docker daemon")
	}
	docker := func(args ...string) string {
		t.Helper()
		out, err := exec.Command("docker", args...).CombinedOutput()
		require.NoError(t, err, "docker %s: %s", strings.Join(args, " "), out)
		return strings.TrimSpace(string(out))
	}
	docker("pull", "--quiet", "busybox:stable")
	busybox := docker("image", "inspect", "--format", "{{index .RepoDigests 0}}", "busybox:stable")

	e := newEnvWithTunnel(t, tunnel)
	admin := e.signInAdmin()
	var gpu core.GPU
	e.ok(http.StatusCreated, "POST", "/v1/admin/gpus", admin, core.GPUInput{Name: "NVIDIA RTX 4090", VRAMMB: 24564}, &gpu)
	var app core.App
	e.ok(http.StatusCreated, "POST", "/v1/admin/apps", admin, core.AppInput{
		Name: "echo", Mode: core.ModeSingleShot, Price: core.Price{Amount: "0.50", Unit: "hour"}, RequiredVersion: "1.0.0",
		// A web server standing in for a model server; this box needs no GPU for it.
		Container:       core.Container{Image: busybox, Command: []string{"httpd", "-f", "-p", "8000"}, Port: 8000, NoGPU: true, Env: map[string]string{"NOTE": "costs $5"}},
		DefaultCapacity: 1, MaxCapacity: 1,
	}, &app)
	e.ok(http.StatusOK, "POST", "/v1/admin/terms", admin, map[string]string{"scope": "global", "body": "Be good."}, nil)
	token, machine, credential := e.join(admin, gpu, app)

	// The command the member is shown, with this test's addresses: the
	// service listens on this host's loopback, so the agent shares the
	// host's network, and this box is not asked for GPUs.
	name := fmt.Sprintf("openpool-agent-test-%d", time.Now().UnixNano())
	project := agent.ProjectFor(e.url)
	t.Cleanup(func() {
		_ = exec.Command("docker", "rm", "--force", name).Run()
		_ = exec.Command("docker", "compose", "-p", project, "down", "--remove-orphans").Run()
	})
	// Rootless Docker keeps its socket elsewhere; DOCKER_HOST says where.
	socket := "/var/run/docker.sock"
	if path, ok := strings.CutPrefix(os.Getenv("DOCKER_HOST"), "unix://"); ok {
		socket = path
	}
	docker("run", "-d", "--name", name, "--network", "host",
		"-v", socket+":/var/run/docker.sock",
		"-e", "OPENPOOL_CREDENTIAL="+credential.Secret,
		agentImage, "--pool", e.url, "--key", compose.EncodePublicKey(e.composeKey), "--interval", "1s")

	reported := func() *core.AgentReport {
		var machines struct {
			Machines []core.Machine `json:"machines"`
		}
		e.ok(http.StatusOK, "GET", "/v1/me/machines", token, nil, &machines)
		return machines.Machines[0].Reported
	}
	// eventually waits for cond, and says what the agent logged if it never holds.
	eventually := func(cond func() bool, wait time.Duration, what string) {
		t.Helper()
		for deadline := time.Now().Add(wait); time.Now().Before(deadline); time.Sleep(time.Second) {
			if cond() {
				return
			}
		}
		t.Fatalf("%s\nreported: %+v\nagent log:\n%s", what, reported(), docker("logs", name))
	}
	assignment := machine.Assignments[0].ID
	eventually(func() bool {
		r := reported()
		if r == nil || len(r.Containers) != 2 {
			return false
		}
		for _, c := range r.Containers {
			if c.Service == "app-"+assignment && c.State != "running" {
				return false
			}
		}
		return true
	}, 2*time.Minute, "the agent did not bring the app up")
	r := reported()
	require.NotEqual(t, "dev", r.AgentVersion)
	require.Empty(t, r.Error)
	require.NotEmpty(t, r.Generation)
	require.True(t, len(r.GPUs) > 0 || r.GPUError != "", "either the GPUs or why they could not be listed")

	// The app runs the pinned image, unprivileged, on a private network,
	// with no port published on the host.
	container := func(service string) string {
		t.Helper()
		id := docker("ps", "--all", "--quiet", "--filter", "label=com.docker.compose.project="+project, "--filter", "label=com.docker.compose.service="+service)
		require.NotEmpty(t, id, "no container for %s", service)
		return id
	}
	appContainer, tunnelContainer := container("app-"+assignment), container("tunnel-"+assignment)
	require.Equal(t, busybox+" false "+project+"_net-"+assignment+" map[]",
		docker("inspect", "--format", "{{.Config.Image}} {{.HostConfig.Privileged}} {{.HostConfig.NetworkMode}} {{.HostConfig.PortBindings}}", appContainer))
	require.Contains(t, docker("inspect", "--format", "{{.Config.Env}}", appContainer), "NOTE=costs $5")
	// The tunnel is the real one, told the machine's credential by the agent.
	command := docker("inspect", "--format", "{{.Config.Image}} {{.Path}} {{.Args}}", tunnelContainer)
	require.Contains(t, command, tunnel+" "+compose.TunnelBinary)
	require.Contains(t, command, "-credential "+credential.Secret)
	require.Contains(t, command, "-target http://app-"+assignment+":8000")
	require.Contains(t, command, "-app echo")

	// The operator unassigns the app. The agent takes both containers down.
	e.ok(http.StatusOK, "PUT", "/v1/admin/machines/"+machine.ID+"/assignments", admin, map[string]any{"assignments": []map[string]any{}}, nil)
	eventually(func() bool {
		r := reported()
		return r != nil && len(r.Containers) == 0 && docker("ps", "--all", "--quiet", "--filter", "label=com.docker.compose.project="+project) == ""
	}, time.Minute, "the agent did not take the app down")
}
