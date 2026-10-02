package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Livepeer-Open-Pool/pool/internal/compose"
)

// TestCLIAgainstRealDocker drives a real Docker daemon. It pulls busybox and
// starts containers, so it runs only when asked to:
//
//	OPENPOOL_DOCKER_TEST=1 go test ./internal/agent -run RealDocker
func TestCLIAgainstRealDocker(t *testing.T) {
	if os.Getenv("OPENPOOL_DOCKER_TEST") == "" {
		t.Skip("set OPENPOOL_DOCKER_TEST=1 to run against a real Docker daemon")
	}
	docker := func(args ...string) string {
		t.Helper()
		out, err := exec.Command("docker", args...).CombinedOutput()
		require.NoError(t, err, "docker %s: %s", strings.Join(args, " "), out)
		return strings.TrimSpace(string(out))
	}
	docker("pull", "--quiet", "busybox:stable")
	image := docker("image", "inspect", "--format", "{{index .RepoDigests 0}}", "busybox:stable")

	// The file a pool would sign, with busybox standing in for the app and the tunnel.
	file := func(services ...string) []byte {
		f := compose.File{Name: compose.Project, Services: map[string]compose.Service{}, Networks: map[string]struct{}{"net-a": {}}}
		for _, name := range services {
			f.Services[name] = compose.Service{
				Image: image, Entrypoint: []string{"sleep"}, Command: []string{"300"}, Restart: "unless-stopped", Networks: []string{"net-a"},
				Labels: map[string]string{"openpool.credential-seen": "${" + compose.CredentialVariable + "}", "openpool.price": "$$5"},
			}
		}
		data, err := json.Marshal(f)
		require.NoError(t, err)
		_, err = compose.Parse(data)
		require.NoError(t, err)
		return data
	}

	ctx := context.Background()
	cli := CLI{Project: fmt.Sprintf("openpool-test-%d", time.Now().UnixNano())}
	t.Cleanup(func() { _ = cli.Down(ctx) })

	containers, err := cli.Containers(ctx)
	require.NoError(t, err)
	require.Empty(t, containers)

	require.NoError(t, cli.Up(ctx, file("app-a", "tunnel-a"), "opc_secret"))
	containers, err = cli.Containers(ctx)
	require.NoError(t, err)
	require.Len(t, containers, 2)
	for _, c := range containers {
		require.Equal(t, "running", c.State, c.Service)
	}
	// The credential comes from the agent's environment, and a $ of the
	// operator's stays a $.
	labels := docker("inspect", "--format", `{{index .Config.Labels "openpool.credential-seen"}} {{index .Config.Labels "openpool.price"}}`, cli.Project+"-tunnel-a-1")
	require.Equal(t, "opc_secret $5", labels)

	// Applying the same file again changes nothing.
	started := docker("inspect", "--format", "{{.State.StartedAt}}", cli.Project+"-app-a-1")
	require.NoError(t, cli.Up(ctx, file("app-a", "tunnel-a"), "opc_secret"))
	require.Equal(t, started, docker("inspect", "--format", "{{.State.StartedAt}}", cli.Project+"-app-a-1"))

	// A service no longer in the file is removed.
	require.NoError(t, cli.Up(ctx, file("app-a"), "opc_secret"))
	containers, err = cli.Containers(ctx)
	require.NoError(t, err)
	require.Equal(t, []Container{{Service: "app-a", State: "running"}}, containers)

	require.NoError(t, cli.Down(ctx))
	containers, err = cli.Containers(ctx)
	require.NoError(t, err)
	require.Empty(t, containers)

	// What Docker refuses comes back as an error the member can read.
	err = cli.Up(ctx, []byte(`{"name":"openpool","services":{"app-a":{"image":"openpool.invalid/none@sha256:`+strings.Repeat("0", 64)+`","restart":"no","networks":["net-a"]}},"networks":{"net-a":{}}}`), "opc_secret")
	require.ErrorContains(t, err, "docker compose up:")
}
