package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/Livepeer-Open-Pool/pool/internal/compose"
)

// ProjectFor is the Compose project an agent keeps its containers in. It is
// derived from the pool's address, so a machine that joined two regions runs
// an agent for each without one removing the other's containers.
func ProjectFor(poolURL string) string {
	sum := sha256.Sum256([]byte(strings.ToLower(strings.TrimRight(strings.TrimSpace(poolURL), "/"))))
	return compose.Project + "-" + hex.EncodeToString(sum[:4])
}

// CLI drives Docker through the docker command and its compose plugin.
type CLI struct {
	// Project is the Compose project; empty means the plain "openpool".
	Project string
	// UpTimeout bounds one "up", which includes pulling images that can be
	// tens of gigabytes; zero means two hours.
	UpTimeout time.Duration
}

func (c CLI) project() string {
	if c.Project != "" {
		return c.Project
	}
	return compose.Project
}

// run runs docker compose for the project; verb names the call in errors.
// Only what Docker itself needs is passed on from the agent's environment,
// plus extra.
func (c CLI) run(ctx context.Context, verb string, stdin []byte, extra []string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "docker", append([]string{"compose", "-p", c.project()}, args...)...)
	for _, name := range []string{"PATH", "HOME", "DOCKER_HOST", "DOCKER_CONFIG", "DOCKER_CERT_PATH", "DOCKER_TLS_VERIFY"} {
		if v, ok := os.LookupEnv(name); ok {
			cmd.Env = append(cmd.Env, name+"="+v)
		}
	}
	cmd.Env = append(cmd.Env, extra...)
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("docker compose %s: %w: %s", verb, err, lastLines(stderr.String(), 5))
	}
	return stdout.Bytes(), nil
}

// lastLines keeps the end of a command's output, where the error is.
func lastLines(s string, n int) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, " | ")
}

func (c CLI) Up(ctx context.Context, file []byte, credential string) error {
	timeout := c.UpTimeout
	if timeout == 0 {
		timeout = 2 * time.Hour
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	_, err := c.run(ctx, "up", file, []string{compose.CredentialVariable + "=" + credential},
		"-f", "-", "up", "--detach", "--remove-orphans", "--quiet-pull")
	return err
}

func (c CLI) Down(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	_, err := c.run(ctx, "down", nil, nil, "down", "--remove-orphans")
	return err
}

func (c CLI) Containers(ctx context.Context) ([]Container, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	out, err := c.run(ctx, "ps", nil, nil, "ps", "--all", "--format", "json")
	if err != nil {
		return nil, err
	}
	return parseContainers(out)
}

// parseContainers reads "docker compose ps --format json", which is one JSON
// object per line in current versions and a single array in older ones.
func parseContainers(out []byte) ([]Container, error) {
	type row struct {
		Service, State, Health string
	}
	var rows []row
	out = bytes.TrimSpace(out)
	if len(out) > 0 && out[0] == '[' {
		if err := json.Unmarshal(out, &rows); err != nil {
			return nil, fmt.Errorf("reading the container list: %w", err)
		}
	} else {
		dec := json.NewDecoder(bytes.NewReader(out))
		for dec.More() {
			var r row
			if err := dec.Decode(&r); err != nil {
				return nil, fmt.Errorf("reading the container list: %w", err)
			}
			rows = append(rows, r)
		}
	}
	containers := make([]Container, 0, len(rows))
	for _, r := range rows {
		containers = append(containers, Container{Service: r.Service, State: r.State, Health: r.Health})
	}
	return containers, nil
}

// NvidiaGPUs lists the machine's GPUs with nvidia-smi, which the NVIDIA
// container runtime provides inside the agent's container when the agent is
// started with --gpus all.
func NvidiaGPUs(ctx context.Context) ([]GPU, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "nvidia-smi", "--query-gpu=name,memory.total", "--format=csv,noheader,nounits").Output()
	if err != nil {
		return nil, fmt.Errorf("nvidia-smi did not run; start the agent with --gpus all on a machine with the NVIDIA container toolkit: %w", err)
	}
	return parseGPUs(string(out))
}

func parseGPUs(out string) ([]GPU, error) {
	gpus := []GPU{}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		i := strings.LastIndex(line, ",")
		if i < 0 {
			return nil, fmt.Errorf("unexpected nvidia-smi output %q", line)
		}
		memory, err := strconv.Atoi(strings.TrimSpace(line[i+1:]))
		if err != nil {
			return nil, fmt.Errorf("unexpected nvidia-smi output %q", line)
		}
		gpus = append(gpus, GPU{Name: strings.TrimSpace(line[:i]), MemoryMB: memory})
	}
	return gpus, nil
}
