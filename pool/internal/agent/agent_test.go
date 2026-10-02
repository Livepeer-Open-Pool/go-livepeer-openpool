package agent

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Livepeer-Open-Pool/pool/internal/compose"
)

const (
	appImage    = "vllm/vllm-openai@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	tunnelImage = "tztcloud/livepeer-runner-tunnel@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

// pool stands in for the pool service: it answers with whatever answer is set.
type pool struct {
	mu      sync.Mutex
	key     ed25519.PrivateKey
	status  int
	answer  compose.Signed
	reports []Report
	server  *httptest.Server
}

func newPool(t *testing.T) *pool {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	p := &pool{key: key, status: http.StatusOK}
	p.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.mu.Lock()
		defer p.mu.Unlock()
		if r.Header.Get("Authorization") != "Bearer opc_secret" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if p.status != http.StatusOK {
			w.WriteHeader(p.status)
			json.NewEncoder(w).Encode(map[string]string{"error": "the database is busy"})
			return
		}
		if r.URL.Path == "/v1/agent/report" {
			var report Report
			require.NoError(t, json.NewDecoder(r.Body).Decode(&report))
			p.reports = append(p.reports, report)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		json.NewEncoder(w).Encode(p.answer)
	}))
	t.Cleanup(p.server.Close)
	return p
}

// assign makes the pool's answer a file running n apps, issued at the time.
func (p *pool) assign(t *testing.T, machineID string, issuedAt time.Time, n int) {
	t.Helper()
	spec := compose.Spec{OrchestratorURL: "https://orch:8935", TunnelImage: tunnelImage}
	for i := 0; i < n; i++ {
		id := string(rune('a' + i))
		spec.Apps = append(spec.Apps, compose.App{ID: id, CacheID: id, Name: "app/" + id, Mode: "single-shot", Version: "1.0.0", Capacity: 1, Image: appImage, Port: 8000})
	}
	file, err := compose.Generate(spec)
	require.NoError(t, err)
	p.mu.Lock()
	defer p.mu.Unlock()
	p.answer = compose.Sign(p.key, machineID, issuedAt, file)
}

func (p *pool) lastReport(t *testing.T) Report {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	require.NotEmpty(t, p.reports)
	return p.reports[len(p.reports)-1]
}

// docker records what the agent asks of Docker.
type docker struct {
	ups, downs int
	file       []byte
	fail       error
}

func (d *docker) Up(ctx context.Context, file []byte, credential string) error {
	if d.fail != nil {
		return d.fail
	}
	d.ups, d.file = d.ups+1, file
	return nil
}

func (d *docker) Down(ctx context.Context) error {
	d.downs, d.file = d.downs+1, nil
	return nil
}

func (d *docker) Containers(ctx context.Context) ([]Container, error) {
	if d.file == nil {
		return nil, nil
	}
	return []Container{{Service: "app-a", State: "running"}}, nil
}

func newAgent(p *pool, d *docker, now *time.Time) *Agent {
	return &Agent{
		PoolURL: p.server.URL, Credential: "opc_secret", Key: p.key.Public().(ed25519.PublicKey), Docker: d,
		Now: func() time.Time { return *now }, Log: slog.New(slog.DiscardHandler),
	}
}

func TestAppliesOnlyWhenSomethingChangedAndAgainAfterAWhile(t *testing.T) {
	p, d, now := newPool(t), &docker{}, time.Unix(1_800_000_000, 0)
	a := newAgent(p, d, &now)
	p.assign(t, "mch_1", now, 1)

	require.NoError(t, a.Step(context.Background()))
	require.NoError(t, a.Step(context.Background()))
	require.Equal(t, 1, d.ups)
	require.Equal(t, compose.Generation(d.file), p.lastReport(t).Generation)
	require.Equal(t, []Container{{Service: "app-a", State: "running"}}, p.lastReport(t).Containers)

	// The same file is applied again after a while, which brings back a
	// container someone removed by hand.
	now = now.Add(reapplyInterval)
	p.assign(t, "mch_1", now, 1)
	require.NoError(t, a.Step(context.Background()))
	require.Equal(t, 2, d.ups)

	// A changed file is applied at once.
	p.assign(t, "mch_1", now, 2)
	require.NoError(t, a.Step(context.Background()))
	require.Equal(t, 3, d.ups)
}

func TestKeepsRunningWhileThePoolIsUnreachable(t *testing.T) {
	p, d, now := newPool(t), &docker{}, time.Unix(1_800_000_000, 0)
	a := newAgent(p, d, &now)
	p.assign(t, "mch_1", now, 1)
	require.NoError(t, a.Step(context.Background()))

	p.mu.Lock()
	p.status = http.StatusServiceUnavailable
	p.mu.Unlock()
	now = now.Add(24 * time.Hour)
	require.ErrorContains(t, a.Step(context.Background()), "the pool answered 503: the database is busy")
	require.NotNil(t, d.file)
	require.Zero(t, d.downs)

	p.server.Close()
	require.Error(t, a.Step(context.Background()))
	require.NotNil(t, d.file)
}

func TestRefusesAFileForAnotherMachineOrAnOlderOne(t *testing.T) {
	p, d, now := newPool(t), &docker{}, time.Unix(1_800_000_000, 0)
	a := newAgent(p, d, &now)
	p.assign(t, "mch_1", now, 1)
	require.NoError(t, a.Step(context.Background()))

	p.assign(t, "mch_2", now.Add(time.Minute), 2)
	require.ErrorContains(t, a.Step(context.Background()), "it is for machine mch_2, not mch_1")
	p.assign(t, "mch_1", now.Add(-time.Hour), 2)
	require.ErrorContains(t, a.Step(context.Background()), "older than one already applied")
	require.Equal(t, 1, d.ups)
	require.Contains(t, p.lastReport(t).Error, "older than one already applied")
}

func TestAFailedApplyIsReportedAndTriedAgain(t *testing.T) {
	p, d, now := newPool(t), &docker{fail: errors.New("pull access denied")}, time.Unix(1_800_000_000, 0)
	a := newAgent(p, d, &now)
	p.assign(t, "mch_1", now, 1)

	require.ErrorContains(t, a.Step(context.Background()), "pull access denied")
	require.Contains(t, p.lastReport(t).Error, "pull access denied")
	require.Empty(t, p.lastReport(t).Generation)

	d.fail = nil
	require.NoError(t, a.Step(context.Background()))
	require.Equal(t, 1, d.ups)
	require.Empty(t, p.lastReport(t).Error)
}

func TestGPUsAreListedNowAndThen(t *testing.T) {
	p, d, now := newPool(t), &docker{}, time.Unix(1_800_000_000, 0)
	a := newAgent(p, d, &now)
	p.assign(t, "mch_1", now, 1)
	listed := 0
	a.ListGPUs = func(context.Context) ([]GPU, error) {
		listed++
		if listed == 1 {
			return nil, errors.New("nvidia-smi did not run")
		}
		return []GPU{{Name: "NVIDIA GeForce RTX 4090", MemoryMB: 24564}}, nil
	}
	require.NoError(t, a.Step(context.Background()))
	require.NoError(t, a.Step(context.Background()))
	require.Equal(t, 1, listed)
	require.Equal(t, "nvidia-smi did not run", p.lastReport(t).GPUError)
	require.Empty(t, p.lastReport(t).GPUs)

	now = now.Add(gpuInterval)
	p.assign(t, "mch_1", now, 1)
	require.NoError(t, a.Step(context.Background()))
	require.Equal(t, []GPU{{Name: "NVIDIA GeForce RTX 4090", MemoryMB: 24564}}, p.lastReport(t).GPUs)
	require.Empty(t, p.lastReport(t).GPUError)
}

func TestReadsDockersOutput(t *testing.T) {
	lines := `{"Service":"app-a","State":"running","Health":"healthy","Name":"openpool-app-a-1"}
{"Service":"tunnel-a","State":"restarting","Health":""}
`
	containers, err := parseContainers([]byte(lines))
	require.NoError(t, err)
	require.Equal(t, []Container{{Service: "app-a", State: "running", Health: "healthy"}, {Service: "tunnel-a", State: "restarting"}}, containers)

	containers, err = parseContainers([]byte(`[{"Service":"app-a","State":"exited"}]`))
	require.NoError(t, err)
	require.Equal(t, []Container{{Service: "app-a", State: "exited"}}, containers)
	containers, err = parseContainers(nil)
	require.NoError(t, err)
	require.Empty(t, containers)

	gpus, err := parseGPUs("NVIDIA GeForce RTX 4090, 24564\nNVIDIA RTX A6000, 49140\n")
	require.NoError(t, err)
	require.Equal(t, []GPU{{Name: "NVIDIA GeForce RTX 4090", MemoryMB: 24564}, {Name: "NVIDIA RTX A6000", MemoryMB: 49140}}, gpus)
	_, err = parseGPUs("No devices were found")
	require.Error(t, err)
}
