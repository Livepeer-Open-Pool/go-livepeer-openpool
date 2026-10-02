package compose

import (
	"crypto/ed25519"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	appImage    = "vllm/vllm-openai@sha256:" + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	tunnelImage = "tztcloud/livepeer-runner-tunnel@sha256:" + "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

func spec() Spec {
	return Spec{
		OrchestratorURL: "https://orch.us-east.open-pool.com:8935",
		TunnelImage:     tunnelImage,
		Apps: []App{{
			ID: "asg_abc", CacheID: "app_vllm", Name: "vllm/qwen2.5-7b-instruct", Mode: "single-shot", Version: "1.2.0", Capacity: 4,
			Image: appImage, Command: []string{"--model", "Qwen/Qwen2.5-7B-Instruct"}, Env: map[string]string{"HF_HOME": "/cache", "PROMPT": "cost: $5"},
			Port: 8000, HealthPath: "/health", ShmSize: "8g", CachePath: "/cache",
		}},
	}
}

func TestGeneratedFileIsWhatAMachineRuns(t *testing.T) {
	data, err := Generate(spec())
	require.NoError(t, err)
	again, err := Generate(spec())
	require.NoError(t, err)
	require.Equal(t, string(data), string(again), "the same spec must give the same bytes")

	file, err := Parse(data)
	require.NoError(t, err)
	app, tunnel := file.Services["app-asg_abc"], file.Services["tunnel-asg_abc"]
	require.Equal(t, appImage, app.Image)
	require.Equal(t, []string{"--model", "Qwen/Qwen2.5-7B-Instruct"}, app.Command)
	require.Equal(t, "cost: $$5", app.Environment["PROMPT"], "a $ of the operator's is not a variable")
	require.Equal(t, []string{"cache-app_vllm:/cache"}, app.Volumes)
	require.Equal(t, "all", app.Deploy.Resources.Reservations.Devices[0].Count)
	require.Equal(t, []string{"net-asg_abc"}, app.Networks)

	require.Equal(t, tunnelImage, tunnel.Image)
	require.Equal(t, []string{TunnelBinary}, tunnel.Entrypoint)
	require.Equal(t, []string{
		"-orchestrator", "https://orch.us-east.open-pool.com:8935",
		"-credential", "${OPENPOOL_CREDENTIAL}",
		"-target", "http://app-asg_abc:8000",
		"-app", "vllm/qwen2.5-7b-instruct",
		"-mode", "single-shot",
		"-capacity", "4",
		"-version", "1.2.0",
		"-health", "/health",
	}, tunnel.Command)
	require.Nil(t, tunnel.Deploy, "the tunnel needs no GPU")
	require.NotContains(t, string(data), "ports", "no port is published on the host")
	require.NotContains(t, string(data), "opc_", "the file holds no credential")
}

func TestGPUCountAndNoGPU(t *testing.T) {
	s := spec()
	s.Apps[0].GPUs = 2
	data, err := Generate(s)
	require.NoError(t, err)
	file, err := Parse(data)
	require.NoError(t, err)
	require.EqualValues(t, 2, file.Services["app-asg_abc"].Deploy.Resources.Reservations.Devices[0].Count)

	s.Apps[0].GPUs, s.Apps[0].NoGPU = 0, true
	data, err = Generate(s)
	require.NoError(t, err)
	file, err = Parse(data)
	require.NoError(t, err)
	require.Nil(t, file.Services["app-asg_abc"].Deploy)
}

func TestAMachineWithNothingAssignedGetsAnEmptyFile(t *testing.T) {
	s := spec()
	s.Apps = nil
	data, err := Generate(s)
	require.NoError(t, err)
	file, err := Parse(data)
	require.NoError(t, err)
	require.Empty(t, file.Services)
}

func TestGenerateRefusesWhatAgentsWouldRefuse(t *testing.T) {
	s := spec()
	s.TunnelImage = "tztcloud/livepeer-runner-tunnel:latest"
	_, err := Generate(s)
	require.ErrorContains(t, err, "pinned by digest")

	s = spec()
	s.Apps[0].Image = "vllm/vllm-openai:latest"
	_, err = Generate(s)
	require.ErrorContains(t, err, "not pinned by digest")

	s = spec()
	s.Apps = append(s.Apps, s.Apps[0])
	_, err = Generate(s)
	require.ErrorContains(t, err, "share the id")
}

// edit returns the generated file with one change made to its JSON.
func edit(t *testing.T, change func(file map[string]any, app map[string]any)) []byte {
	t.Helper()
	data, err := Generate(spec())
	require.NoError(t, err)
	var file map[string]any
	require.NoError(t, json.Unmarshal(data, &file))
	change(file, file["services"].(map[string]any)["app-asg_abc"].(map[string]any))
	out, err := json.Marshal(file)
	require.NoError(t, err)
	return out
}

func TestAgentRefusesWhatReachesOutsideTheContainers(t *testing.T) {
	for name, tc := range map[string]struct {
		change func(file, app map[string]any)
		want   string
	}{
		"privileged":        {func(_, app map[string]any) { app["privileged"] = true }, `unknown field "privileged"`},
		"host network":      {func(_, app map[string]any) { app["network_mode"] = "host" }, `unknown field "network_mode"`},
		"host pid":          {func(_, app map[string]any) { app["pid"] = "host" }, `unknown field "pid"`},
		"added capability":  {func(_, app map[string]any) { app["cap_add"] = []string{"SYS_ADMIN"} }, `unknown field "cap_add"`},
		"device":            {func(_, app map[string]any) { app["devices"] = []string{"/dev/sda:/dev/sda"} }, `unknown field "devices"`},
		"published port":    {func(_, app map[string]any) { app["ports"] = []string{"8000:8000"} }, `unknown field "ports"`},
		"host path":         {func(_, app map[string]any) { app["volumes"] = []string{"/:/host"} }, "does not mount a volume declared in the file"},
		"docker socket":     {func(_, app map[string]any) { app["volumes"] = []string{"/var/run/docker.sock:/var/run/docker.sock"} }, "does not mount a volume declared in the file"},
		"relative path":     {func(_, app map[string]any) { app["volumes"] = []string{"./x:/x"} }, "does not mount a volume declared in the file"},
		"undeclared volume": {func(_, app map[string]any) { app["volumes"] = []string{"other:/x"} }, "does not mount a volume declared in the file"},
		"volume options":    {func(_, app map[string]any) { app["volumes"] = []string{"cache-app_vllm:/cache:shared"} }, "with no options"},
		"bind through a volume driver": {func(file, _ map[string]any) {
			file["volumes"].(map[string]any)["cache-app_vllm"] = map[string]any{"driver_opts": map[string]string{"type": "none", "o": "bind", "device": "/"}}
		}, `unknown field "driver_opts"`},
		"external network": {func(file, _ map[string]any) {
			file["networks"].(map[string]any)["net-asg_abc"] = map[string]any{"external": true}
		}, `unknown field "external"`},
		"undeclared network": {func(_, app map[string]any) { app["networks"] = []string{"host"} }, `network "host" is not declared`},
		"unpinned image":     {func(_, app map[string]any) { app["image"] = "ubuntu:latest" }, "not pinned by digest"},
		"another project":    {func(file, _ map[string]any) { file["name"] = "other" }, "must be named"},
		"other device driver": {func(_, app map[string]any) {
			app["deploy"] = map[string]any{"resources": map[string]any{"reservations": map[string]any{"devices": []any{map[string]any{"driver": "cdi", "count": "all", "capabilities": []string{"gpu"}}}}}}
		}, "only NVIDIA GPUs"},
		"top-level include": {func(file, _ map[string]any) { file["include"] = []string{"/etc/other.yml"} }, `unknown field "include"`},
		"secrets": {func(file, _ map[string]any) {
			file["secrets"] = map[string]any{"s": map[string]string{"file": "/etc/shadow"}}
		}, `unknown field "secrets"`},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Parse(edit(t, tc.change))
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestSignatureCoversTheFileTheMachineAndTheTime(t *testing.T) {
	key, err := LoadOrCreateKey(filepath.Join(t.TempDir(), "compose.key"))
	require.NoError(t, err)
	public := key.Public().(ed25519.PublicKey)
	data, err := Generate(spec())
	require.NoError(t, err)
	signed := Sign(key, "mch_1", time.Unix(1_800_000_000, 0), data)

	file, err := Verify(public, signed)
	require.NoError(t, err)
	require.Len(t, file.Services, 2)

	for name, tamper := range map[string]func(*Signed){
		"file":      func(s *Signed) { s.Compose = strings.Replace(s.Compose, "-capacity", "-Capacity", 1) },
		"machine":   func(s *Signed) { s.MachineID = "mch_2" },
		"time":      func(s *Signed) { s.IssuedAt++ },
		"signature": func(s *Signed) { s.Signature = "AAAA" },
	} {
		t.Run(name, func(t *testing.T) {
			tampered := signed
			tamper(&tampered)
			_, err := Verify(public, tampered)
			require.ErrorContains(t, err, "not signed by this pool's key")
		})
	}

	other, err := LoadOrCreateKey(filepath.Join(t.TempDir(), "other.key"))
	require.NoError(t, err)
	_, err = Verify(other.Public().(ed25519.PublicKey), signed)
	require.ErrorContains(t, err, "not signed by this pool's key")
}

func TestASignedFileIsStillCheckedAgainstTheAllowlist(t *testing.T) {
	key, err := LoadOrCreateKey(filepath.Join(t.TempDir(), "compose.key"))
	require.NoError(t, err)
	bad := edit(t, func(_, app map[string]any) { app["privileged"] = true })
	_, err = Verify(key.Public().(ed25519.PublicKey), Sign(key, "mch_1", time.Now(), bad))
	require.ErrorContains(t, err, `unknown field "privileged"`)
}

func TestKeyFileIsCreatedOnceAndReadBack(t *testing.T) {
	path := filepath.Join(t.TempDir(), "compose.key")
	first, err := LoadOrCreateKey(path)
	require.NoError(t, err)
	second, err := LoadOrCreateKey(path)
	require.NoError(t, err)
	require.Equal(t, first, second)

	encoded := EncodePublicKey(first.Public().(ed25519.PublicKey))
	require.Len(t, encoded, 43)
	decoded, err := ParsePublicKey(encoded)
	require.NoError(t, err)
	require.Equal(t, first.Public(), decoded)
	_, err = ParsePublicKey("short")
	require.Error(t, err)
}
