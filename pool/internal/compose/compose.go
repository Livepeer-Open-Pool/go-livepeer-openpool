// Package compose builds, signs and checks the Docker Compose file that
// tells a member's machine what to run. The pool service generates and signs
// it; the member agent verifies the signature and then checks the file
// against an allowlist of its own, so that even a correctly signed file
// cannot ask for more than app containers on private networks.
//
// The package is shared by the service and the agent and depends on neither.
package compose

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Project is the name every generated file carries. An agent applies the
// file under a project name of its own that starts with this one.
const Project = "openpool"

// CredentialVariable is the environment variable the file reads the
// machine's credential from. The agent supplies it when it applies the file,
// so the signed file itself holds no secret.
const CredentialVariable = "OPENPOOL_CREDENTIAL"

// TunnelBinary is where livepeer_runner_tunnel sits in the tunnel image.
const TunnelBinary = "/usr/local/bin/livepeer_runner_tunnel"

// App is one app a machine runs, with the container that serves it.
type App struct {
	// ID names the app's containers and network; it is unique on the machine.
	ID string
	// CacheID names the volume the app's model files are kept in. It stays
	// the same across versions so a new version does not download them again.
	CacheID  string
	Name     string
	Mode     string
	Version  string
	Capacity int

	Image      string
	Command    []string
	Env        map[string]string
	Port       int
	HealthPath string
	// GPUs is how many GPUs the container gets; 0 means all of them, unless NoGPU.
	GPUs      int
	NoGPU     bool
	ShmSize   string
	CachePath string
}

// Spec is everything one machine runs.
type Spec struct {
	// OrchestratorURL is where the tunnels register.
	OrchestratorURL string
	// TunnelImage is the image holding livepeer_runner_tunnel, pinned by digest.
	TunnelImage string
	Apps        []App
}

// File is a Compose file restricted to what a pool may ask of a member's
// machine. Decoding is strict: a key that is not here is refused.
type File struct {
	Name     string              `json:"name"`
	Services map[string]Service  `json:"services"`
	Networks map[string]struct{} `json:"networks,omitempty"`
	Volumes  map[string]struct{} `json:"volumes,omitempty"`
}

// Service is one container.
type Service struct {
	Image       string            `json:"image"`
	Entrypoint  []string          `json:"entrypoint,omitempty"`
	Command     []string          `json:"command,omitempty"`
	Environment map[string]string `json:"environment,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	Restart     string            `json:"restart"`
	ShmSize     string            `json:"shm_size,omitempty"`
	Volumes     []string          `json:"volumes,omitempty"`
	Networks    []string          `json:"networks"`
	DependsOn   []string          `json:"depends_on,omitempty"`
	Deploy      *Deploy           `json:"deploy,omitempty"`
}

// Deploy carries the one thing a service may reserve: GPUs.
type Deploy struct {
	Resources Resources `json:"resources"`
}

type Resources struct {
	Reservations Reservations `json:"reservations"`
}

type Reservations struct {
	Devices []Device `json:"devices"`
}

// Device is a GPU reservation. Count is a number or "all".
type Device struct {
	Driver       string   `json:"driver"`
	Count        any      `json:"count"`
	Capabilities []string `json:"capabilities"`
}

var (
	pinnedImage = regexp.MustCompile(`^[a-z0-9][a-z0-9._:/-]*@sha256:[0-9a-f]{64}$`)
	identifier  = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)
	shmSize     = regexp.MustCompile(`^[0-9]+(b|k|m|g|kb|mb|gb)?$`)
)

// literal makes s safe from Compose's variable interpolation.
func literal(s string) string { return strings.ReplaceAll(s, "$", "$$") }

func literals(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, len(in))
	for i, s := range in {
		out[i] = literal(s)
	}
	return out
}

// Generate returns the Compose file for spec. The same spec always gives
// the same bytes, so a file changes only when what the machine runs does.
func Generate(spec Spec) ([]byte, error) {
	if !pinnedImage.MatchString(spec.TunnelImage) {
		return nil, fmt.Errorf("compose: the tunnel image must be pinned by digest, as name@sha256:<64 hex digits>")
	}
	if spec.OrchestratorURL == "" {
		return nil, fmt.Errorf("compose: the orchestrator's address is missing")
	}
	file := File{Name: Project, Services: map[string]Service{}}
	for _, app := range spec.Apps {
		if !identifier.MatchString(app.ID) || !identifier.MatchString(app.CacheID) {
			return nil, fmt.Errorf("compose: %q and %q must be lowercase identifiers", app.ID, app.CacheID)
		}
		appService, tunnelService, network := "app-"+app.ID, "tunnel-"+app.ID, "net-"+app.ID
		if _, taken := file.Services[appService]; taken {
			return nil, fmt.Errorf("compose: two apps share the id %q", app.ID)
		}
		if file.Networks == nil {
			file.Networks = map[string]struct{}{}
		}
		file.Networks[network] = struct{}{}
		labels := map[string]string{"openpool.app": literal(app.Name), "openpool.version": literal(app.Version)}

		service := Service{
			Image:    app.Image,
			Command:  literals(app.Command),
			Labels:   labels,
			Restart:  "unless-stopped",
			ShmSize:  app.ShmSize,
			Networks: []string{network},
		}
		if len(app.Env) > 0 {
			service.Environment = map[string]string{}
			for k, v := range app.Env {
				service.Environment[k] = literal(v)
			}
		}
		if app.CachePath != "" {
			volume := "cache-" + app.CacheID
			if file.Volumes == nil {
				file.Volumes = map[string]struct{}{}
			}
			file.Volumes[volume] = struct{}{}
			service.Volumes = []string{volume + ":" + app.CachePath}
		}
		if !app.NoGPU {
			var count any = "all"
			if app.GPUs > 0 {
				count = app.GPUs
			}
			service.Deploy = &Deploy{Resources{Reservations{[]Device{{Driver: "nvidia", Count: count, Capabilities: []string{"gpu"}}}}}}
		}
		file.Services[appService] = service

		command := []string{
			"-orchestrator", literal(spec.OrchestratorURL),
			"-credential", "${" + CredentialVariable + "}",
			"-target", "http://" + appService + ":" + strconv.Itoa(app.Port),
			"-app", literal(app.Name),
			"-mode", app.Mode,
			"-capacity", strconv.Itoa(app.Capacity),
			"-version", literal(app.Version),
			"-health", app.HealthPath,
		}
		file.Services[tunnelService] = Service{
			Image:      spec.TunnelImage,
			Entrypoint: []string{TunnelBinary},
			Command:    command,
			Labels:     labels,
			Restart:    "unless-stopped",
			Networks:   []string{network},
			DependsOn:  []string{appService},
		}
	}
	data, err := json.MarshalIndent(file, "", "  ")
	if err != nil {
		return nil, err
	}
	if _, err := Parse(data); err != nil {
		return nil, fmt.Errorf("compose: the generated file would be refused by agents: %w", err)
	}
	return data, nil
}

// Parse decodes a Compose file and checks it against what an agent accepts.
// It refuses anything that could reach outside the app's containers:
// privileged mode, the host's network or process space, added capabilities,
// devices, published ports and mounts of host paths are not keys this
// package knows, and unknown keys are refused; images must be pinned by
// digest; volumes and networks must be plain ones declared in the file.
func Parse(data []byte) (*File, error) {
	var file File
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&file); err != nil {
		return nil, fmt.Errorf("not an accepted compose file: %w", err)
	}
	if dec.More() {
		return nil, fmt.Errorf("not an accepted compose file: content after the document")
	}
	if file.Name != Project {
		return nil, fmt.Errorf("the project must be named %q, not %q", Project, file.Name)
	}
	for name := range file.Networks {
		if !identifier.MatchString(name) {
			return nil, fmt.Errorf("network %q is not a plain name", name)
		}
	}
	for name := range file.Volumes {
		if !identifier.MatchString(name) {
			return nil, fmt.Errorf("volume %q is not a plain name", name)
		}
	}
	for name, service := range file.Services {
		if !identifier.MatchString(name) {
			return nil, fmt.Errorf("service %q is not a plain name", name)
		}
		if !pinnedImage.MatchString(service.Image) {
			return nil, fmt.Errorf("service %s: image %q is not pinned by digest", name, service.Image)
		}
		if service.Restart != "unless-stopped" && service.Restart != "no" && service.Restart != "on-failure" {
			return nil, fmt.Errorf("service %s: restart policy %q is not accepted", name, service.Restart)
		}
		if service.ShmSize != "" && !shmSize.MatchString(service.ShmSize) {
			return nil, fmt.Errorf("service %s: shared memory size %q is not accepted", name, service.ShmSize)
		}
		if len(service.Networks) == 0 {
			return nil, fmt.Errorf("service %s: is on no network of this file", name)
		}
		for _, network := range service.Networks {
			if _, declared := file.Networks[network]; !declared {
				return nil, fmt.Errorf("service %s: network %q is not declared in the file", name, network)
			}
		}
		for _, mount := range service.Volumes {
			volume, path, ok := strings.Cut(mount, ":")
			if _, declared := file.Volumes[volume]; !ok || !declared {
				return nil, fmt.Errorf("service %s: %q does not mount a volume declared in the file", name, mount)
			}
			if !strings.HasPrefix(path, "/") || strings.Contains(path, ":") {
				return nil, fmt.Errorf("service %s: %q must mount at an absolute path, with no options", name, mount)
			}
		}
		for _, other := range service.DependsOn {
			if _, declared := file.Services[other]; !declared {
				return nil, fmt.Errorf("service %s: depends on %q, which is not in the file", name, other)
			}
		}
		if service.Deploy != nil {
			for _, device := range service.Deploy.Resources.Reservations.Devices {
				if device.Driver != "nvidia" || len(device.Capabilities) != 1 || device.Capabilities[0] != "gpu" {
					return nil, fmt.Errorf("service %s: only NVIDIA GPUs may be reserved", name)
				}
				switch count := device.Count.(type) {
				case string:
					if count != "all" {
						return nil, fmt.Errorf("service %s: GPU count %q is not accepted", name, count)
					}
				case float64:
					if count < 1 || count != float64(int(count)) {
						return nil, fmt.Errorf("service %s: GPU count %v is not accepted", name, count)
					}
				default:
					return nil, fmt.Errorf("service %s: GPU count is missing", name)
				}
			}
		}
	}
	return &file, nil
}
