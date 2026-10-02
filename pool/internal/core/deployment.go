package core

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

// DeployedApp is one app a machine is told to run, at the version and with
// the container the pool last told it.
type DeployedApp struct {
	AssignmentID string
	AppID        string
	App          string
	Mode         string
	Version      string
	Capacity     int
	Container    Container
}

// Deployment is everything a machine is told to run.
type Deployment struct {
	MachineID string
	Apps      []DeployedApp
}

// AgentContainer is a container on a member's machine, as its agent reports it.
type AgentContainer struct {
	Service string `json:"service"`
	State   string `json:"state"`
	Health  string `json:"health,omitempty"`
}

// AgentGPU is a GPU the agent found on the machine.
type AgentGPU struct {
	Name     string `json:"name"`
	MemoryMB int    `json:"memory_mb"`
}

// AgentReport is what a machine's agent says about the machine.
type AgentReport struct {
	AgentVersion string `json:"agent_version"`
	// Generation identifies the compose file the agent last applied.
	Generation string `json:"generation"`
	// Error is why the agent could not apply what it was told to, if it could not.
	Error      string           `json:"error,omitempty"`
	Containers []AgentContainer `json:"containers"`
	GPUs       []AgentGPU       `json:"gpus"`
	// GPUError is why the GPUs could not be listed.
	GPUError   string     `json:"gpu_error,omitempty"`
	ReportedAt *time.Time `json:"reported_at,omitempty"`
}

// machineForCredential finds the machine a credential belongs to. A
// credential that is revoked, or whose machine or member is no longer in
// good standing, is as good as unknown.
func (s *Service) machineForCredential(ctx context.Context, credential string) (string, error) {
	var machineID, machineStatus, memberStatus string
	err := s.db.QueryRowContext(ctx, `SELECT m.id, m.status, b.status FROM credentials c
		JOIN machines m ON m.id = c.machine_id JOIN members b ON b.id = m.member_id
		WHERE c.secret_hash = ? AND c.revoked_at = 0`, hashSecret(credential)).Scan(&machineID, &machineStatus, &memberStatus)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && (machineStatus != MachineApproved || memberStatus != MemberActive)) {
		return "", forbiddenf("unknown credential")
	}
	return machineID, err
}

// Deployment returns what the machine presenting credential is told to run:
// its assignments that have not ended, each at the version and container it
// was last moved to. A draining assignment stays until its runner is gone,
// which is what lets its sessions finish.
func (s *Service) Deployment(ctx context.Context, credential string) (*Deployment, error) {
	machineID, err := s.machineForCredential(ctx, credential)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT a.id, p.id, p.name, p.mode, a.capacity, a.deployed_version, a.deployed_container, p.required_version, p.container
		FROM assignments a JOIN apps p ON p.id = a.app_id WHERE a.machine_id = ? AND a.status != ? ORDER BY a.id`, machineID, AssignmentRemoved)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	deployment := &Deployment{MachineID: machineID, Apps: []DeployedApp{}}
	for rows.Next() {
		var app DeployedApp
		var deployedContainer, currentVersion, currentContainer string
		if err := rows.Scan(&app.AssignmentID, &app.AppID, &app.App, &app.Mode, &app.Capacity, &app.Version, &deployedContainer, &currentVersion, &currentContainer); err != nil {
			return nil, err
		}
		if app.Version == "" || deployedContainer == "" {
			app.Version, deployedContainer = currentVersion, currentContainer
		}
		if err := json.Unmarshal([]byte(deployedContainer), &app.Container); err != nil {
			return nil, err
		}
		deployment.Apps = append(deployment.Apps, app)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	_, err = s.db.ExecContext(ctx, `UPDATE machines SET last_seen_at = ? WHERE id = ?`, s.nowMs(), machineID)
	return deployment, err
}

// RecordAgentReport stores what a machine's agent says about it.
func (s *Service) RecordAgentReport(ctx context.Context, credential string, report AgentReport) error {
	machineID, err := s.machineForCredential(ctx, credential)
	if err != nil {
		return err
	}
	now := s.now()
	report.ReportedAt = &now
	if report.Containers == nil {
		report.Containers = []AgentContainer{}
	}
	if report.GPUs == nil {
		report.GPUs = []AgentGPU{}
	}
	_, err = s.db.ExecContext(ctx, `UPDATE machines SET reported = ?, last_seen_at = ? WHERE id = ?`, mustJSON(report), now.UnixMilli(), machineID)
	return err
}
