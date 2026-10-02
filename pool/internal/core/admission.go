package core

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// AdmissionRequest is what the orchestrator sends when a runner asks to
// register. It is the orchestrator's contract (see doc/live-runner-pool.md
// in go-livepeer), restated here because the two share no code.
type AdmissionRequest struct {
	Event       string          `json:"event"`
	OrchAddress string          `json:"orch_address"`
	RemoteAddr  string          `json:"remote_addr"`
	Credential  string          `json:"credential"`
	Runner      AdmissionRunner `json:"runner"`
}

// AdmissionRunner is the runner asking to register.
type AdmissionRunner struct {
	RunnerID string `json:"runner_id"`
	Tunnel   bool   `json:"tunnel"`
	Version  string `json:"version"`
	Status   string `json:"status"`
	Mode     string `json:"mode"`
	App      string `json:"app"`
	Capacity int    `json:"capacity"`
}

// AdmissionResponse is the pool service's answer.
type AdmissionResponse struct {
	Allow         bool   `json:"allow"`
	Reason        string `json:"reason,omitempty"`
	MemberID      string `json:"member_id,omitempty"`
	PayoutAddress string `json:"payout_address,omitempty"`
	MaxRunners    int    `json:"max_runners,omitempty"`
	State         string `json:"state,omitempty"`
}

// Runner states, as the orchestrator names them.
const (
	StateProbation = "probation"
	StateActive    = "active"
	StateSuspended = "suspended"
)

func deny(format string, args ...any) AdmissionResponse {
	return AdmissionResponse{Allow: false, Reason: fmt.Sprintf(format, args...)}
}

// Admit decides whether a runner may register. A machine's credential
// admits one runner for each app the operator assigned it, at the version
// and within the capacity the pool requires; anything else is refused with a
// reason the member's machine logs. An error means no decision was reached,
// and the orchestrator refuses the registration.
func (s *Service) Admit(ctx context.Context, req AdmissionRequest) (AdmissionResponse, error) {
	if req.Credential == "" || req.Runner.RunnerID == "" {
		return deny("unknown credential"), nil
	}
	var machineID, machineStatus, memberID, memberStatus, address string
	err := s.db.QueryRowContext(ctx, `SELECT m.id, m.status, b.id, b.status, b.address
		FROM credentials c JOIN machines m ON m.id = c.machine_id JOIN members b ON b.id = m.member_id
		WHERE c.secret_hash = ? AND c.revoked_at = 0`, hashSecret(req.Credential)).
		Scan(&machineID, &machineStatus, &memberID, &memberStatus, &address)
	if errors.Is(err, sql.ErrNoRows) {
		return deny("unknown credential"), nil
	}
	if err != nil {
		return AdmissionResponse{}, err
	}
	if memberStatus != MemberActive {
		return deny("this membership is %s", memberStatus), nil
	}
	if machineStatus != MachineApproved {
		return deny("this machine is %s", machineStatus), nil
	}

	app, err := s.appByName(ctx, req.Runner.App)
	var assignmentID, deployedVersion string
	var capacity int
	if err == nil {
		err = s.db.QueryRowContext(ctx, `SELECT id, capacity, deployed_version FROM assignments WHERE machine_id = ? AND app_id = ? AND status = ?`,
			machineID, app.ID, AssignmentActive).Scan(&assignmentID, &capacity, &deployedVersion)
	}
	var notFound *Error
	if errors.Is(err, sql.ErrNoRows) || (errors.As(err, &notFound) && notFound.Kind == NotFound) {
		return deny("this machine is not assigned %s", req.Runner.App), nil
	}
	if err != nil {
		return AdmissionResponse{}, err
	}
	if !app.Offered {
		return deny("%s is paused by the pool", app.Name), nil
	}
	if req.Runner.Mode != app.Mode {
		return deny("%s runs in %s mode", app.Name, app.Mode), nil
	}
	// The version the machine was told to run is always admitted: a machine
	// waiting its turn in a rollout still runs the one before.
	if req.Runner.Version != deployedVersion && !app.versionAdmitted(req.Runner.Version, s.now()) {
		return deny("%s requires version %s", app.Name, app.RequiredVersion), nil
	}
	if req.Runner.Capacity > capacity {
		return deny("capacity %d exceeds this machine's limit of %d for %s", req.Runner.Capacity, capacity, app.Name), nil
	}

	// One runner per machine and app: this registration replaces the one the
	// machine had, which is what lets it reconnect after a network drop
	// without waiting for the old registration to expire, and what stops a
	// copied credential from doubling a machine.
	replaced, err := s.liveRunners(ctx, machineID, app.Name)
	if err != nil {
		return AdmissionResponse{}, err
	}
	now := s.nowMs()
	err = s.db.Tx(ctx, func(tx *sql.Tx) error {
		for _, runnerID := range replaced {
			if _, err := tx.ExecContext(ctx, `UPDATE runners SET gone_at = ? WHERE runner_id = ?`, now, runnerID); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO runners (runner_id, machine_id, assignment_id, app, version, remote_addr, admitted_at)
			VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (runner_id) DO UPDATE SET gone_at = 0, admitted_at = excluded.admitted_at`,
			req.Runner.RunnerID, machineID, assignmentID, app.Name, req.Runner.Version, req.RemoteAddr, now); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE machines SET last_seen_at = ? WHERE id = ?`, now, machineID)
		return err
	})
	if err != nil {
		return AdmissionResponse{}, err
	}
	for _, runnerID := range replaced {
		s.evictOnOrchestrator(ctx, runnerID, "replaced by a new registration from the same machine")
	}

	var assignments int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM assignments a JOIN machines m ON m.id = a.machine_id
		WHERE m.member_id = ? AND m.status = ? AND a.status = ?`, memberID, MachineApproved, AssignmentActive).Scan(&assignments); err != nil {
		return AdmissionResponse{}, err
	}
	s.log.Info("runner admitted", "runner_id", req.Runner.RunnerID, "machine_id", machineID, "member_id", memberID, "app", app.Name, "remote_addr", req.RemoteAddr)
	return AdmissionResponse{
		Allow:         true,
		MemberID:      memberID,
		PayoutAddress: address,
		// The orchestrator counts a member's runners; the limit that matters,
		// one per machine and app, is enforced above.
		MaxRunners: max(assignments, 1),
		State:      StateProbation,
	}, nil
}
