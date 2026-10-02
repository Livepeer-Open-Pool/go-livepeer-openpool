package core

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Machine statuses.
const (
	MachinePending  = "pending"  // requested by the member, waiting for the operator
	MachineApproved = "approved" // may be assigned apps and given a credential
	MachineRemoved  = "removed"
)

// Assignment statuses.
const (
	AssignmentActive   = "active"
	AssignmentDraining = "draining" // its runner takes no new work and is removed once idle
	AssignmentRemoved  = "removed"
)

// Assignment is the operator's decision that a machine runs an app.
type Assignment struct {
	ID        string    `json:"id"`
	MachineID string    `json:"machine_id"`
	AppID     string    `json:"app_id"`
	AppName   string    `json:"app_name"`
	Capacity  int       `json:"capacity"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// AssignmentInput assigns an app to a machine. A capacity of zero takes the
// app's default.
type AssignmentInput struct {
	AppID    string `json:"app_id"`
	Capacity int    `json:"capacity"`
}

type resolvedAssignment struct {
	app      *App
	capacity int
}

// Machine is one box a member declared: one install, one credential, one or
// more GPUs, and the apps the operator assigned it.
type Machine struct {
	ID          string       `json:"id"`
	MemberID    string       `json:"member_id"`
	Label       string       `json:"label"`
	GPUs        []MachineGPU `json:"gpus"`
	Status      string       `json:"status"`
	CreatedAt   time.Time    `json:"created_at"`
	LastSeenAt  *time.Time   `json:"last_seen_at,omitempty"`
	Assignments []Assignment `json:"assignments"`
	// CredentialCreatedAt is set while the machine has a credential that has
	// not been revoked. The secret itself is never stored or shown again.
	CredentialCreatedAt *time.Time `json:"credential_created_at,omitempty"`
}

// resolveAssignments checks the apps the operator wants on a machine: each
// exists, appears once, suits the machine's GPUs, and has a capacity within
// the app's limit.
func (s *Service) resolveAssignments(ctx context.Context, spec MachineSpec, inputs []AssignmentInput) ([]resolvedAssignment, error) {
	resolved := make([]resolvedAssignment, 0, len(inputs))
	seen := map[string]bool{}
	for _, in := range inputs {
		app, err := s.App(ctx, in.AppID)
		if err != nil {
			return nil, invalidf("app %s does not exist", in.AppID)
		}
		if seen[app.ID] {
			return nil, invalidf("%s is assigned to machine %q twice", app.Name, spec.Label)
		}
		seen[app.ID] = true
		if len(app.EligibleGPUs) > 0 {
			eligible := false
			for _, g := range spec.GPUs {
				for _, id := range app.EligibleGPUs {
					eligible = eligible || g.GPUID == id
				}
			}
			if !eligible {
				return nil, invalidf("machine %q has no GPU that %s may run on", spec.Label, app.Name)
			}
		}
		capacity := in.Capacity
		if capacity == 0 {
			capacity = app.DefaultCapacity
		}
		if capacity < 1 || capacity > app.MaxCapacity {
			return nil, invalidf("%s takes a capacity between 1 and %d", app.Name, app.MaxCapacity)
		}
		resolved = append(resolved, resolvedAssignment{app: app, capacity: capacity})
	}
	return resolved, nil
}

const machineColumns = `id, member_id, label, gpus, status, created_at, last_seen_at`

func scanMachine(row rowScanner) (*Machine, error) {
	var m Machine
	var gpus string
	var created, seen int64
	if err := row.Scan(&m.ID, &m.MemberID, &m.Label, &gpus, &m.Status, &created, &seen); err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(gpus), &m.GPUs); err != nil {
		return nil, err
	}
	m.CreatedAt, m.LastSeenAt = fromMs(created), optionalTime(seen)
	m.Assignments = []Assignment{}
	return &m, nil
}

// fillMachine adds a machine's assignments and credential status.
func (s *Service) fillMachine(ctx context.Context, m *Machine) error {
	rows, err := s.db.QueryContext(ctx, `SELECT a.id, a.machine_id, a.app_id, p.name, a.capacity, a.status, a.created_at, a.updated_at
		FROM assignments a JOIN apps p ON p.id = a.app_id WHERE a.machine_id = ? AND a.status != ? ORDER BY p.name`, m.ID, AssignmentRemoved)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var a Assignment
		var created, updated int64
		if err := rows.Scan(&a.ID, &a.MachineID, &a.AppID, &a.AppName, &a.Capacity, &a.Status, &created, &updated); err != nil {
			return err
		}
		a.CreatedAt, a.UpdatedAt = fromMs(created), fromMs(updated)
		m.Assignments = append(m.Assignments, a)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	var created int64
	err = s.db.QueryRowContext(ctx, `SELECT created_at FROM credentials WHERE machine_id = ? AND revoked_at = 0 ORDER BY created_at DESC LIMIT 1`, m.ID).Scan(&created)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	m.CredentialCreatedAt = optionalTime(created)
	return nil
}

// Machine returns one machine with its assignments.
func (s *Service) Machine(ctx context.Context, id string) (*Machine, error) {
	m, err := scanMachine(s.db.QueryRowContext(ctx, `SELECT `+machineColumns+` FROM machines WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, notFoundf("machine %s does not exist", id)
	}
	if err != nil {
		return nil, err
	}
	return m, s.fillMachine(ctx, m)
}

// Machines lists machines that have not been removed, oldest first; memberID
// and status filter when not empty.
func (s *Service) Machines(ctx context.Context, memberID, status string) ([]Machine, error) {
	query, args := `SELECT `+machineColumns+` FROM machines WHERE status != ?`, []any{MachineRemoved}
	if memberID != "" {
		query, args = query+` AND member_id = ?`, append(args, memberID)
	}
	if status != "" {
		query, args = query+` AND status = ?`, append(args, status)
	}
	rows, err := s.db.QueryContext(ctx, query+` ORDER BY created_at, id`, args...)
	if err != nil {
		return nil, err
	}
	machines := []Machine{}
	for rows.Next() {
		m, err := scanMachine(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		machines = append(machines, *m)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i := range machines {
		if err := s.fillMachine(ctx, &machines[i]); err != nil {
			return nil, err
		}
	}
	return machines, nil
}

// memberMachine returns a machine that belongs to the member and has not
// been removed; anyone else's machine does not exist as far as they can tell.
func (s *Service) memberMachine(ctx context.Context, memberID, machineID string) (*Machine, error) {
	m, err := s.Machine(ctx, machineID)
	if err != nil {
		return nil, err
	}
	if m.MemberID != memberID || m.Status == MachineRemoved {
		return nil, notFoundf("machine %s does not exist", machineID)
	}
	return m, nil
}

// RequestMachine adds a machine to a member, waiting for the operator's approval.
func (s *Service) RequestMachine(ctx context.Context, member *Member, spec MachineSpec) (*Machine, error) {
	if member.Status != MemberActive {
		return nil, forbiddenf("this membership is not active")
	}
	spec, err := s.validateMachineSpec(ctx, spec, true)
	if err != nil {
		return nil, err
	}
	existing, err := s.Machines(ctx, member.ID, "")
	if err != nil {
		return nil, err
	}
	if len(existing) >= 50 {
		return nil, conflictf("a member may have at most 50 machines")
	}
	for _, m := range existing {
		if m.Label == spec.Label {
			return nil, conflictf("you already have a machine labelled %q", spec.Label)
		}
	}
	id := newID("mch")
	if _, err := s.db.ExecContext(ctx, `INSERT INTO machines (id, member_id, label, gpus, status, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
		id, member.ID, spec.Label, mustJSON(spec.GPUs), MachinePending, s.nowMs()); err != nil {
		return nil, err
	}
	if err := s.audit(ctx, s.db, member.Address, "machine.request", id, spec.Label); err != nil {
		return nil, err
	}
	s.notifyAdmin(ctx, fmt.Sprintf("Open Pool %s: %s asks to add a machine", s.cfg.RegionName, member.DisplayName),
		fmt.Sprintf("%s (%s) asks to add the machine %q in %s.\n\nReview it: %s",
			member.DisplayName, member.Address, spec.Label, s.cfg.RegionName, s.adminLink("machines/"+id)))
	return s.Machine(ctx, id)
}

// ApproveMachine approves a machine a member requested.
func (s *Service) ApproveMachine(ctx context.Context, actor, id string) (*Machine, error) {
	m, err := s.Machine(ctx, id)
	if err != nil {
		return nil, err
	}
	if m.Status != MachinePending {
		return nil, conflictf("machine %q is %s, not waiting for approval", m.Label, m.Status)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE machines SET status = ? WHERE id = ?`, MachineApproved, id); err != nil {
		return nil, err
	}
	if err := s.audit(ctx, s.db, actor, "machine.approve", id, m.Label); err != nil {
		return nil, err
	}
	return s.Machine(ctx, id)
}

// RemoveMachine removes a machine: its credential stops working at once and
// its assignments end, so its runners take no new work and are removed once
// the sessions they are running finish. A member removes their own machines;
// the operator may remove any, with memberID empty.
func (s *Service) RemoveMachine(ctx context.Context, actor, memberID, id string) error {
	m, err := s.Machine(ctx, id)
	if err != nil {
		return err
	}
	if m.Status == MachineRemoved || (memberID != "" && m.MemberID != memberID) {
		return notFoundf("machine %s does not exist", id)
	}
	now := s.nowMs()
	err = s.db.Tx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `UPDATE machines SET status = ? WHERE id = ?`, MachineRemoved, id); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE credentials SET revoked_at = ? WHERE machine_id = ? AND revoked_at = 0`, now, id); err != nil {
			return err
		}
		// Its runners stop taking work and leave once their sessions finish.
		if _, err := tx.ExecContext(ctx, `UPDATE assignments SET status = ?, updated_at = ? WHERE machine_id = ? AND status != ?`,
			AssignmentDraining, now, id, AssignmentRemoved); err != nil {
			return err
		}
		return s.audit(ctx, tx, actor, "machine.remove", id, m.Label)
	})
	if err != nil {
		return err
	}
	return s.finishDrainedAssignments(ctx)
}

// SetAssignments makes inputs the apps a machine runs. An app no longer
// listed is drained: its runner takes no new work and is removed once the
// sessions it is running finish. A new app may register as soon as the
// machine presents its credential.
func (s *Service) SetAssignments(ctx context.Context, actor, machineID string, inputs []AssignmentInput) (*Machine, error) {
	m, err := s.Machine(ctx, machineID)
	if err != nil {
		return nil, err
	}
	if m.Status != MachineApproved {
		return nil, conflictf("machine %q is %s; only an approved machine can be assigned apps", m.Label, m.Status)
	}
	resolved, err := s.resolveAssignments(ctx, MachineSpec{Label: m.Label, GPUs: m.GPUs}, inputs)
	if err != nil {
		return nil, err
	}
	wanted := map[string]resolvedAssignment{}
	for _, r := range resolved {
		wanted[r.app.ID] = r
	}
	now := s.nowMs()
	err = s.db.Tx(ctx, func(tx *sql.Tx) error {
		for _, current := range m.Assignments {
			want, keep := wanted[current.AppID]
			if !keep {
				if _, err := tx.ExecContext(ctx, `UPDATE assignments SET status = ?, updated_at = ? WHERE id = ?`, AssignmentDraining, now, current.ID); err != nil {
					return err
				}
				continue
			}
			if _, err := tx.ExecContext(ctx, `UPDATE assignments SET capacity = ?, status = ?, updated_at = ? WHERE id = ?`,
				want.capacity, AssignmentActive, now, current.ID); err != nil {
				return err
			}
			// An app assigned again while its runner was draining: the runner
			// stays, and comes back once it passes its tests.
			if _, err := tx.ExecContext(ctx, `UPDATE runners SET suspend_reason = ?, passes = 0 WHERE assignment_id = ? AND gone_at = 0 AND suspend_reason = ?`,
				SuspendedByTests, current.ID, SuspendedByDrain); err != nil {
				return err
			}
			delete(wanted, current.AppID)
		}
		for _, want := range wanted {
			if _, err := tx.ExecContext(ctx, `INSERT INTO assignments (id, machine_id, app_id, capacity, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
				newID("asg"), machineID, want.app.ID, want.capacity, AssignmentActive, now, now); err != nil {
				return err
			}
		}
		return s.audit(ctx, tx, actor, "machine.assign", machineID, mustJSON(inputs))
	})
	if err != nil {
		return nil, err
	}
	if err := s.finishDrainedAssignments(ctx); err != nil {
		return nil, err
	}
	return s.Machine(ctx, machineID)
}

// NewCredential is a machine's credential at the one moment its secret is
// known. Commands are how to start each assigned app's runner by hand with
// livepeer_runner_tunnel, for a machine that does not use the agent.
type NewCredential struct {
	MachineID string   `json:"machine_id"`
	Secret    string   `json:"secret"`
	Commands  []string `json:"commands"`
}

// CreateCredential gives a member's machine a new credential and revokes
// the one it had. The secret is returned once and only its hash is kept.
func (s *Service) CreateCredential(ctx context.Context, member *Member, machineID string) (*NewCredential, error) {
	if member.Status != MemberActive {
		return nil, forbiddenf("this membership is not active")
	}
	m, err := s.memberMachine(ctx, member.ID, machineID)
	if err != nil {
		return nil, err
	}
	if m.Status != MachineApproved {
		return nil, conflictf("machine %q has not been approved yet", m.Label)
	}
	secret, now := newSecret(), s.nowMs()
	err = s.db.Tx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `UPDATE credentials SET revoked_at = ? WHERE machine_id = ? AND revoked_at = 0`, now, machineID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO credentials (id, machine_id, secret_hash, created_at) VALUES (?, ?, ?, ?)`,
			newID("crd"), machineID, hashSecret(secret), now); err != nil {
			return err
		}
		return s.audit(ctx, tx, member.Address, "credential.create", machineID, m.Label)
	})
	if err != nil {
		return nil, err
	}
	credential := &NewCredential{MachineID: machineID, Secret: secret, Commands: []string{}}
	for _, a := range m.Assignments {
		app, err := s.App(ctx, a.AppID)
		if err != nil {
			return nil, err
		}
		credential.Commands = append(credential.Commands, s.runnerCommand(app, a.Capacity, secret))
	}
	return credential, nil
}

// runnerCommand is the livepeer_runner_tunnel invocation that serves app
// from a local port.
func (s *Service) runnerCommand(app *App, capacity int, secret string) string {
	parts := []string{
		"livepeer_runner_tunnel",
		"-orchestrator " + s.cfg.OrchestratorURL,
		"-credential " + secret,
		fmt.Sprintf("-target http://127.0.0.1:%d", app.Container.Port),
		"-app " + app.Name,
		"-mode " + app.Mode,
		fmt.Sprintf("-capacity %d", capacity),
		"-version " + app.RequiredVersion,
	}
	if app.Container.HealthPath != "" {
		parts = append(parts, "-health "+app.Container.HealthPath)
	} else {
		parts = append(parts, `-health ""`)
	}
	return strings.Join(parts, " ")
}

// RevokeCredential revokes a member's machine's credential without issuing another.
func (s *Service) RevokeCredential(ctx context.Context, member *Member, machineID string) error {
	m, err := s.memberMachine(ctx, member.ID, machineID)
	if err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE credentials SET revoked_at = ? WHERE machine_id = ? AND revoked_at = 0`, s.nowMs(), machineID); err != nil {
		return err
	}
	return s.audit(ctx, s.db, member.Address, "credential.revoke", machineID, m.Label)
}
