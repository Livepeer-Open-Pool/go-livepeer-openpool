package core

import (
	"context"
	"database/sql"
	"strings"
	"time"
)

// OrchRunner is a registered runner as the orchestrator reports it on
// GET /liveRunner/runners.
type OrchRunner struct {
	RunnerID           string   `json:"runner_id"`
	MemberID           string   `json:"member_id"`
	PayoutAddress      string   `json:"payout_address"`
	State              string   `json:"state"`
	Status             string   `json:"status"`
	Ready              bool     `json:"ready"`
	App                string   `json:"app"`
	Mode               string   `json:"mode"`
	Version            string   `json:"version"`
	Capacity           int      `json:"capacity"`
	CapacityRegistered int      `json:"capacity_registered"`
	CapacityUsed       int      `json:"capacity_used"`
	Tunnel             bool     `json:"tunnel"`
	TunnelConnected    bool     `json:"tunnel_connected"`
	RemoteAddr         string   `json:"remote_addr"`
	RegisteredAtMs     int64    `json:"registered_at_ms"`
	LastHeartbeatMs    int64    `json:"last_heartbeat_ms"`
	SessionIDs         []string `json:"session_ids"`
	Static             bool     `json:"static"`
}

// Runner is a runner on the orchestrator together with what the pool knows
// about it: the machine it runs on and whose that is.
type Runner struct {
	OrchRunner
	MachineID    string `json:"machine_id,omitempty"`
	MachineLabel string `json:"machine_label,omitempty"`
	MemberName   string `json:"member_name,omitempty"`
	// Unknown marks a runner the pool did not admit, such as a static runner
	// left in the orchestrator's configuration.
	Unknown bool `json:"unknown,omitempty"`
	// Hold is set while the operator has taken the runner out of the pool's
	// own promoting, suspending and draining.
	Hold       bool   `json:"hold"`
	HoldReason string `json:"hold_reason,omitempty"`
	// SuspendReason says why the pool suspended the runner: "tests",
	// "version" or "draining".
	SuspendReason string     `json:"suspend_reason,omitempty"`
	LastTestAt    *time.Time `json:"last_test_at,omitempty"`
	LastTestOK    bool       `json:"last_test_ok"`
	// Failures is how many tests in a row the runner has failed.
	Failures int `json:"failures"`
}

// runnerGrace is how long a runner this service admitted may be missing from
// the orchestrator's list before it is taken for gone. The orchestrator
// creates the runner only after the admission answer reaches it.
const runnerGrace = 2 * time.Minute

// Runners lists the runners registered on the orchestrator; memberID filters
// when not empty. It also notes which runners this service admitted are gone.
func (s *Service) Runners(ctx context.Context, memberID string) ([]Runner, error) {
	if s.orch == nil {
		return nil, unavailablef("this service is not connected to an orchestrator")
	}
	listed, err := s.orch.Runners(ctx)
	if err != nil {
		return nil, unavailablef("the orchestrator did not answer: %v", err)
	}
	type known struct {
		machineID, label, memberID, memberName string
		hold                                   bool
		holdReason, suspendReason              string
		lastTestAt                             int64
		lastTestOK                             bool
		failures                               int
	}
	admitted := map[string]known{}
	rows, err := s.db.QueryContext(ctx, `SELECT r.runner_id, m.id, m.label, b.id, b.display_name,
		r.hold, r.hold_reason, r.suspend_reason, r.last_test_at, r.last_test_ok, r.failures
		FROM runners r JOIN machines m ON m.id = r.machine_id JOIN members b ON b.id = m.member_id WHERE r.gone_at = 0`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var runnerID string
		var k known
		if err := rows.Scan(&runnerID, &k.machineID, &k.label, &k.memberID, &k.memberName,
			&k.hold, &k.holdReason, &k.suspendReason, &k.lastTestAt, &k.lastTestOK, &k.failures); err != nil {
			rows.Close()
			return nil, err
		}
		admitted[runnerID] = k
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	present := map[string]bool{}
	runners := []Runner{}
	for _, r := range listed {
		present[r.RunnerID] = true
		view := Runner{OrchRunner: r}
		if k, ok := admitted[r.RunnerID]; ok {
			view.MachineID, view.MachineLabel, view.MemberName = k.machineID, k.label, k.memberName
			view.Hold, view.HoldReason, view.SuspendReason = k.hold, k.holdReason, k.suspendReason
			view.LastTestAt, view.LastTestOK, view.Failures = optionalTime(k.lastTestAt), k.lastTestOK, k.failures
		} else {
			view.Unknown = true
		}
		if memberID == "" || r.MemberID == memberID {
			runners = append(runners, view)
		}
	}
	// Runners that left the orchestrator by themselves: expired or unregistered.
	cutoff := s.now().Add(-runnerGrace).UnixMilli()
	for runnerID := range admitted {
		if !present[runnerID] {
			if _, err := s.db.ExecContext(ctx, `UPDATE runners SET gone_at = ? WHERE runner_id = ? AND gone_at = 0 AND admitted_at < ?`,
				s.nowMs(), runnerID, cutoff); err != nil {
				return nil, err
			}
		}
	}
	return runners, nil
}

// SetRunnerState moves a runner between probation, active and suspended and
// puts it on hold, so that the pool's tests do not move it back.
func (s *Service) SetRunnerState(ctx context.Context, actor, runnerID, state, reason string) error {
	if state != StateProbation && state != StateActive && state != StateSuspended {
		return invalidf("state must be probation, active, or suspended")
	}
	if s.orch == nil {
		return unavailablef("this service is not connected to an orchestrator")
	}
	if err := s.orch.SetState(ctx, runnerID, state, reason); err != nil {
		return orchError(err)
	}
	// The operator overrode the pool's own decision, so the pool stops
	// deciding for this runner until the hold is lifted.
	if _, err := s.db.ExecContext(ctx, `UPDATE runners SET hold = 1, hold_reason = ?, suspend_reason = '' WHERE runner_id = ? AND gone_at = 0`,
		"state set to "+state+" by the operator", runnerID); err != nil {
		return err
	}
	return s.audit(ctx, s.db, actor, "runner.state", runnerID, strings.TrimSpace(state+" "+reason))
}

// EvictRunner removes a runner from the orchestrator. Its machine may
// register again unless its assignment, credential or membership has ended.
func (s *Service) EvictRunner(ctx context.Context, actor, runnerID, reason string) error {
	if s.orch == nil {
		return unavailablef("this service is not connected to an orchestrator")
	}
	if err := s.orch.EvictRunner(ctx, runnerID, reason); err != nil {
		return orchError(err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE runners SET gone_at = ? WHERE runner_id = ? AND gone_at = 0`, s.nowMs(), runnerID); err != nil {
		return err
	}
	return s.audit(ctx, s.db, actor, "runner.evict", runnerID, reason)
}

// orchError passes on an error the orchestrator client already classified
// and treats anything else as the orchestrator being unreachable.
func orchError(err error) error {
	if _, ok := err.(*Error); ok {
		return err
	}
	return unavailablef("the orchestrator did not answer: %v", err)
}

// liveRunners are the runners this service admitted for a machine that it
// has not seen leave; app narrows to one app when not empty.
func (s *Service) liveRunners(ctx context.Context, machineID, app string) ([]string, error) {
	query, args := `SELECT runner_id FROM runners WHERE machine_id = ? AND gone_at = 0`, []any{machineID}
	if app != "" {
		query, args = query+` AND app = ?`, append(args, app)
	}
	return s.queryRunnerIDs(ctx, s.db, query, args...)
}

type queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func (s *Service) queryRunnerIDs(ctx context.Context, db queryer, query string, args ...any) ([]string, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// evictOnOrchestrator removes a runner, logging a failure: the caller has
// already ended whatever let the runner register, so one that survives is
// refused the next time it registers.
func (s *Service) evictOnOrchestrator(ctx context.Context, runnerID, reason string) {
	if s.orch == nil {
		return
	}
	if err := s.orch.EvictRunner(ctx, runnerID, reason); err != nil {
		var e *Error
		if asError(err, &e) && e.Kind == NotFound {
			return // already gone
		}
		s.log.Warn("runner not evicted from the orchestrator", "runner_id", runnerID, "reason", reason, "err", err)
	}
}

func asError(err error, target **Error) bool {
	e, ok := err.(*Error)
	if ok {
		*target = e
	}
	return ok
}

// evictMemberRunners removes every runner of a member from the orchestrator.
func (s *Service) evictMemberRunners(ctx context.Context, memberID, reason string) {
	if _, err := s.db.ExecContext(ctx, `UPDATE runners SET gone_at = ? WHERE gone_at = 0 AND machine_id IN (SELECT id FROM machines WHERE member_id = ?)`,
		s.nowMs(), memberID); err != nil {
		s.log.Error("marking a member's runners gone", "member_id", memberID, "err", err)
	}
	if s.orch == nil {
		return
	}
	if err := s.orch.EvictMember(ctx, memberID, reason); err != nil {
		var e *Error
		if asError(err, &e) && e.Kind == NotFound {
			return // the member had no runners registered
		}
		s.log.Warn("member's runners not evicted from the orchestrator", "member_id", memberID, "err", err)
	}
}

// ConnectedRunners counts, per app, the runners the pool admitted that have
// not left. It reads the pool's own record and does not ask the orchestrator,
// so it is cheap enough to answer anyone.
func (s *Service) ConnectedRunners(ctx context.Context) (map[string]int, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT app, COUNT(*) FROM runners WHERE gone_at = 0 GROUP BY app`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	counts := map[string]int{}
	for rows.Next() {
		var app string
		var n int
		if err := rows.Scan(&app, &n); err != nil {
			return nil, err
		}
		counts[app] = n
	}
	return counts, rows.Err()
}
