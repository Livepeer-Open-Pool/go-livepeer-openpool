package core

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"
)

// Prober runs an app's test against one runner, through the orchestrator's
// unpaid probe session, which the runner cannot tell from paid work.
type Prober interface {
	Probe(ctx context.Context, runner OrchRunner, test Test) TestOutcome
}

// TestOutcome is what a test found.
type TestOutcome struct {
	OK        bool
	LatencyMs int64
	// Detail says what was wrong, for the member and the operator to read.
	Detail string
}

// TestResult is one recorded test of a runner.
type TestResult struct {
	ID        int64     `json:"id"`
	RunnerID  string    `json:"runner_id"`
	MachineID string    `json:"machine_id"`
	App       string    `json:"app"`
	At        time.Time `json:"at"`
	OK        bool      `json:"ok"`
	LatencyMs int64     `json:"latency_ms"`
	Detail    string    `json:"detail,omitempty"`
}

// Why this service suspended a runner.
const (
	SuspendedByTests   = "tests"    // failed tests in a row; passing them brings it back
	SuspendedByVersion = "version"  // still on a version whose deadline passed
	SuspendedByDrain   = "draining" // its assignment ended; removed once its sessions finish
)

const (
	// probeConcurrency bounds how many tests run at once.
	probeConcurrency = 8
	// testResultRetention is how long test results are kept.
	testResultRetention = 14 * 24 * time.Hour
)

// tracked is what the pool knows about a runner it admitted.
type tracked struct {
	runnerID, machineID, assignmentID, app string
	assignmentStatus                       string
	hold                                   bool
	suspendReason                          string
	failures, passes                       int
	lastTestAt                             int64
}

func (s *Service) trackedRunners(ctx context.Context) (map[string]*tracked, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT r.runner_id, r.machine_id, r.assignment_id, r.app, a.status,
		r.hold, r.suspend_reason, r.failures, r.passes, r.last_test_at
		FROM runners r JOIN assignments a ON a.id = r.assignment_id WHERE r.gone_at = 0`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]*tracked{}
	for rows.Next() {
		t := &tracked{}
		if err := rows.Scan(&t.runnerID, &t.machineID, &t.assignmentID, &t.app, &t.assignmentStatus,
			&t.hold, &t.suspendReason, &t.failures, &t.passes, &t.lastTestAt); err != nil {
			return nil, err
		}
		out[t.runnerID] = t
	}
	return out, rows.Err()
}

// Reconcile is one pass of what the pool does with registered runners by
// itself: it drains runners whose assignment ended, suspends runners left on
// a version whose deadline passed, and tests the rest, promoting those that
// pass and suspending those that keep failing. A runner the operator put on
// hold is tested and otherwise left alone. The service runs it on a timer;
// it is safe to call at any time.
func (s *Service) Reconcile(ctx context.Context) error {
	if s.orch == nil {
		return nil
	}
	s.reconcileMu.Lock()
	defer s.reconcileMu.Unlock()

	listed, err := s.orch.Runners(ctx)
	if err != nil {
		return unavailablef("the orchestrator did not answer: %v", err)
	}
	known, err := s.trackedRunners(ctx)
	if err != nil {
		return err
	}
	settings, err := s.Settings(ctx)
	if err != nil {
		return err
	}
	apps := map[string]*App{}
	now := s.now()

	present := map[string]bool{}
	type due struct {
		runner OrchRunner
		t      *tracked
		app    *App
	}
	var toTest []due
	for _, r := range listed {
		present[r.RunnerID] = true
		t, ok := known[r.RunnerID]
		if !ok {
			continue // not one the pool admitted
		}
		app := apps[t.app]
		if app == nil {
			if app, err = s.appByName(ctx, t.app); err != nil {
				return err
			}
			apps[t.app] = app
		}

		if !t.hold {
			if t.assignmentStatus != AssignmentActive {
				if err := s.drain(ctx, r, t); err != nil {
					s.log.Warn("draining a runner", "runner_id", r.RunnerID, "err", err)
				}
				continue
			}
			if !app.versionAdmitted(r.Version, now) {
				if r.State != StateSuspended || t.suspendReason != SuspendedByVersion {
					reason := fmt.Sprintf("runs version %s; %s requires %s", r.Version, app.Name, app.RequiredVersion)
					if err := s.suspend(ctx, r.RunnerID, SuspendedByVersion, reason); err != nil {
						s.log.Warn("suspending an outdated runner", "runner_id", r.RunnerID, "err", err)
					}
				}
				continue
			}
		}
		interval := time.Duration(settings.TestIntervalSeconds) * time.Second
		if app.Test.Defined() && r.Ready && (t.lastTestAt == 0 || now.Sub(fromMs(t.lastTestAt)) >= interval) {
			toTest = append(toTest, due{r, t, app})
		}
	}

	// Runners that left the orchestrator by themselves, and assignments that
	// were draining with nothing left to drain.
	cutoff := now.Add(-runnerGrace).UnixMilli()
	for runnerID := range known {
		if !present[runnerID] {
			if _, err := s.db.ExecContext(ctx, `UPDATE runners SET gone_at = ? WHERE runner_id = ? AND gone_at = 0 AND admitted_at < ?`,
				now.UnixMilli(), runnerID, cutoff); err != nil {
				return err
			}
		}
	}
	if err := s.finishDrainedAssignments(ctx); err != nil {
		return err
	}

	if s.prober != nil && len(toTest) > 0 {
		outcomes := make([]TestOutcome, len(toTest))
		var wg sync.WaitGroup
		slots := make(chan struct{}, probeConcurrency)
		for i, d := range toTest {
			wg.Add(1)
			slots <- struct{}{}
			go func() {
				defer wg.Done()
				defer func() { <-slots }()
				outcomes[i] = s.prober.Probe(ctx, d.runner, d.app.Test)
			}()
		}
		wg.Wait()
		for i, d := range toTest {
			if err := s.applyTest(ctx, d.runner, d.t, outcomes[i], settings); err != nil {
				s.log.Warn("recording a test", "runner_id", d.runner.RunnerID, "err", err)
			}
		}
	}

	_, err = s.db.ExecContext(ctx, `DELETE FROM test_results WHERE at < ?`, now.Add(-testResultRetention).UnixMilli())
	return err
}

// suspend suspends a runner on the orchestrator and remembers why.
func (s *Service) suspend(ctx context.Context, runnerID, why, reason string) error {
	if err := s.orch.SetState(ctx, runnerID, StateSuspended, reason); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE runners SET suspend_reason = ?, passes = 0 WHERE runner_id = ?`, why, runnerID); err != nil {
		return err
	}
	return s.audit(ctx, s.db, "pool", "runner.suspend", runnerID, reason)
}

// drain stops a runner whose assignment ended from taking new work, and
// removes it once the sessions it was running have finished.
func (s *Service) drain(ctx context.Context, r OrchRunner, t *tracked) error {
	if r.State != StateSuspended || t.suspendReason != SuspendedByDrain {
		if err := s.suspend(ctx, r.RunnerID, SuspendedByDrain, "its assignment ended; finishing running sessions"); err != nil {
			return err
		}
	}
	if r.CapacityUsed > 0 {
		return nil
	}
	s.evictOnOrchestrator(ctx, r.RunnerID, "its assignment ended")
	if _, err := s.db.ExecContext(ctx, `UPDATE runners SET gone_at = ? WHERE runner_id = ?`, s.nowMs(), r.RunnerID); err != nil {
		return err
	}
	return s.audit(ctx, s.db, "pool", "runner.drained", r.RunnerID, t.app)
}

// finishDrainedAssignments ends draining assignments that have no runner left.
func (s *Service) finishDrainedAssignments(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `UPDATE assignments SET status = ?, updated_at = ? WHERE status = ?
		AND NOT EXISTS (SELECT 1 FROM runners r WHERE r.assignment_id = assignments.id AND r.gone_at = 0)`,
		AssignmentRemoved, s.nowMs(), AssignmentDraining)
	return err
}

// applyTest records a test and acts on it: a pass takes a runner off
// probation, enough failures in a row suspend an active one, and enough
// passes in a row bring back one that tests suspended.
func (s *Service) applyTest(ctx context.Context, r OrchRunner, t *tracked, outcome TestOutcome, settings Settings) error {
	now := s.nowMs()
	failures, passes := 0, t.passes+1
	if !outcome.OK {
		failures, passes = t.failures+1, 0
	}
	err := s.db.Tx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO test_results (runner_id, machine_id, app, at, ok, latency_ms, detail) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			r.RunnerID, t.machineID, t.app, now, outcome.OK, outcome.LatencyMs, outcome.Detail); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE runners SET failures = ?, passes = ?, last_test_at = ?, last_test_ok = ? WHERE runner_id = ?`,
			failures, passes, now, outcome.OK, r.RunnerID)
		return err
	})
	if err != nil || t.hold {
		return err
	}

	switch {
	case outcome.OK && r.State == StateProbation:
		return s.activate(ctx, r.RunnerID, "passed its test")
	case outcome.OK && r.State == StateSuspended && t.suspendReason == SuspendedByTests && passes >= settings.TestPassesToReinstate:
		return s.activate(ctx, r.RunnerID, fmt.Sprintf("passed %d tests in a row", passes))
	case !outcome.OK && r.State == StateActive && failures >= settings.TestFailuresToSuspend:
		return s.suspend(ctx, r.RunnerID, SuspendedByTests, fmt.Sprintf("failed %d tests in a row: %s", failures, outcome.Detail))
	}
	return nil
}

func (s *Service) activate(ctx context.Context, runnerID, reason string) error {
	if err := s.orch.SetState(ctx, runnerID, StateActive, reason); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE runners SET suspend_reason = '', failures = 0 WHERE runner_id = ?`, runnerID); err != nil {
		return err
	}
	return s.audit(ctx, s.db, "pool", "runner.activate", runnerID, reason)
}

// TestRunnerNow tests a runner at once, whatever its schedule, and acts on
// the result as a scheduled test would.
func (s *Service) TestRunnerNow(ctx context.Context, actor, runnerID string) (*TestResult, error) {
	if s.orch == nil || s.prober == nil {
		return nil, unavailablef("this service is not connected to an orchestrator")
	}
	s.reconcileMu.Lock()
	defer s.reconcileMu.Unlock()
	listed, err := s.orch.Runners(ctx)
	if err != nil {
		return nil, unavailablef("the orchestrator did not answer: %v", err)
	}
	known, err := s.trackedRunners(ctx)
	if err != nil {
		return nil, err
	}
	t, ok := known[runnerID]
	var runner *OrchRunner
	for i := range listed {
		if listed[i].RunnerID == runnerID {
			runner = &listed[i]
		}
	}
	if !ok || runner == nil {
		return nil, notFoundf("runner %s is not registered", runnerID)
	}
	app, err := s.appByName(ctx, t.app)
	if err != nil {
		return nil, err
	}
	if !app.Test.Defined() {
		return nil, conflictf("%s has no test defined", app.Name)
	}
	settings, err := s.Settings(ctx)
	if err != nil {
		return nil, err
	}
	outcome := s.prober.Probe(ctx, *runner, app.Test)
	if err := s.applyTest(ctx, *runner, t, outcome, settings); err != nil {
		return nil, orchError(err)
	}
	if err := s.audit(ctx, s.db, actor, "runner.test", runnerID, outcome.Detail); err != nil {
		return nil, err
	}
	results, err := s.TestResults(ctx, runnerID, "", 1)
	if err != nil || len(results) == 0 {
		return nil, err
	}
	return &results[0], nil
}

// TestResults returns recorded tests, newest first; runnerID and memberID
// filter when not empty.
func (s *Service) TestResults(ctx context.Context, runnerID, memberID string, limit int) ([]TestResult, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	query, args := `SELECT t.id, t.runner_id, t.machine_id, t.app, t.at, t.ok, t.latency_ms, t.detail FROM test_results t`, []any{}
	if memberID != "" {
		query += ` JOIN machines m ON m.id = t.machine_id WHERE m.member_id = ?`
		args = append(args, memberID)
	} else {
		query += ` WHERE 1 = 1`
	}
	if runnerID != "" {
		query, args = query+` AND t.runner_id = ?`, append(args, runnerID)
	}
	rows, err := s.db.QueryContext(ctx, query+` ORDER BY t.id DESC LIMIT ?`, append(args, limit)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	results := []TestResult{}
	for rows.Next() {
		var r TestResult
		var at int64
		if err := rows.Scan(&r.ID, &r.RunnerID, &r.MachineID, &r.App, &at, &r.OK, &r.LatencyMs, &r.Detail); err != nil {
			return nil, err
		}
		r.At = fromMs(at)
		results = append(results, r)
	}
	return results, rows.Err()
}

// SetRunnerHold puts a runner on hold, so the pool stops promoting,
// suspending and draining it by itself, or lifts the hold. Lifting it hands
// the runner back: if it is suspended, passing tests bring it back.
func (s *Service) SetRunnerHold(ctx context.Context, actor, runnerID string, hold bool, reason string) error {
	query := `UPDATE runners SET hold = 1, hold_reason = ? WHERE runner_id = ? AND gone_at = 0`
	if !hold {
		reason = ""
		query = `UPDATE runners SET hold = 0, hold_reason = ?, suspend_reason = '` + SuspendedByTests + `', failures = 0, passes = 0 WHERE runner_id = ? AND gone_at = 0`
	}
	res, err := s.db.ExecContext(ctx, query, reason, runnerID)
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err != nil || n == 0 {
		return notFoundf("runner %s is not one this pool admitted", runnerID)
	}
	action := "runner.hold"
	if !hold {
		action = "runner.release"
	}
	return s.audit(ctx, s.db, actor, action, runnerID, reason)
}
