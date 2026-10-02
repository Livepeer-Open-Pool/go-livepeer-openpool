package core

import (
	"context"
	"sort"
	"time"
)

// A new version of an app reaches machines a few at a time, so the app is
// never without runners: at most a quarter of its running machines, and
// always at least one, are being moved at once. A machine being moved stops
// taking new work, finishes its sessions, is told to run the new version,
// and counts as moved once its new runner is active. Idle machines go first.
//
// Until its turn comes a machine keeps running, and being admitted on, the
// version it was last told to run, whatever the app's deadline says: the
// deadline is for machines that were told and did not follow.

// SuspendedByUpgrade marks a runner that stopped taking work because its
// machine is being moved to a new version.
const SuspendedByUpgrade = "upgrade"

// upgradeTimeout is how long the pool waits for a machine's new runner
// before moving on to the next machine.
const upgradeTimeout = 15 * time.Minute

// liveRunner is a registered runner with what the pool knows about it.
type liveRunner struct {
	OrchRunner
	t *tracked
}

type rollingAssignment struct {
	id, appID                          string
	deployedVersion, deployedContainer string
	upgradingSince                     int64
}

// rollOut moves assignments toward their app's current version. live is the
// registered runner of each assignment, for those that have one.
func (s *Service) rollOut(ctx context.Context, live map[string]liveRunner) error {
	rows, err := s.db.QueryContext(ctx, `SELECT a.id, a.app_id, a.deployed_version, a.deployed_container, a.upgrading_since
		FROM assignments a JOIN machines m ON m.id = a.machine_id WHERE a.status = ? AND m.status = ? ORDER BY a.created_at, a.id`,
		AssignmentActive, MachineApproved)
	if err != nil {
		return err
	}
	var appIDs []string
	byApp := map[string][]rollingAssignment{}
	for rows.Next() {
		var a rollingAssignment
		if err := rows.Scan(&a.id, &a.appID, &a.deployedVersion, &a.deployedContainer, &a.upgradingSince); err != nil {
			rows.Close()
			return err
		}
		if _, ok := byApp[a.appID]; !ok {
			appIDs = append(appIDs, a.appID)
		}
		byApp[a.appID] = append(byApp[a.appID], a)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	now := s.nowMs()
	for _, appID := range appIDs {
		assignments := byApp[appID]
		app, err := s.App(ctx, appID)
		if err != nil {
			return err
		}
		version, container := app.RequiredVersion, mustJSON(app.Container)
		current := func(a rollingAssignment) bool {
			return a.deployedVersion == version && a.deployedContainer == container
		}
		// deploy tells the machine to run the current version from now on.
		deploy := func(a rollingAssignment, upgradingSince int64) error {
			_, err := s.db.ExecContext(ctx, `UPDATE assignments SET deployed_version = ?, deployed_container = ?, upgrading_since = ?, updated_at = ? WHERE id = ?`,
				version, container, upgradingSince, now, a.id)
			return err
		}
		suspendForUpgrade := func(r liveRunner) {
			if err := s.suspend(ctx, r.RunnerID, SuspendedByUpgrade, "moving to version "+version+"; finishing running sessions"); err != nil {
				s.log.Warn("suspending a runner to move it to a new version", "runner_id", r.RunnerID, "err", err)
			}
		}
		running := 0
		for _, a := range assignments {
			if _, ok := live[a.id]; ok {
				running++
			}
		}
		limit := max(1, (running+3)/4)

		// Machines already being moved.
		moving := 0
		for _, a := range assignments {
			if a.upgradingSince == 0 {
				continue
			}
			runner, hasRunner := live[a.id]
			switch {
			case !current(a):
				// It stopped taking work. Once its sessions are done it is told to switch.
				if !hasRunner || runner.CapacityUsed == 0 {
					if err := deploy(a, now); err != nil {
						return err
					}
				} else if !runner.t.hold && (runner.State != StateSuspended || runner.t.suspendReason != SuspendedByUpgrade) {
					suspendForUpgrade(runner) // it reconnected in the meantime
				}
				moving++
			case hasRunner && runner.Version == version && runner.State == StateActive,
				time.Duration(now-a.upgradingSince)*time.Millisecond >= upgradeTimeout:
				// Its new runner is serving, or it did not come back in time. Either way, next.
				if _, err := s.db.ExecContext(ctx, `UPDATE assignments SET upgrading_since = 0, updated_at = ? WHERE id = ?`, now, a.id); err != nil {
					return err
				}
				a.upgradingSince = 0
				if hasRunner && runner.Version != version {
					if err := s.audit(ctx, s.db, "pool", "assignment.upgrade_timeout", a.id, app.Name+" "+version); err != nil {
						return err
					}
				}
			default:
				moving++
			}
			if a.upgradingSince == 0 && hasRunner && !runner.t.hold && runner.t.suspendReason == SuspendedByUpgrade {
				// A runner the rollout stopped and then gave up on goes back to
				// the tests: passing them brings it back, until the deadline.
				if _, err := s.db.ExecContext(ctx, `UPDATE runners SET suspend_reason = ?, passes = 0 WHERE runner_id = ?`, SuspendedByTests, runner.RunnerID); err != nil {
					return err
				}
			}
		}

		// Machines still on what they were told before.
		var waiting []rollingAssignment
		for _, a := range assignments {
			if a.upgradingSince != 0 || current(a) {
				continue
			}
			runner, hasRunner := live[a.id]
			if !hasRunner || a.deployedVersion == "" || runner.Version == version {
				// Nothing there is serving the old version: tell it the new one at once.
				if err := deploy(a, 0); err != nil {
					return err
				}
				continue
			}
			if !runner.t.hold {
				waiting = append(waiting, a)
			}
		}
		// Idle machines go first: they have no sessions to wait for.
		sort.SliceStable(waiting, func(i, j int) bool {
			return live[waiting[i].id].CapacityUsed == 0 && live[waiting[j].id].CapacityUsed != 0
		})
		for _, a := range waiting {
			if moving >= limit {
				break
			}
			runner := live[a.id]
			if _, err := s.db.ExecContext(ctx, `UPDATE assignments SET upgrading_since = ?, updated_at = ? WHERE id = ?`, now, now, a.id); err != nil {
				return err
			}
			suspendForUpgrade(runner)
			moving++
		}
	}
	return nil
}
