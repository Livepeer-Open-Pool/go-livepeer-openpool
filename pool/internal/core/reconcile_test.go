package core_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Livepeer-Open-Pool/pool/internal/core"
	"github.com/Livepeer-Open-Pool/pool/internal/testkit"
)

// pooled sets up one member with one machine assigned the apps, and returns
// the machine's credential.
func (f *fixture) pooled(apps ...*core.App) (*core.Member, *core.Machine, string) {
	f.t.Helper()
	gpu := f.gpu("NVIDIA RTX 4090", 24564)
	f.publishTerms()
	member, machine := f.member(testkit.NewWallet(f.t), "Alice", gpu, apps...)
	return member, machine, f.credential(member, machine)
}

func TestPassingItsTestTakesARunnerOffProbation(t *testing.T) {
	f := newFixture(t)
	app := f.testedApp("vllm/qwen2.5-7b-instruct")
	member, machine, credential := f.pooled(app)
	f.runner(member, credential, "runner_a", app)

	// A failing runner stays where it is, and the member can read why.
	f.prober.Fail("runner_a", `answered 500, expected 200: CUDA out of memory`)
	require.NoError(t, f.svc.Reconcile(f.ctx))
	require.Equal(t, core.StateProbation, f.state("runner_a"))
	results, err := f.svc.TestResults(f.ctx, "", member.ID, 0)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, core.TestResult{
		ID: results[0].ID, RunnerID: "runner_a", MachineID: machine.ID, App: "vllm/qwen2.5-7b-instruct", At: f.clock.Now(),
		OK: false, LatencyMs: 80, Detail: "answered 500, expected 200: CUDA out of memory",
	}, results[0])

	// It is not tested again until the interval has passed.
	require.NoError(t, f.svc.Reconcile(f.ctx))
	require.Equal(t, 1, f.prober.Count("runner_a"))

	f.prober.Pass("runner_a")
	f.pass()
	require.Equal(t, 2, f.prober.Count("runner_a"))
	require.Equal(t, core.StateActive, f.state("runner_a"))

	runners, err := f.svc.Runners(f.ctx, "")
	require.NoError(t, err)
	require.True(t, runners[0].LastTestOK)
	require.Equal(t, f.clock.Now(), *runners[0].LastTestAt)
	require.Zero(t, runners[0].Failures)
}

func TestRepeatedFailuresSuspendAndPassesReinstate(t *testing.T) {
	f := newFixture(t)
	app := f.testedApp("echo")
	member, _, credential := f.pooled(app)
	f.runner(member, credential, "runner_a", app)
	f.pass()
	require.Equal(t, core.StateActive, f.state("runner_a"))

	// Two failures and a pass: the count starts over.
	f.prober.Fail("runner_a", "no answer within 5s")
	f.pass()
	f.pass()
	require.Equal(t, core.StateActive, f.state("runner_a"))
	f.prober.Pass("runner_a")
	f.pass()
	f.prober.Fail("runner_a", "no answer within 5s")
	f.pass()
	f.pass()
	require.Equal(t, core.StateActive, f.state("runner_a"))

	// The third in a row suspends it.
	f.pass()
	require.Equal(t, core.StateSuspended, f.state("runner_a"))
	runners, err := f.svc.Runners(f.ctx, "")
	require.NoError(t, err)
	require.Equal(t, core.SuspendedByTests, runners[0].SuspendReason)
	require.Equal(t, 3, runners[0].Failures)

	// It keeps being tested. Two passes are not enough; a failure starts the count over.
	f.prober.Pass("runner_a")
	f.pass()
	f.pass()
	require.Equal(t, core.StateSuspended, f.state("runner_a"))
	f.prober.Fail("runner_a", "no answer within 5s")
	f.pass()
	f.prober.Pass("runner_a")
	f.pass()
	f.pass()
	require.Equal(t, core.StateSuspended, f.state("runner_a"))
	f.pass()
	require.Equal(t, core.StateActive, f.state("runner_a"))
	runners, err = f.svc.Runners(f.ctx, "")
	require.NoError(t, err)
	require.Empty(t, runners[0].SuspendReason)
}

func TestOnlyRunnersThatCanBeTestedAre(t *testing.T) {
	f := newFixture(t)
	tested, untested := f.testedApp("echo"), f.app("flux/schnell")
	member, _, credential := f.pooled(tested, untested)
	f.runner(member, credential, "runner_tested", tested)
	f.runner(member, credential, "runner_untested", untested)
	f.orch.Register(core.OrchRunner{RunnerID: "runner_static", MemberID: "operator", App: "echo", State: core.StateActive, Ready: true})

	// A runner that is not ready is not asked: it takes no work either way.
	f.orch.Update("runner_tested", func(r *core.OrchRunner) { r.Ready = false })
	f.pass()
	require.Empty(t, f.prober.Probed)
	require.Equal(t, core.StateProbation, f.state("runner_tested"))

	f.orch.Update("runner_tested", func(r *core.OrchRunner) { r.Ready = true })
	f.pass()
	require.Equal(t, []string{"runner_tested"}, f.prober.Probed)
	require.Equal(t, core.StateActive, f.state("runner_tested"))
	// An app with no test defined is left for the operator to promote.
	require.Equal(t, core.StateProbation, f.state("runner_untested"))
	_, err := f.svc.TestRunnerNow(f.ctx, operator, "runner_untested")
	requireKind(t, err, core.Conflict, "flux/schnell has no test defined")
}

func TestOperatorsHoldStopsThePoolDecidingForARunner(t *testing.T) {
	f := newFixture(t)
	app := f.testedApp("echo")
	member, _, credential := f.pooled(app)
	f.runner(member, credential, "runner_a", app)

	// The operator sets the state by hand: that is a hold.
	require.NoError(t, f.svc.SetRunnerState(f.ctx, operator, "runner_a", core.StateSuspended, "investigating"))
	f.pass()
	f.pass()
	f.pass()
	require.Equal(t, core.StateSuspended, f.state("runner_a"), "passing tests do not bring back a runner the operator suspended")
	require.Equal(t, 3, f.prober.Count("runner_a"), "it is still tested, so the operator can see how it does")
	runners, err := f.svc.Runners(f.ctx, "")
	require.NoError(t, err)
	require.True(t, runners[0].Hold)
	require.Equal(t, "state set to suspended by the operator", runners[0].HoldReason)

	// Lifting the hold hands it back: three passes later it is active.
	require.NoError(t, f.svc.SetRunnerHold(f.ctx, operator, "runner_a", false, ""))
	f.pass()
	f.pass()
	require.Equal(t, core.StateSuspended, f.state("runner_a"))
	f.pass()
	require.Equal(t, core.StateActive, f.state("runner_a"))

	// A hold on an active runner keeps it active through failures.
	require.NoError(t, f.svc.SetRunnerHold(f.ctx, operator, "runner_a", true, "known flaky test"))
	f.prober.Fail("runner_a", "no answer within 5s")
	for i := 0; i < 4; i++ {
		f.pass()
	}
	require.Equal(t, core.StateActive, f.state("runner_a"))

	requireKind(t, f.svc.SetRunnerHold(f.ctx, operator, "runner_missing", true, ""), core.NotFound, "")
}

func TestRunnerLeftOnAnOldVersionIsSuspendedAtTheDeadline(t *testing.T) {
	f := newFixture(t)
	app := f.testedApp("echo")
	member, _, credential := f.pooled(app)
	f.runner(member, credential, "runner_old", app)
	require.NoError(t, f.svc.Reconcile(f.ctx))
	require.Equal(t, core.StateActive, f.state("runner_old"))

	deadline := f.clock.Now().Add(2 * time.Hour)
	updated, err := f.svc.SetAppVersion(f.ctx, operator, app.ID, "2.0.0", &deadline)
	require.NoError(t, err)
	f.runner(member, credential, "runner_new", updated) // replaces runner_old in the pool's books
	// Put the old one back as if a second machine still ran it.
	_, other := f.member(testkit.NewWallet(t), "Bob", f.gpu("NVIDIA RTX 3090", 24000), updated)
	bob, err := f.svc.Member(f.ctx, other.MemberID)
	require.NoError(t, err)
	onOld := func(r *core.AdmissionRunner) { r.Version = "1.0.0" }
	require.True(t, f.admit(f.credential(bob, other), "runner_bob", updated, onOld).Allow)
	f.orch.Register(core.OrchRunner{RunnerID: "runner_bob", MemberID: bob.ID, App: "echo", Mode: app.Mode, Version: "1.0.0",
		State: core.StateActive, Status: "ready", Ready: true, Capacity: 2})

	// Before the deadline the old version keeps working.
	f.clock.Advance(time.Hour)
	require.NoError(t, f.svc.Reconcile(f.ctx))
	require.Equal(t, core.StateActive, f.state("runner_bob"))

	// After it, the runner is suspended, and passing tests do not bring it back.
	f.clock.Advance(time.Hour)
	require.NoError(t, f.svc.Reconcile(f.ctx))
	require.Equal(t, core.StateSuspended, f.state("runner_bob"))
	for i := 0; i < 4; i++ {
		f.pass()
	}
	require.Equal(t, core.StateSuspended, f.state("runner_bob"))
	require.Equal(t, core.StateActive, f.state("runner_new"))
	runners, err := f.svc.Runners(f.ctx, bob.ID)
	require.NoError(t, err)
	require.Equal(t, core.SuspendedByVersion, runners[0].SuspendReason)
}

func TestUnassigningLetsRunningSessionsFinish(t *testing.T) {
	f := newFixture(t)
	echo, vllm := f.testedApp("echo"), f.testedApp("vllm/qwen2.5-7b-instruct")
	member, machine, credential := f.pooled(echo)
	f.runner(member, credential, "runner_echo", echo)
	require.NoError(t, f.svc.Reconcile(f.ctx))
	require.Equal(t, core.StateActive, f.state("runner_echo"))
	// A customer is in the middle of a session.
	f.orch.Update("runner_echo", func(r *core.OrchRunner) { r.CapacityUsed = 1 })

	_, err := f.svc.SetAssignments(f.ctx, operator, machine.ID, []core.AssignmentInput{{AppID: vllm.ID}})
	require.NoError(t, err)

	// It stops taking new work at the next pass and is not removed while busy.
	require.NoError(t, f.svc.Reconcile(f.ctx))
	require.Equal(t, core.StateSuspended, f.state("runner_echo"))
	f.pass()
	require.NotNil(t, f.orch.Runner("runner_echo"))
	require.Empty(t, f.orch.Evicted())
	require.False(t, f.admit(credential, "runner_echo2", echo).Allow, "and the machine cannot register the app again")

	// The session ends; the runner goes and the assignment with it.
	f.orch.Update("runner_echo", func(r *core.OrchRunner) { r.CapacityUsed = 0 })
	require.NoError(t, f.svc.Reconcile(f.ctx))
	require.Equal(t, []string{"runner_echo"}, f.orch.Evicted())
	updated, err := f.svc.Machine(f.ctx, machine.ID)
	require.NoError(t, err)
	require.Len(t, updated.Assignments, 1)
	require.Equal(t, "vllm/qwen2.5-7b-instruct", updated.Assignments[0].AppName)
}

func TestAssigningAnAppAgainWhileItDrainsKeepsItsRunner(t *testing.T) {
	f := newFixture(t)
	echo := f.testedApp("echo")
	member, machine, credential := f.pooled(echo)
	f.runner(member, credential, "runner_echo", echo)
	require.NoError(t, f.svc.Reconcile(f.ctx))
	f.orch.Update("runner_echo", func(r *core.OrchRunner) { r.CapacityUsed = 1 })

	_, err := f.svc.SetAssignments(f.ctx, operator, machine.ID, nil)
	require.NoError(t, err)
	require.NoError(t, f.svc.Reconcile(f.ctx))
	require.Equal(t, core.StateSuspended, f.state("runner_echo"))

	// The operator changes their mind before the session ended.
	_, err = f.svc.SetAssignments(f.ctx, operator, machine.ID, []core.AssignmentInput{{AppID: echo.ID}})
	require.NoError(t, err)
	f.orch.Update("runner_echo", func(r *core.OrchRunner) { r.CapacityUsed = 0 })
	f.pass()
	f.pass()
	f.pass()
	require.Empty(t, f.orch.Evicted())
	require.Equal(t, core.StateActive, f.state("runner_echo"), "back once it has passed its tests again")
}

func TestRunnersThatLeftAreForgottenAfterAGracePeriod(t *testing.T) {
	f := newFixture(t)
	app := f.testedApp("echo")
	member, _, credential := f.pooled(app)

	// Admitted, but the orchestrator has not listed it yet: not gone.
	require.True(t, f.admit(credential, "runner_a", app).Allow)
	require.NoError(t, f.svc.Reconcile(f.ctx))
	f.orch.Register(core.OrchRunner{RunnerID: "runner_a", MemberID: member.ID, App: "echo", Mode: app.Mode, Version: "1.0.0",
		State: core.StateProbation, Status: "ready", Ready: true})
	runners, err := f.svc.Runners(f.ctx, "")
	require.NoError(t, err)
	require.False(t, runners[0].Unknown)

	// It expires on the orchestrator. A later registration with the same
	// credential replaces nothing, because the pool noticed it had gone.
	require.NoError(t, f.orch.EvictRunner(f.ctx, "runner_a", "expired"))
	f.clock.Advance(3 * time.Minute)
	require.NoError(t, f.svc.Reconcile(f.ctx))
	evictedBefore := len(f.orch.Evicted())
	require.True(t, f.admit(credential, "runner_b", app).Allow)
	require.Len(t, f.orch.Evicted(), evictedBefore)
}

func TestOperatorTestsARunnerOnDemand(t *testing.T) {
	f := newFixture(t)
	app := f.testedApp("echo")
	member, _, credential := f.pooled(app)
	f.runner(member, credential, "runner_a", app)

	result, err := f.svc.TestRunnerNow(f.ctx, operator, "runner_a")
	require.NoError(t, err)
	require.True(t, result.OK)
	require.Equal(t, int64(120), result.LatencyMs)
	require.Equal(t, core.StateActive, f.state("runner_a"), "a test on demand counts like any other")

	_, err = f.svc.TestRunnerNow(f.ctx, operator, "runner_missing")
	requireKind(t, err, core.NotFound, "")
	f.orch.Down = errDown
	_, err = f.svc.TestRunnerNow(f.ctx, operator, "runner_a")
	requireKind(t, err, core.Unavailable, "")
	requireKind(t, f.svc.Reconcile(f.ctx), core.Unavailable, "")
}
