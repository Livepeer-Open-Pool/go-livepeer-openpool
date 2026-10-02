package core_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Livepeer-Open-Pool/pool/internal/core"
	"github.com/Livepeer-Open-Pool/pool/internal/testkit"
)

const newImage = "vllm/vllm-openai@sha256:fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"

// box is one member's machine in a rollout test.
type box struct {
	member     *core.Member
	machine    *core.Machine
	credential string
	runner     string // the id of its registered runner
}

// fleet sets up n members, each with one machine running app, all active.
func (f *fixture) fleet(app *core.App, n int) []*box {
	f.t.Helper()
	gpu := f.gpu("NVIDIA RTX 4090", 24564)
	f.publishTerms()
	boxes := make([]*box, n)
	for i := range boxes {
		member, machine := f.member(testkit.NewWallet(f.t), fmt.Sprintf("Member %d", i), gpu, app)
		b := &box{member: member, machine: machine, credential: f.credential(member, machine), runner: fmt.Sprintf("runner_%d_v1", i)}
		f.runner(member, b.credential, b.runner, app)
		boxes[i] = b
	}
	require.NoError(f.t, f.svc.Reconcile(f.ctx))
	for _, b := range boxes {
		require.Equal(f.t, core.StateActive, f.state(b.runner))
	}
	return boxes
}

// told is the version the machine's agent is told to run.
func (f *fixture) told(b *box) string {
	f.t.Helper()
	deployment, err := f.svc.Deployment(f.ctx, b.credential)
	require.NoError(f.t, err)
	require.Len(f.t, deployment.Apps, 1)
	return deployment.Apps[0].Version
}

// follow does what a machine's agent does: when the machine is told a
// version its runner is not on, the runner is replaced by one that is.
func (f *fixture) follow(b *box, app *core.App) {
	f.t.Helper()
	version := f.told(b)
	if r := f.orch.Runner(b.runner); r != nil && r.Version == version {
		return
	}
	b.runner = fmt.Sprintf("%s_%s", b.machine.ID, version)
	onVersion := func(r *core.AdmissionRunner) { r.Version = version }
	require.True(f.t, f.admit(b.credential, b.runner, app, onVersion).Allow)
	f.orch.Register(core.OrchRunner{
		RunnerID: b.runner, MemberID: b.member.ID, App: app.Name, Mode: app.Mode, Version: version,
		State: core.StateProbation, Status: "ready", Ready: true, Capacity: 2, Tunnel: true, TunnelConnected: true,
	})
}

func (f *fixture) count(boxes []*box, state string) int {
	n := 0
	for _, b := range boxes {
		if r := f.orch.Runner(b.runner); r != nil && r.State == state {
			n++
		}
	}
	return n
}

func TestNewVersionReachesMachinesAFewAtATime(t *testing.T) {
	f := newFixture(t)
	app := f.testedApp("vllm/qwen2.5-7b-instruct")
	boxes := f.fleet(app, 5)

	container := app.Container
	container.Image = newImage
	updated, err := f.svc.SetAppVersion(f.ctx, operator, app.ID, "2.0.0", &container, nil)
	require.NoError(t, err)

	// Nothing moves until the pool's next pass: every machine is still told 1.0.0.
	for _, b := range boxes {
		require.Equal(t, "1.0.0", f.told(b))
	}

	// Customers are in the middle of sessions on every machine but the last.
	for _, b := range boxes[:4] {
		f.orch.Update(b.runner, func(r *core.OrchRunner) { r.CapacityUsed = 1 })
	}
	require.NoError(t, f.svc.Reconcile(f.ctx))

	// A quarter of five machines, rounded up, stop taking work, the idle one
	// first; the rest serve.
	require.Equal(t, 2, f.count(boxes, core.StateSuspended))
	require.Equal(t, 3, f.count(boxes, core.StateActive))
	require.Equal(t, core.StateSuspended, f.state(boxes[4].runner))
	runners, err := f.svc.Runners(f.ctx, boxes[4].member.ID)
	require.NoError(t, err)
	require.Equal(t, core.SuspendedByUpgrade, runners[0].SuspendReason)
	machines, err := f.svc.Machines(f.ctx, boxes[4].member.ID, "")
	require.NoError(t, err)
	require.True(t, machines[0].Assignments[0].Upgrading)
	require.Equal(t, "1.0.0", machines[0].Assignments[0].Version)
	var busy *box
	for _, b := range boxes[:4] {
		if f.state(b.runner) == core.StateSuspended {
			busy = b
		}
	}

	// The idle machine is told the new version on the next pass; the busy one
	// only once its session is done.
	require.NoError(t, f.svc.Reconcile(f.ctx))
	require.Equal(t, "2.0.0", f.told(boxes[4]))
	require.Equal(t, "1.0.0", f.told(busy))
	deployment, err := f.svc.Deployment(f.ctx, boxes[4].credential)
	require.NoError(t, err)
	require.Equal(t, newImage, deployment.Apps[0].Container.Image)
	for _, b := range boxes[:4] {
		f.orch.Update(b.runner, func(r *core.OrchRunner) { r.CapacityUsed = 0 })
	}

	// From here every machine's agent follows what it is told. The app never
	// has fewer than three machines serving, and ends with all five on 2.0.0.
	for pass := 0; pass < 40; pass++ {
		require.NoError(t, f.svc.Reconcile(f.ctx))
		for _, b := range boxes {
			f.follow(b, updated)
		}
		require.GreaterOrEqual(t, f.count(boxes, core.StateActive)+f.count(boxes, core.StateProbation), 3, "pass %d", pass)
		require.LessOrEqual(t, f.count(boxes, core.StateSuspended), 2, "pass %d", pass)
	}
	for _, b := range boxes {
		require.Equal(t, "2.0.0", f.told(b))
		machines, err := f.svc.Machines(f.ctx, b.member.ID, "")
		require.NoError(t, err)
		require.False(t, machines[0].Assignments[0].Upgrading)
		require.Equal(t, "2.0.0", machines[0].Assignments[0].Version)
		r := f.orch.Runner(b.runner)
		require.Equal(t, "2.0.0", r.Version)
		require.Equal(t, core.StateActive, r.State)
	}
}

func TestMachineWaitingItsTurnIsNotOutdated(t *testing.T) {
	f := newFixture(t)
	app := f.testedApp("echo")
	boxes := f.fleet(app, 2)
	// Both machines have sessions that never end, so the one that goes first
	// never finishes and the other never gets its turn.
	for _, b := range boxes {
		f.orch.Update(b.runner, func(r *core.OrchRunner) { r.CapacityUsed = 1 })
	}

	deadline := f.clock.Now().Add(time.Hour)
	updated, err := f.svc.SetAppVersion(f.ctx, operator, app.ID, "2.0.0", nil, &deadline)
	require.NoError(t, err)
	require.NoError(t, f.svc.Reconcile(f.ctx))
	require.Equal(t, 1, f.count(boxes, core.StateSuspended))
	waiting := boxes[0]
	if f.state(waiting.runner) == core.StateSuspended {
		waiting = boxes[1]
	}

	// Past the deadline the waiting machine still serves the version it was
	// told to run, and may reconnect on it.
	f.clock.Advance(2 * time.Hour)
	require.NoError(t, f.svc.Reconcile(f.ctx))
	require.Equal(t, core.StateActive, f.state(waiting.runner))
	onOld := func(r *core.AdmissionRunner) { r.Version = "1.0.0" }
	require.True(t, f.admit(waiting.credential, "runner_reconnected", updated, onOld).Allow)
}

func TestRunnerOnHoldIsNotMoved(t *testing.T) {
	f := newFixture(t)
	app := f.testedApp("echo")
	boxes := f.fleet(app, 1)
	require.NoError(t, f.svc.SetRunnerHold(f.ctx, operator, boxes[0].runner, true, "watching it"))

	_, err := f.svc.SetAppVersion(f.ctx, operator, app.ID, "2.0.0", nil, nil)
	require.NoError(t, err)
	require.NoError(t, f.svc.Reconcile(f.ctx))
	require.Equal(t, core.StateActive, f.state(boxes[0].runner))
	require.Equal(t, "1.0.0", f.told(boxes[0]))

	require.NoError(t, f.svc.SetRunnerHold(f.ctx, operator, boxes[0].runner, false, ""))
	require.NoError(t, f.svc.Reconcile(f.ctx))
	require.Equal(t, core.StateSuspended, f.state(boxes[0].runner))
}

func TestMachineThatDoesNotFollowGoesBackToTheTests(t *testing.T) {
	f := newFixture(t)
	app := f.testedApp("echo")
	boxes := f.fleet(app, 2)
	deadline := f.clock.Now().Add(3 * time.Hour)
	_, err := f.svc.SetAppVersion(f.ctx, operator, app.ID, "2.0.0", nil, &deadline)
	require.NoError(t, err)

	// The first machine is stopped and told the new version, and nothing
	// happens there: its member runs the tunnel by hand.
	require.NoError(t, f.svc.Reconcile(f.ctx))
	require.NoError(t, f.svc.Reconcile(f.ctx))
	if f.state(boxes[0].runner) != core.StateSuspended {
		boxes[0], boxes[1] = boxes[1], boxes[0]
	}
	require.Equal(t, core.StateSuspended, f.state(boxes[0].runner))
	require.Equal(t, "2.0.0", f.told(boxes[0]))
	require.Equal(t, core.StateActive, f.state(boxes[1].runner), "one at a time")

	// After a while the pool moves on to the second machine, and the first
	// one's old runner earns its way back by passing tests.
	f.clock.Advance(16 * time.Minute)
	require.NoError(t, f.svc.Reconcile(f.ctx))
	require.Equal(t, core.StateSuspended, f.state(boxes[1].runner))
	for i := 0; i < 3; i++ {
		f.pass()
	}
	require.Equal(t, core.StateActive, f.state(boxes[0].runner))

	// Until the deadline: then it is outdated.
	f.clock.Advance(3 * time.Hour)
	require.NoError(t, f.svc.Reconcile(f.ctx))
	require.Equal(t, core.StateSuspended, f.state(boxes[0].runner))
	runners, err := f.svc.Runners(f.ctx, boxes[0].member.ID)
	require.NoError(t, err)
	require.Equal(t, core.SuspendedByVersion, runners[0].SuspendReason)
}

func TestMachineWithNothingRunningIsToldAtOnce(t *testing.T) {
	f := newFixture(t)
	app := f.testedApp("echo")
	member, machine, credential := f.pooled(app)
	b := &box{member: member, machine: machine, credential: credential}
	require.Equal(t, "1.0.0", f.told(b))

	_, err := f.svc.SetAppVersion(f.ctx, operator, app.ID, "2.0.0", nil, nil)
	require.NoError(t, err)
	require.NoError(t, f.svc.Reconcile(f.ctx))
	require.Equal(t, "2.0.0", f.told(b))

	// A machine assigned the app after the change starts on the new version.
	other, otherMachine := f.member(testkit.NewWallet(t), "Bob", f.gpu("NVIDIA RTX 3090", 24000), app)
	require.Equal(t, "2.0.0", f.told(&box{member: other, machine: otherMachine, credential: f.credential(other, otherMachine)}))
}

func TestWhatAMachineIsToldToRun(t *testing.T) {
	f := newFixture(t)
	echo, vllm := f.testedApp("echo"), f.testedApp("vllm/qwen2.5-7b-instruct")
	member, machine, credential := f.pooled(echo)

	requireKind(t, errOf(f.svc.Deployment(f.ctx, "opc_wrong")), core.Forbidden, "unknown credential")
	deployment, err := f.svc.Deployment(f.ctx, credential)
	require.NoError(t, err)
	require.Equal(t, machine.ID, deployment.MachineID)
	require.Len(t, deployment.Apps, 1)
	require.Equal(t, "echo", deployment.Apps[0].App)
	require.Equal(t, echo.ID, deployment.Apps[0].AppID)
	require.Equal(t, 2, deployment.Apps[0].Capacity)
	require.Equal(t, echo.Container, deployment.Apps[0].Container)

	// While an app it was unassigned still has sessions, its container stays.
	f.runner(member, credential, "runner_echo", echo)
	require.NoError(t, f.svc.Reconcile(f.ctx))
	f.orch.Update("runner_echo", func(r *core.OrchRunner) { r.CapacityUsed = 1 })
	_, err = f.svc.SetAssignments(f.ctx, operator, machine.ID, []core.AssignmentInput{{AppID: vllm.ID}})
	require.NoError(t, err)
	require.NoError(t, f.svc.Reconcile(f.ctx))
	deployment, err = f.svc.Deployment(f.ctx, credential)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"echo", "vllm/qwen2.5-7b-instruct"}, []string{deployment.Apps[0].App, deployment.Apps[1].App})

	f.orch.Update("runner_echo", func(r *core.OrchRunner) { r.CapacityUsed = 0 })
	require.NoError(t, f.svc.Reconcile(f.ctx))
	deployment, err = f.svc.Deployment(f.ctx, credential)
	require.NoError(t, err)
	require.Len(t, deployment.Apps, 1)
	require.Equal(t, "vllm/qwen2.5-7b-instruct", deployment.Apps[0].App)

	// The agent's report is shown with the machine.
	require.NoError(t, f.svc.RecordAgentReport(f.ctx, credential, core.AgentReport{
		AgentVersion: "0.1.0", Generation: "abc", Containers: []core.AgentContainer{{Service: "app-x", State: "running", Health: "healthy"}},
		GPUs: []core.AgentGPU{{Name: "NVIDIA GeForce RTX 4090", MemoryMB: 24564}},
	}))
	machines, err := f.svc.Machines(f.ctx, member.ID, "")
	require.NoError(t, err)
	require.Equal(t, "0.1.0", machines[0].Reported.AgentVersion)
	require.Equal(t, "NVIDIA GeForce RTX 4090", machines[0].Reported.GPUs[0].Name)
	require.Equal(t, f.clock.Now(), *machines[0].Reported.ReportedAt)
	require.NotNil(t, machines[0].LastSeenAt)

	// A removed machine's credential tells its agent nothing.
	require.NoError(t, f.svc.RemoveMachine(f.ctx, operator, "", machine.ID))
	requireKind(t, errOf(f.svc.Deployment(f.ctx, credential)), core.Forbidden, "unknown credential")
	requireKind(t, f.svc.RecordAgentReport(f.ctx, credential, core.AgentReport{}), core.Forbidden, "unknown credential")
}

func errOf(_ any, err error) error { return err }
