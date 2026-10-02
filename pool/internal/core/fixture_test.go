package core_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Livepeer-Open-Pool/pool/internal/auth"
	"github.com/Livepeer-Open-Pool/pool/internal/core"
	"github.com/Livepeer-Open-Pool/pool/internal/testkit"
)

const operator = "admin:ops@open-pool.com"

// fixture is a pool service over a fresh database, with a stand-in
// orchestrator, a mailbox and a clock the test controls.
type fixture struct {
	t      *testing.T
	ctx    context.Context
	svc    *core.Service
	orch   *testkit.Orchestrator
	prober *testkit.Prober
	mail   *testkit.Mailbox
	clock  *testkit.Clock
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{t: t, ctx: context.Background(), orch: &testkit.Orchestrator{}, prober: &testkit.Prober{}, mail: &testkit.Mailbox{}, clock: testkit.NewClock()}
	f.svc = core.New(testkit.OpenDB(t), f.orch, f.mail, core.Config{
		RegionID:        "us-east",
		RegionName:      "US East",
		PublicURL:       "https://us-east.api.open-pool.com",
		OrchestratorURL: "https://orch.us-east.open-pool.com:8935",
		AdminURL:        "https://admin.open-pool.com",
		RecoverSigner:   auth.RecoverSigner,
		Now:             f.clock.Now,
	}, nil)
	f.svc.SetProber(f.prober)
	return f
}

// testedApp is an app with a test defined, so its runners are tested.
func (f *fixture) testedApp(name string) *core.App {
	f.t.Helper()
	return f.app(name, func(in *core.AppInput) {
		in.Test = core.Test{Method: "GET", Path: "/v1/models", ExpectStatus: 200, ExpectContains: []string{"data"}, TimeoutMs: 5000}
	})
}

// runner admits a runner for the machine and registers it on the stand-in
// orchestrator, on probation and ready, as the real one would.
func (f *fixture) runner(member *core.Member, credential, runnerID string, app *core.App) {
	f.t.Helper()
	require.True(f.t, f.admit(credential, runnerID, app).Allow)
	f.orch.Register(core.OrchRunner{
		RunnerID: runnerID, MemberID: member.ID, App: app.Name, Mode: app.Mode, Version: app.RequiredVersion,
		State: core.StateProbation, Status: "ready", Ready: true, Capacity: 2, Tunnel: true, TunnelConnected: true,
	})
}

func (f *fixture) state(runnerID string) string {
	f.t.Helper()
	r := f.orch.Runner(runnerID)
	require.NotNil(f.t, r, "runner %s is not registered", runnerID)
	return r.State
}

// pass runs one reconcile pass after moving the clock past the test interval.
func (f *fixture) pass() {
	f.t.Helper()
	f.clock.Advance(10 * time.Minute)
	require.NoError(f.t, f.svc.Reconcile(f.ctx))
}

// requireKind asserts err is a core error of the kind, optionally with the message.
func requireKind(t *testing.T, err error, kind core.ErrorKind, message string) {
	t.Helper()
	var coreErr *core.Error
	require.True(t, errors.As(err, &coreErr), "expected a core error, got %v", err)
	require.Equal(t, kind, coreErr.Kind, coreErr.Message)
	if message != "" {
		require.Equal(t, message, coreErr.Message)
	}
}

func (f *fixture) gpu(name string, vramMB int) *core.GPU {
	f.t.Helper()
	gpu, err := f.svc.CreateGPU(f.ctx, operator, core.GPUInput{Name: name, VRAMMB: vramMB})
	require.NoError(f.t, err)
	return gpu
}

// appInput is a valid single-shot app definition tests adjust.
func appInput(name string) core.AppInput {
	return core.AppInput{
		Name:            name,
		Mode:            core.ModeSingleShot,
		Price:           core.Price{Amount: "0.50", Unit: "hour"},
		RequiredVersion: "1.0.0",
		Container:       core.Container{Image: testkit.PinnedImage, Port: 8000, HealthPath: "/health"},
		DefaultCapacity: 2,
		MaxCapacity:     4,
	}
}

func (f *fixture) app(name string, adjust ...func(*core.AppInput)) *core.App {
	f.t.Helper()
	in := appInput(name)
	for _, fn := range adjust {
		fn(&in)
	}
	app, err := f.svc.CreateApp(f.ctx, operator, in)
	require.NoError(f.t, err)
	return app
}

func (f *fixture) publishTerms() {
	f.t.Helper()
	_, err := f.svc.PublishTerms(f.ctx, operator, core.TermsGlobal, "Global terms, version one.")
	require.NoError(f.t, err)
}

// apply signs and submits an application the way the member site does.
func (f *fixture) apply(wallet *testkit.Wallet, draft core.ApplicationDraft) *core.Application {
	f.t.Helper()
	message, err := f.svc.ApplicationMessage(f.ctx, wallet.Address, draft)
	require.NoError(f.t, err)
	application, err := f.svc.SubmitApplication(f.ctx, wallet.Address, draft, wallet.Sign(f.t, message))
	require.NoError(f.t, err)
	return application
}

// member applies with one machine carrying one of gpu and approves it with
// the apps assigned, returning the member and that machine.
func (f *fixture) member(wallet *testkit.Wallet, name string, gpu *core.GPU, apps ...*core.App) (*core.Member, *core.Machine) {
	f.t.Helper()
	application := f.apply(wallet, core.ApplicationDraft{
		DisplayName: name,
		Machines:    []core.MachineSpec{{Label: "box-1", GPUs: []core.MachineGPU{{GPUID: gpu.ID, Count: 2}}}},
	})
	assignments := []core.AssignmentInput{}
	for _, app := range apps {
		assignments = append(assignments, core.AssignmentInput{AppID: app.ID})
	}
	member, err := f.svc.ApproveApplication(f.ctx, operator, application.ID, []core.MachineAssignments{{Machine: 0, Assignments: assignments}})
	require.NoError(f.t, err)
	machines, err := f.svc.Machines(f.ctx, member.ID, "")
	require.NoError(f.t, err)
	require.Len(f.t, machines, 1)
	return member, &machines[0]
}

func (f *fixture) credential(member *core.Member, machine *core.Machine) string {
	f.t.Helper()
	credential, err := f.svc.CreateCredential(f.ctx, member, machine.ID)
	require.NoError(f.t, err)
	return credential.Secret
}

// admit asks the pool service to admit a runner as the orchestrator does.
func (f *fixture) admit(credential, runnerID string, app *core.App, adjust ...func(*core.AdmissionRunner)) core.AdmissionResponse {
	f.t.Helper()
	runner := core.AdmissionRunner{
		RunnerID: runnerID, Tunnel: true, Version: app.RequiredVersion, Status: "ready", Mode: app.Mode, App: app.Name, Capacity: 2,
	}
	for _, fn := range adjust {
		fn(&runner)
	}
	resp, err := f.svc.Admit(f.ctx, core.AdmissionRequest{
		Event: "register", OrchAddress: "0xAA", RemoteAddr: "203.0.113.7", Credential: credential, Runner: runner,
	})
	require.NoError(f.t, err)
	return resp
}

var errDown = errors.New("connection refused")
