package core_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Livepeer-Open-Pool/pool/internal/core"
	"github.com/Livepeer-Open-Pool/pool/internal/testkit"
)

func TestMachineIsAdmittedOnProbationOnlyForWhatItWasAssigned(t *testing.T) {
	f := newFixture(t)
	gpu := f.gpu("NVIDIA RTX 4090", 24564)
	vllm := f.app("vllm/qwen2.5-7b-instruct")
	flux := f.app("flux/schnell", func(in *core.AppInput) { in.Mode = core.ModePersistent })
	f.publishTerms()
	alice := testkit.NewWallet(t)
	member, machine := f.member(alice, "Alice", gpu, vllm)
	credential := f.credential(member, machine)

	resp := f.admit(credential, "runner_aaaa", vllm)
	require.Equal(t, core.AdmissionResponse{
		Allow: true, MemberID: member.ID, PayoutAddress: alice.Address, MaxRunners: 1, State: core.StateProbation,
	}, resp)

	refused := map[string]struct {
		app    *core.App
		adjust func(*core.AdmissionRunner)
		reason string
	}{
		"an app it was not assigned": {flux, nil, "this machine is not assigned flux/schnell"},
		"an app the pool does not have": {vllm, func(r *core.AdmissionRunner) { r.App = "llama/huge" },
			"this machine is not assigned llama/huge"},
		"the wrong mode": {vllm, func(r *core.AdmissionRunner) { r.Mode = core.ModePersistent },
			"vllm/qwen2.5-7b-instruct runs in single-shot mode"},
		"the wrong version": {vllm, func(r *core.AdmissionRunner) { r.Version = "0.9.0" },
			"vllm/qwen2.5-7b-instruct requires version 1.0.0"},
		"more capacity than it was given": {vllm, func(r *core.AdmissionRunner) { r.Capacity = 3 },
			"capacity 3 exceeds this machine's limit of 2 for vllm/qwen2.5-7b-instruct"},
	}
	for name, tc := range refused {
		t.Run(name, func(t *testing.T) {
			adjust := []func(*core.AdmissionRunner){}
			if tc.adjust != nil {
				adjust = append(adjust, tc.adjust)
			}
			resp := f.admit(credential, "runner_bbbb", tc.app, adjust...)
			require.Equal(t, core.AdmissionResponse{Allow: false, Reason: tc.reason}, resp)
		})
	}

	for name, bad := range map[string]string{"no credential": "", "a made-up credential": "opc_nope", "the pool secret": "pool-secret"} {
		resp := f.admit(bad, "runner_cccc", vllm)
		require.Equal(t, core.AdmissionResponse{Allow: false, Reason: "unknown credential"}, resp, name)
	}
}

func TestAdmissionFollowsTheCatalog(t *testing.T) {
	f := newFixture(t)
	gpu := f.gpu("NVIDIA RTX 4090", 24564)
	echo := f.app("echo")
	f.publishTerms()
	member, machine := f.member(testkit.NewWallet(t), "Alice", gpu, echo)
	credential := f.credential(member, machine)

	// A paused app admits nobody.
	_, err := f.svc.SetAppOffered(f.ctx, operator, echo.ID, false)
	require.NoError(t, err)
	resp := f.admit(credential, "runner_aaaa", echo)
	require.Equal(t, "echo is paused by the pool", resp.Reason)
	_, err = f.svc.SetAppOffered(f.ctx, operator, echo.ID, true)
	require.NoError(t, err)

	// After a version change the new version is admitted at once, and the
	// old one only until its deadline.
	deadline := f.clock.Now().Add(time.Hour)
	updated, err := f.svc.SetAppVersion(f.ctx, operator, echo.ID, "2.0.0", nil, &deadline)
	require.NoError(t, err)
	require.NoError(t, f.svc.Reconcile(f.ctx)) // nothing is running there, so the machine is told at once
	require.True(t, f.admit(credential, "runner_new", updated).Allow)
	onOld := func(r *core.AdmissionRunner) { r.Version = "1.0.0" }
	require.True(t, f.admit(credential, "runner_old", updated, onOld).Allow)
	f.clock.Advance(time.Hour)
	resp = f.admit(credential, "runner_old2", updated, onOld)
	require.False(t, resp.Allow)
	require.Equal(t, "echo requires version 2.0.0", resp.Reason)
}

func TestNewRegistrationReplacesTheMachinesOldRunner(t *testing.T) {
	f := newFixture(t)
	gpu := f.gpu("NVIDIA RTX 4090", 24564)
	echo, vllm := f.app("echo"), f.app("vllm/qwen2.5-7b-instruct")
	f.publishTerms()
	member, machine := f.member(testkit.NewWallet(t), "Alice", gpu, echo, vllm)
	credential := f.credential(member, machine)

	require.True(t, f.admit(credential, "runner_echo1", echo).Allow)
	f.orch.Register(core.OrchRunner{RunnerID: "runner_echo1", MemberID: member.ID, App: "echo"})
	resp := f.admit(credential, "runner_vllm1", vllm)
	require.True(t, resp.Allow)
	require.Equal(t, 2, resp.MaxRunners, "one runner per assigned app")
	f.orch.Register(core.OrchRunner{RunnerID: "runner_vllm1", MemberID: member.ID, App: "vllm/qwen2.5-7b-instruct"})
	require.Empty(t, f.orch.Evicted(), "a second app on the machine replaces nothing")

	// The machine drops off the network and comes back: its old echo runner
	// is still on the orchestrator when the new one registers.
	require.True(t, f.admit(credential, "runner_echo2", echo).Allow)
	require.Equal(t, []string{"runner_echo1"}, f.orch.Evicted())

	// The old runner may also be gone already; that is not an error.
	require.True(t, f.admit(credential, "runner_echo3", echo).Allow)
	require.Equal(t, []string{"runner_echo1", "runner_echo2"}, f.orch.Evicted())

	// Nor is an orchestrator that does not answer the eviction.
	f.orch.Down = errDown
	require.True(t, f.admit(credential, "runner_echo4", echo).Allow)
}

func TestRunnersAreShownWithTheirMachineAndOwner(t *testing.T) {
	f := newFixture(t)
	gpu := f.gpu("NVIDIA RTX 4090", 24564)
	echo := f.app("echo")
	f.publishTerms()
	alice, aliceMachine := f.member(testkit.NewWallet(t), "Alice", gpu, echo)
	bob, bobMachine := f.member(testkit.NewWallet(t), "Bob", gpu, echo)
	require.True(t, f.admit(f.credential(alice, aliceMachine), "runner_alice", echo).Allow)
	require.True(t, f.admit(f.credential(bob, bobMachine), "runner_bob", echo).Allow)
	f.orch.Register(core.OrchRunner{RunnerID: "runner_alice", MemberID: alice.ID, App: "echo", State: core.StateProbation})
	f.orch.Register(core.OrchRunner{RunnerID: "runner_bob", MemberID: bob.ID, App: "echo", State: core.StateProbation})
	f.orch.Register(core.OrchRunner{RunnerID: "runner_static", MemberID: "operator", App: "echo", Static: true})

	all, err := f.svc.Runners(f.ctx, "")
	require.NoError(t, err)
	require.Len(t, all, 3)
	byID := map[string]core.Runner{}
	for _, r := range all {
		byID[r.RunnerID] = r
	}
	require.Equal(t, aliceMachine.ID, byID["runner_alice"].MachineID)
	require.Equal(t, "box-1", byID["runner_alice"].MachineLabel)
	require.Equal(t, "Alice", byID["runner_alice"].MemberName)
	require.False(t, byID["runner_alice"].Unknown)
	require.True(t, byID["runner_static"].Unknown, "a runner the pool did not admit is flagged")

	mine, err := f.svc.Runners(f.ctx, alice.ID)
	require.NoError(t, err)
	require.Len(t, mine, 1)
	require.Equal(t, "runner_alice", mine[0].RunnerID)

	// The operator moves a runner between states and evicts it.
	require.NoError(t, f.svc.SetRunnerState(f.ctx, operator, "runner_alice", core.StateActive, "tested by hand"))
	requireKind(t, f.svc.SetRunnerState(f.ctx, operator, "runner_alice", "trusted", ""), core.Invalid, "")
	requireKind(t, f.svc.SetRunnerState(f.ctx, operator, "runner_missing", core.StateActive, ""), core.NotFound, "")
	require.NoError(t, f.svc.EvictRunner(f.ctx, operator, "runner_bob", "slow"))
	requireKind(t, f.svc.EvictRunner(f.ctx, operator, "runner_bob", "slow"), core.NotFound, "")
	all, err = f.svc.Runners(f.ctx, "")
	require.NoError(t, err)
	require.Len(t, all, 2)

	f.orch.Down = errDown
	_, err = f.svc.Runners(f.ctx, "")
	requireKind(t, err, core.Unavailable, "")
	requireKind(t, f.svc.EvictRunner(f.ctx, operator, "runner_alice", ""), core.Unavailable, "")
}

func TestEverythingTheOperatorDoesIsRecorded(t *testing.T) {
	f := newFixture(t)
	gpu := f.gpu("NVIDIA RTX 4090", 24564)
	echo := f.app("echo")
	f.publishTerms()
	alice := testkit.NewWallet(t)
	member, _ := f.member(alice, "Alice", gpu, echo)
	_, err := f.svc.BanMember(f.ctx, operator, member.ID, "malicious")
	require.NoError(t, err)

	entries, err := f.svc.Audit(f.ctx, 0)
	require.NoError(t, err)
	actions := []string{}
	for i := len(entries) - 1; i >= 0; i-- {
		actions = append(actions, entries[i].Actor+" "+entries[i].Action)
	}
	require.Equal(t, []string{
		operator + " gpu.create",
		operator + " app.create",
		operator + " terms.publish",
		alice.Address + " application.submit",
		operator + " application.approve",
		operator + " member.ban",
	}, actions)
	require.Equal(t, "malicious", entries[0].Detail)
}
