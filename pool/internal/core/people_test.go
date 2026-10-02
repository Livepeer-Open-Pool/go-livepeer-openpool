package core_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Livepeer-Open-Pool/pool/internal/core"
	"github.com/Livepeer-Open-Pool/pool/internal/testkit"
)

func oneMachine(gpu *core.GPU) []core.MachineSpec {
	return []core.MachineSpec{{Label: "box-1", GPUs: []core.MachineGPU{{GPUID: gpu.ID, Count: 2}}}}
}

func TestApplyingSignsTheTermsInForce(t *testing.T) {
	f := newFixture(t)
	gpu := f.gpu("NVIDIA RTX 4090", 24564)
	alice := testkit.NewWallet(t)
	draft := core.ApplicationDraft{DisplayName: " Alice ", Machines: oneMachine(gpu)}

	// No terms, no applications.
	_, err := f.svc.ApplicationMessage(f.ctx, alice.Address, draft)
	requireKind(t, err, core.Conflict, "")

	f.publishTerms()
	_, err = f.svc.PublishTerms(f.ctx, operator, core.TermsRegional, "Regional terms.")
	require.NoError(t, err)
	terms, err := f.svc.Terms(f.ctx)
	require.NoError(t, err)

	message, err := f.svc.ApplicationMessage(f.ctx, alice.Address, draft)
	require.NoError(t, err)
	require.Equal(t, "Open Pool application\n\n"+
		"Region: us-east\n"+
		"Address: "+alice.Address+"\n"+
		"Display name: Alice\n"+
		"Machines: 1\n"+
		"Global terms: version 1, sha256 "+terms.Global.Hash+"\n"+
		"Regional terms: version 1, sha256 "+terms.Regional.Hash+"\n"+
		"\nBy signing I accept these terms. Payouts go to the address above, which cannot be changed.", message)

	application, err := f.svc.SubmitApplication(f.ctx, alice.Address, draft, alice.Sign(t, message))
	require.NoError(t, err)
	require.Equal(t, core.ApplicationPending, application.Status)
	require.Equal(t, alice.Address, application.Address)
	require.Equal(t, "Alice", application.DisplayName)
	require.Equal(t, 1, application.GlobalTermsVersion)
	require.Equal(t, terms.Global.Hash, application.GlobalTermsHash)
	require.Equal(t, terms.Regional.Hash, application.RegionalTermsHash)

	// The operator is told.
	notices := f.mail.AllNotices()
	require.Len(t, notices, 1)
	require.Equal(t, "Open Pool US East: application from Alice needs approval", notices[0].Subject)
	require.Contains(t, notices[0].Body, alice.Address)
	require.Contains(t, notices[0].Body, "https://admin.open-pool.com/applications/"+application.ID+"?region=us-east")

	// One application at a time.
	_, err = f.svc.ApplicationMessage(f.ctx, alice.Address, draft)
	requireKind(t, err, core.Conflict, "this address already has an application waiting for a decision")
}

func TestApplicationSignatureMustBeTheApplicantsOverThisApplication(t *testing.T) {
	f := newFixture(t)
	gpu := f.gpu("NVIDIA RTX 4090", 24564)
	f.publishTerms()
	alice, mallory := testkit.NewWallet(t), testkit.NewWallet(t)
	draft := core.ApplicationDraft{DisplayName: "Alice", Machines: oneMachine(gpu)}
	message, err := f.svc.ApplicationMessage(f.ctx, alice.Address, draft)
	require.NoError(t, err)

	_, err = f.svc.SubmitApplication(f.ctx, alice.Address, draft, mallory.Sign(t, message))
	requireKind(t, err, core.Conflict, "")
	_, err = f.svc.SubmitApplication(f.ctx, alice.Address, draft, "0x1234")
	requireKind(t, err, core.Invalid, "")

	// The draft changed after the message was signed.
	other := core.ApplicationDraft{DisplayName: "Alice the Second", Machines: oneMachine(gpu)}
	_, err = f.svc.SubmitApplication(f.ctx, alice.Address, other, alice.Sign(t, message))
	requireKind(t, err, core.Conflict, "")

	// The terms changed after the message was signed: the signature is not acceptance of the new ones.
	signature := alice.Sign(t, message)
	_, err = f.svc.PublishTerms(f.ctx, operator, core.TermsGlobal, "Global terms, version two.")
	require.NoError(t, err)
	_, err = f.svc.SubmitApplication(f.ctx, alice.Address, draft, signature)
	requireKind(t, err, core.Conflict, "the signature does not match this application; fetch the message again and sign it")

	applications, err := f.svc.Applications(f.ctx, "", "")
	require.NoError(t, err)
	require.Empty(t, applications)
	require.Empty(t, f.mail.AllNotices())
}

func TestApplicationDraftIsValidated(t *testing.T) {
	f := newFixture(t)
	gpu := f.gpu("NVIDIA RTX 4090", 24564)
	retired := f.gpu("NVIDIA GTX 1080", 8192)
	no := false
	_, err := f.svc.UpdateGPU(f.ctx, operator, retired.ID, core.GPUInput{Name: retired.Name, VRAMMB: retired.VRAMMB, Accepted: &no})
	require.NoError(t, err)
	f.publishTerms()
	alice := testkit.NewWallet(t)

	bad := map[string]core.ApplicationDraft{
		"no name":        {Machines: oneMachine(gpu)},
		"name too long":  {DisplayName: strings.Repeat("a", 41), Machines: oneMachine(gpu)},
		"no machines":    {DisplayName: "Alice"},
		"unlabelled":     {DisplayName: "Alice", Machines: []core.MachineSpec{{GPUs: []core.MachineGPU{{GPUID: gpu.ID, Count: 1}}}}},
		"no GPUs":        {DisplayName: "Alice", Machines: []core.MachineSpec{{Label: "box"}}},
		"zero GPUs":      {DisplayName: "Alice", Machines: []core.MachineSpec{{Label: "box", GPUs: []core.MachineGPU{{GPUID: gpu.ID}}}}},
		"unknown GPU":    {DisplayName: "Alice", Machines: []core.MachineSpec{{Label: "box", GPUs: []core.MachineGPU{{GPUID: "gpu_x", Count: 1}}}}},
		"retired GPU":    {DisplayName: "Alice", Machines: []core.MachineSpec{{Label: "box", GPUs: []core.MachineGPU{{GPUID: retired.ID, Count: 1}}}}},
		"same GPU twice": {DisplayName: "Alice", Machines: []core.MachineSpec{{Label: "box", GPUs: []core.MachineGPU{{GPUID: gpu.ID, Count: 1}, {GPUID: gpu.ID, Count: 1}}}}},
		"same label twice": {DisplayName: "Alice", Machines: []core.MachineSpec{
			{Label: "box", GPUs: []core.MachineGPU{{GPUID: gpu.ID, Count: 1}}}, {Label: "box", GPUs: []core.MachineGPU{{GPUID: gpu.ID, Count: 1}}}}},
	}
	for name, draft := range bad {
		t.Run(name, func(t *testing.T) {
			_, err := f.svc.ApplicationMessage(f.ctx, alice.Address, draft)
			requireKind(t, err, core.Invalid, "")
		})
	}
	_, err = f.svc.ApplicationMessage(f.ctx, "not-an-address", core.ApplicationDraft{DisplayName: "Alice", Machines: oneMachine(gpu)})
	requireKind(t, err, core.Invalid, "")
}

func TestApprovingMakesAMemberWithMachinesAndAssignments(t *testing.T) {
	f := newFixture(t)
	rtx4090, rtx3060 := f.gpu("NVIDIA RTX 4090", 24564), f.gpu("NVIDIA RTX 3060", 12288)
	vllm := f.app("vllm/qwen2.5-7b-instruct", func(in *core.AppInput) { in.EligibleGPUs = []string{rtx4090.ID} })
	echo := f.app("echo")
	f.publishTerms()
	alice := testkit.NewWallet(t)
	application := f.apply(alice, core.ApplicationDraft{DisplayName: "Alice", Machines: []core.MachineSpec{
		{Label: "big", GPUs: []core.MachineGPU{{GPUID: rtx4090.ID, Count: 2}}},
		{Label: "small", GPUs: []core.MachineGPU{{GPUID: rtx3060.ID, Count: 1}}},
	}})

	// Assignments are checked before anything is created.
	_, err := f.svc.ApproveApplication(f.ctx, operator, application.ID, []core.MachineAssignments{
		{Machine: 1, Assignments: []core.AssignmentInput{{AppID: vllm.ID}}},
	})
	requireKind(t, err, core.Invalid, `machine "small" has no GPU that vllm/qwen2.5-7b-instruct may run on`)
	_, err = f.svc.ApproveApplication(f.ctx, operator, application.ID, []core.MachineAssignments{
		{Machine: 0, Assignments: []core.AssignmentInput{{AppID: vllm.ID, Capacity: 5}}},
	})
	requireKind(t, err, core.Invalid, "vllm/qwen2.5-7b-instruct takes a capacity between 1 and 4")
	_, err = f.svc.ApproveApplication(f.ctx, operator, application.ID, []core.MachineAssignments{{Machine: 2}})
	requireKind(t, err, core.Invalid, "")
	members, err := f.svc.Members(f.ctx)
	require.NoError(t, err)
	require.Empty(t, members)

	// The big machine runs two apps; the small one is approved and idle.
	member, err := f.svc.ApproveApplication(f.ctx, operator, application.ID, []core.MachineAssignments{
		{Machine: 0, Assignments: []core.AssignmentInput{{AppID: vllm.ID, Capacity: 4}, {AppID: echo.ID}}},
	})
	require.NoError(t, err)
	require.Equal(t, alice.Address, member.Address)
	require.Equal(t, "Alice", member.DisplayName)
	require.Equal(t, core.MemberActive, member.Status)

	machines, err := f.svc.Machines(f.ctx, member.ID, "")
	require.NoError(t, err)
	require.Len(t, machines, 2)
	big, small := machines[0], machines[1]
	if big.Label != "big" {
		big, small = small, big
	}
	require.Equal(t, core.MachineApproved, big.Status)
	require.Len(t, big.Assignments, 2)
	require.Equal(t, "echo", big.Assignments[0].AppName)
	require.Equal(t, 2, big.Assignments[0].Capacity, "the app's default")
	require.Equal(t, "vllm/qwen2.5-7b-instruct", big.Assignments[1].AppName)
	require.Equal(t, 4, big.Assignments[1].Capacity)
	require.Equal(t, core.MachineApproved, small.Status)
	require.Empty(t, small.Assignments)
	require.Nil(t, big.CredentialCreatedAt)

	decided, err := f.svc.Application(f.ctx, application.ID)
	require.NoError(t, err)
	require.Equal(t, core.ApplicationApproved, decided.Status)
	require.NotNil(t, decided.DecidedAt)

	// A decision is final, and a member does not apply again.
	_, err = f.svc.ApproveApplication(f.ctx, operator, application.ID, nil)
	requireKind(t, err, core.Conflict, "")
	_, err = f.svc.RejectApplication(f.ctx, operator, application.ID, "changed my mind")
	requireKind(t, err, core.Conflict, "")
	_, err = f.svc.ApplicationMessage(f.ctx, alice.Address, core.ApplicationDraft{DisplayName: "Alice", Machines: oneMachine(rtx4090)})
	requireKind(t, err, core.Conflict, "this address is already a member here; add machines from your machines page")
}

func TestRejectedApplicantSeesWhyAndMayApplyAgain(t *testing.T) {
	f := newFixture(t)
	gpu := f.gpu("NVIDIA RTX 4090", 24564)
	f.publishTerms()
	alice := testkit.NewWallet(t)
	draft := core.ApplicationDraft{DisplayName: "Alice", Machines: oneMachine(gpu)}
	application := f.apply(alice, draft)

	_, err := f.svc.RejectApplication(f.ctx, operator, application.ID, " ")
	requireKind(t, err, core.Invalid, "")
	rejected, err := f.svc.RejectApplication(f.ctx, operator, application.ID, "we have enough 4090s for now")
	require.NoError(t, err)
	require.Equal(t, core.ApplicationRejected, rejected.Status)
	require.Equal(t, "we have enough 4090s for now", rejected.Reason)

	mine, err := f.svc.Applications(f.ctx, "", alice.Address)
	require.NoError(t, err)
	require.Len(t, mine, 1)
	member, err := f.svc.MemberByAddress(f.ctx, alice.Address)
	require.NoError(t, err)
	require.Nil(t, member)

	f.apply(alice, draft)
	pending, err := f.svc.Applications(f.ctx, core.ApplicationPending, "")
	require.NoError(t, err)
	require.Len(t, pending, 1)
}

func TestBanRemovesAMemberAndTheyCannotComeBack(t *testing.T) {
	f := newFixture(t)
	gpu := f.gpu("NVIDIA RTX 4090", 24564)
	echo := f.app("echo")
	f.publishTerms()
	alice := testkit.NewWallet(t)
	member, machine := f.member(alice, "Alice", gpu, echo)
	credential := f.credential(member, machine)
	require.True(t, f.admit(credential, "runner_aaaa", echo).Allow)
	f.orch.Register(core.OrchRunner{RunnerID: "runner_aaaa", MemberID: member.ID, App: "echo"})

	_, err := f.svc.BanMember(f.ctx, operator, member.ID, "")
	requireKind(t, err, core.Invalid, "")
	banned, err := f.svc.BanMember(f.ctx, operator, member.ID, "returned garbage")
	require.NoError(t, err)
	require.Equal(t, core.MemberBanned, banned.Status)
	require.Equal(t, "returned garbage", banned.Reason)
	require.Equal(t, []string{member.ID}, f.orch.EvictedMembers, "their runners leave the orchestrator")

	// The credential still exists, and admits nothing.
	resp := f.admit(credential, "runner_bbbb", echo)
	require.False(t, resp.Allow)
	require.Equal(t, "this membership is banned", resp.Reason)
	_, err = f.svc.CreateCredential(f.ctx, banned, machine.ID)
	requireKind(t, err, core.Forbidden, "")
	_, err = f.svc.ApplicationMessage(f.ctx, alice.Address, core.ApplicationDraft{DisplayName: "Alice", Machines: oneMachine(gpu)})
	requireKind(t, err, core.Forbidden, "this address is banned from the pool")

	_, err = f.svc.BanMember(f.ctx, operator, member.ID, "again")
	requireKind(t, err, core.Conflict, "")
	unbanned, err := f.svc.UnbanMember(f.ctx, operator, member.ID)
	require.NoError(t, err)
	require.Equal(t, core.MemberActive, unbanned.Status)
	require.True(t, f.admit(credential, "runner_cccc", echo).Allow)
}

func TestBanHoldsWhenTheOrchestratorIsDown(t *testing.T) {
	f := newFixture(t)
	gpu := f.gpu("NVIDIA RTX 4090", 24564)
	echo := f.app("echo")
	f.publishTerms()
	member, machine := f.member(testkit.NewWallet(t), "Alice", gpu, echo)
	credential := f.credential(member, machine)

	f.orch.Down = errDown
	banned, err := f.svc.BanMember(f.ctx, operator, member.ID, "malicious")
	require.NoError(t, err)
	require.Equal(t, core.MemberBanned, banned.Status)
	f.orch.Down = nil
	require.False(t, f.admit(credential, "runner_aaaa", echo).Allow)
}

func TestMemberAddsAndRemovesMachines(t *testing.T) {
	f := newFixture(t)
	gpu := f.gpu("NVIDIA RTX 4090", 24564)
	echo := f.app("echo")
	f.publishTerms()
	alice := testkit.NewWallet(t)
	member, first := f.member(alice, "Alice", gpu, echo)

	// A machine added later waits for the operator, who is told.
	second, err := f.svc.RequestMachine(f.ctx, member, core.MachineSpec{Label: "box-2", GPUs: []core.MachineGPU{{GPUID: gpu.ID, Count: 1}}})
	require.NoError(t, err)
	require.Equal(t, core.MachinePending, second.Status)
	notices := f.mail.AllNotices()
	require.Equal(t, "Open Pool US East: Alice asks to add a machine", notices[len(notices)-1].Subject)
	_, err = f.svc.RequestMachine(f.ctx, member, core.MachineSpec{Label: "box-2", GPUs: []core.MachineGPU{{GPUID: gpu.ID, Count: 1}}})
	requireKind(t, err, core.Conflict, "")

	// Until approved it can have neither a credential nor apps.
	_, err = f.svc.CreateCredential(f.ctx, member, second.ID)
	requireKind(t, err, core.Conflict, `machine "box-2" has not been approved yet`)
	_, err = f.svc.SetAssignments(f.ctx, operator, second.ID, []core.AssignmentInput{{AppID: echo.ID}})
	requireKind(t, err, core.Conflict, "")
	pending, err := f.svc.Machines(f.ctx, "", core.MachinePending)
	require.NoError(t, err)
	require.Len(t, pending, 1)

	approved, err := f.svc.ApproveMachine(f.ctx, operator, second.ID)
	require.NoError(t, err)
	require.Equal(t, core.MachineApproved, approved.Status)
	_, err = f.svc.ApproveMachine(f.ctx, operator, second.ID)
	requireKind(t, err, core.Conflict, "")

	// Removing a machine ends its credential, its assignments and its runners.
	credential := f.credential(member, first)
	require.True(t, f.admit(credential, "runner_aaaa", echo).Allow)
	f.orch.Register(core.OrchRunner{RunnerID: "runner_aaaa", MemberID: member.ID, App: "echo"})
	require.NoError(t, f.svc.RemoveMachine(f.ctx, alice.Address, member.ID, first.ID))
	require.Equal(t, []string{"runner_aaaa"}, f.orch.Evicted())
	resp := f.admit(credential, "runner_bbbb", echo)
	require.False(t, resp.Allow)
	require.Equal(t, "unknown credential", resp.Reason)
	machines, err := f.svc.Machines(f.ctx, member.ID, "")
	require.NoError(t, err)
	require.Len(t, machines, 1)
	require.Equal(t, "box-2", machines[0].Label)
	requireKind(t, f.svc.RemoveMachine(f.ctx, alice.Address, member.ID, first.ID), core.NotFound, "")

	// A member cannot touch someone else's machine.
	bob, bobMachine := f.member(testkit.NewWallet(t), "Bob", gpu, echo)
	requireKind(t, f.svc.RemoveMachine(f.ctx, alice.Address, member.ID, bobMachine.ID), core.NotFound, "")
	_, err = f.svc.CreateCredential(f.ctx, member, bobMachine.ID)
	requireKind(t, err, core.NotFound, "")
	// The operator can remove any.
	require.NoError(t, f.svc.RemoveMachine(f.ctx, operator, "", bobMachine.ID))
	bobMachines, err := f.svc.Machines(f.ctx, bob.ID, "")
	require.NoError(t, err)
	require.Empty(t, bobMachines)
}

func TestOperatorReassignsAMachine(t *testing.T) {
	f := newFixture(t)
	gpu := f.gpu("NVIDIA RTX 4090", 24564)
	echo, vllm := f.app("echo"), f.app("vllm/qwen2.5-7b-instruct")
	f.publishTerms()
	member, machine := f.member(testkit.NewWallet(t), "Alice", gpu, echo)
	credential := f.credential(member, machine)
	require.True(t, f.admit(credential, "runner_echo", echo).Allow)
	f.orch.Register(core.OrchRunner{RunnerID: "runner_echo", MemberID: member.ID, App: "echo"})

	// From echo to vLLM: echo's runner is removed, vLLM may register.
	updated, err := f.svc.SetAssignments(f.ctx, operator, machine.ID, []core.AssignmentInput{{AppID: vllm.ID, Capacity: 3}})
	require.NoError(t, err)
	require.Len(t, updated.Assignments, 1)
	require.Equal(t, "vllm/qwen2.5-7b-instruct", updated.Assignments[0].AppName)
	require.Equal(t, 3, updated.Assignments[0].Capacity)
	require.Equal(t, []string{"runner_echo"}, f.orch.Evicted())
	resp := f.admit(credential, "runner_echo2", echo)
	require.False(t, resp.Allow)
	require.Equal(t, "this machine is not assigned echo", resp.Reason)
	require.True(t, f.admit(credential, "runner_vllm", vllm, func(r *core.AdmissionRunner) { r.Capacity = 3 }).Allow)

	// Changing only the capacity keeps the assignment; adding echo back gives the machine two apps.
	updated, err = f.svc.SetAssignments(f.ctx, operator, machine.ID, []core.AssignmentInput{{AppID: vllm.ID, Capacity: 1}, {AppID: echo.ID}})
	require.NoError(t, err)
	require.Len(t, updated.Assignments, 2)
	require.Equal(t, []string{"runner_echo"}, f.orch.Evicted(), "nothing more was evicted")
	require.True(t, f.admit(credential, "runner_echo3", echo).Allow)

	_, err = f.svc.SetAssignments(f.ctx, operator, machine.ID, []core.AssignmentInput{{AppID: echo.ID}, {AppID: echo.ID}})
	requireKind(t, err, core.Invalid, "")
	_, err = f.svc.SetAssignments(f.ctx, operator, "mch_missing", nil)
	requireKind(t, err, core.NotFound, "")
}

func TestCredentialIsShownOnceAndRotates(t *testing.T) {
	f := newFixture(t)
	gpu := f.gpu("NVIDIA RTX 4090", 24564)
	echo := f.app("echo")
	f.publishTerms()
	member, machine := f.member(testkit.NewWallet(t), "Alice", gpu, echo)

	first, err := f.svc.CreateCredential(f.ctx, member, machine.ID)
	require.NoError(t, err)
	require.Regexp(t, `^opc_[a-z2-7]{52}$`, first.Secret)
	require.Equal(t, []string{
		"livepeer_runner_tunnel -orchestrator https://orch.us-east.open-pool.com:8935 -credential " + first.Secret +
			" -target http://127.0.0.1:8000 -app echo -mode single-shot -capacity 2 -version 1.0.0 -health /health",
	}, first.Commands)

	listed, err := f.svc.Machine(f.ctx, machine.ID)
	require.NoError(t, err)
	require.NotNil(t, listed.CredentialCreatedAt, "the machine shows that it has a credential, not what it is")

	// A new credential retires the old one.
	second, err := f.svc.CreateCredential(f.ctx, member, machine.ID)
	require.NoError(t, err)
	require.NotEqual(t, first.Secret, second.Secret)
	require.False(t, f.admit(first.Secret, "runner_aaaa", echo).Allow)
	require.True(t, f.admit(second.Secret, "runner_aaaa", echo).Allow)

	require.NoError(t, f.svc.RevokeCredential(f.ctx, member, machine.ID))
	require.False(t, f.admit(second.Secret, "runner_bbbb", echo).Allow)
	listed, err = f.svc.Machine(f.ctx, machine.ID)
	require.NoError(t, err)
	require.Nil(t, listed.CredentialCreatedAt)
}
