package core_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Livepeer-Open-Pool/pool/internal/core"
)

func TestSupportedGPUs(t *testing.T) {
	f := newFixture(t)
	rtx4090 := f.gpu("NVIDIA RTX 4090", 24564)
	f.gpu("NVIDIA RTX 3060", 12288)

	_, err := f.svc.CreateGPU(f.ctx, operator, core.GPUInput{Name: "NVIDIA RTX 4090", VRAMMB: 1})
	requireKind(t, err, core.Conflict, "")
	_, err = f.svc.CreateGPU(f.ctx, operator, core.GPUInput{Name: " ", VRAMMB: 1})
	requireKind(t, err, core.Invalid, "")
	_, err = f.svc.CreateGPU(f.ctx, operator, core.GPUInput{Name: "x", VRAMMB: 0})
	requireKind(t, err, core.Invalid, "")

	// Retiring a model hides it from applicants and keeps it in the operator's list.
	retired := false
	_, err = f.svc.UpdateGPU(f.ctx, operator, rtx4090.ID, core.GPUInput{Name: "NVIDIA RTX 4090", VRAMMB: 24564, Accepted: &retired})
	require.NoError(t, err)
	accepted, err := f.svc.GPUs(f.ctx, true)
	require.NoError(t, err)
	require.Len(t, accepted, 1)
	require.Equal(t, "NVIDIA RTX 3060", accepted[0].Name)
	all, err := f.svc.GPUs(f.ctx, false)
	require.NoError(t, err)
	require.Len(t, all, 2)

	_, err = f.svc.UpdateGPU(f.ctx, operator, "gpu_missing", core.GPUInput{Name: "x", VRAMMB: 1})
	requireKind(t, err, core.NotFound, "")
}

func TestAppDefinitionIsValidated(t *testing.T) {
	f := newFixture(t)
	gpu := f.gpu("NVIDIA RTX 4090", 24564)

	bad := map[string]func(*core.AppInput){
		"no name":                 func(in *core.AppInput) { in.Name = " " },
		"unknown mode":            func(in *core.AppInput) { in.Mode = "batch" },
		"free":                    func(in *core.AppInput) { in.Price.Amount = "0" },
		"price not a number":      func(in *core.AppInput) { in.Price.Amount = "cheap" },
		"unknown unit":            func(in *core.AppInput) { in.Price.Unit = "minute" },
		"no version":              func(in *core.AppInput) { in.RequiredVersion = "" },
		"image by tag":            func(in *core.AppInput) { in.Container.Image = "vllm/vllm-openai:latest" },
		"image digest too short":  func(in *core.AppInput) { in.Container.Image = "vllm/vllm-openai@sha256:abc" },
		"no port":                 func(in *core.AppInput) { in.Container.Port = 0 },
		"relative health path":    func(in *core.AppInput) { in.Container.HealthPath = "health" },
		"relative cache path":     func(in *core.AppInput) { in.Container.CachePath = "cache" },
		"default above maximum":   func(in *core.AppInput) { in.DefaultCapacity = 5 },
		"no capacity":             func(in *core.AppInput) { in.DefaultCapacity = 0 },
		"unknown eligible GPU":    func(in *core.AppInput) { in.EligibleGPUs = []string{"gpu_missing"} },
		"test without a status":   func(in *core.AppInput) { in.Test = core.Test{Path: "/v1/models", TimeoutMs: 1000} },
		"test without time limit": func(in *core.AppInput) { in.Test = core.Test{Path: "/v1/models", ExpectStatus: 200} },
		"test path not absolute":  func(in *core.AppInput) { in.Test = core.Test{Path: "v1/models", ExpectStatus: 200, TimeoutMs: 1} },
	}
	for name, adjust := range bad {
		t.Run(name, func(t *testing.T) {
			in := appInput("vllm/qwen2.5-7b-instruct")
			adjust(&in)
			_, err := f.svc.CreateApp(f.ctx, operator, in)
			requireKind(t, err, core.Invalid, "")
		})
	}

	app := f.app("vllm/qwen2.5-7b-instruct", func(in *core.AppInput) {
		in.EligibleGPUs = []string{gpu.ID}
		in.Test = core.Test{Method: "post", Path: "/v1/chat/completions", Body: `{"model":"m"}`, ExpectStatus: 200, ExpectContains: []string{"choices"}, TimeoutMs: 20000}
		in.Container.Env = map[string]string{"HF_HOME": "/cache"}
		in.Container.CachePath = "/cache"
	})
	require.True(t, app.Offered)
	require.Equal(t, "POST", app.Test.Method)
	require.Equal(t, []string{gpu.ID}, app.EligibleGPUs)
	require.Equal(t, "/cache", app.Container.Env["HF_HOME"])
	require.Nil(t, app.PreviousVersionDeadline)

	_, err := f.svc.CreateApp(f.ctx, operator, appInput("vllm/qwen2.5-7b-instruct"))
	requireKind(t, err, core.Conflict, "")
}

func TestAppCannotBeRenamedOrChangeMode(t *testing.T) {
	f := newFixture(t)
	app := f.app("echo")

	renamed := appInput("echo-2")
	_, err := f.svc.UpdateApp(f.ctx, operator, app.ID, renamed)
	requireKind(t, err, core.Invalid, "")

	otherMode := appInput("echo")
	otherMode.Mode = core.ModePersistent
	_, err = f.svc.UpdateApp(f.ctx, operator, app.ID, otherMode)
	requireKind(t, err, core.Invalid, "")

	otherVersion := appInput("echo")
	otherVersion.RequiredVersion = "2.0.0"
	_, err = f.svc.UpdateApp(f.ctx, operator, app.ID, otherVersion)
	requireKind(t, err, core.Invalid, "")

	// Everything else can change, and the name may be left out.
	change := appInput("")
	change.RequiredVersion = ""
	change.Price = core.Price{Amount: "0.75", Unit: "hour"}
	change.MaxCapacity = 8
	updated, err := f.svc.UpdateApp(f.ctx, operator, app.ID, change)
	require.NoError(t, err)
	require.Equal(t, "echo", updated.Name)
	require.Equal(t, "1.0.0", updated.RequiredVersion)
	require.Equal(t, "0.75", updated.Price.Amount)
	require.Equal(t, 8, updated.MaxCapacity)
}

func TestCatalogPricesReachTheOrchestrator(t *testing.T) {
	f := newFixture(t)
	vllm := f.app("vllm/qwen2.5-7b-instruct")
	f.app("echo", func(in *core.AppInput) { in.Price = core.Price{Amount: "0.001", Unit: "fixed"} })
	require.Equal(t, map[string]core.Price{
		"vllm/qwen2.5-7b-instruct": {Amount: "0.50", Unit: "hour"},
		"echo":                     {Amount: "0.001", Unit: "fixed"},
	}, f.orch.PushedPrices())

	// A paused app is not priced, so the orchestrator stops offering it.
	_, err := f.svc.SetAppOffered(f.ctx, operator, vllm.ID, false)
	require.NoError(t, err)
	require.Equal(t, map[string]core.Price{"echo": {Amount: "0.001", Unit: "fixed"}}, f.orch.PushedPrices())

	// A change made while the orchestrator is down is saved, and the next push delivers it.
	f.orch.Down = errDown
	_, err = f.svc.SetAppOffered(f.ctx, operator, vllm.ID, true)
	require.NoError(t, err)
	requireKind(t, f.svc.PushPrices(f.ctx), core.Unavailable, "")
	f.orch.Down = nil
	require.NoError(t, f.svc.PushPrices(f.ctx))
	require.Len(t, f.orch.PushedPrices(), 2)
}

func TestRaisingAnAppVersionGivesTheOldOneADeadline(t *testing.T) {
	f := newFixture(t)
	app := f.app("echo")

	updated, err := f.svc.SetAppVersion(f.ctx, operator, app.ID, "2.0.0", nil)
	require.NoError(t, err)
	require.Equal(t, "2.0.0", updated.RequiredVersion)
	require.Equal(t, "1.0.0", updated.PreviousVersion)
	require.Equal(t, f.clock.Now().Add(24*time.Hour), *updated.PreviousVersionDeadline, "the pool's default deadline")

	deadline := f.clock.Now().Add(2 * time.Hour)
	updated, err = f.svc.SetAppVersion(f.ctx, operator, app.ID, "3.0.0", &deadline)
	require.NoError(t, err)
	require.Equal(t, "2.0.0", updated.PreviousVersion)
	require.Equal(t, deadline, *updated.PreviousVersionDeadline)

	_, err = f.svc.SetAppVersion(f.ctx, operator, app.ID, "3.0.0", nil)
	requireKind(t, err, core.Conflict, "")
	_, err = f.svc.SetAppVersion(f.ctx, operator, app.ID, " ", nil)
	requireKind(t, err, core.Invalid, "")
}

func TestTermsAreVersionedPerScope(t *testing.T) {
	f := newFixture(t)
	current, err := f.svc.Terms(f.ctx)
	require.NoError(t, err)
	require.Nil(t, current.Global)
	require.Nil(t, current.Regional)

	first, err := f.svc.PublishTerms(f.ctx, operator, core.TermsGlobal, "Be good.")
	require.NoError(t, err)
	require.Equal(t, 1, first.Version)
	require.Len(t, first.Hash, 64)

	// Publishing the text already in force changes nothing: that is how the
	// admin site brings a region level with the others.
	again, err := f.svc.PublishTerms(f.ctx, operator, core.TermsGlobal, "Be good.")
	require.NoError(t, err)
	require.Equal(t, first, again)

	second, err := f.svc.PublishTerms(f.ctx, operator, core.TermsGlobal, "Be very good.")
	require.NoError(t, err)
	require.Equal(t, 2, second.Version)
	require.NotEqual(t, first.Hash, second.Hash)

	regional, err := f.svc.PublishTerms(f.ctx, operator, core.TermsRegional, "In this region, also be on time.")
	require.NoError(t, err)
	require.Equal(t, 1, regional.Version)

	current, err = f.svc.Terms(f.ctx)
	require.NoError(t, err)
	require.Equal(t, second, current.Global)
	require.Equal(t, regional, current.Regional)

	_, err = f.svc.PublishTerms(f.ctx, operator, "local", "x")
	requireKind(t, err, core.Invalid, "")
	_, err = f.svc.PublishTerms(f.ctx, operator, core.TermsGlobal, "  ")
	requireKind(t, err, core.Invalid, "")
}

func TestSettingsDefaultAndSave(t *testing.T) {
	f := newFixture(t)
	settings, err := f.svc.Settings(f.ctx)
	require.NoError(t, err)
	require.Equal(t, core.DefaultSettings(), settings)
	require.Equal(t, 1500, settings.CommissionBps)
	require.Equal(t, 7, settings.WindowRounds)
	require.Equal(t, core.PayoutApprovalRequired, settings.PayoutApproval)

	settings.CommissionBps, settings.PayoutApproval = 1000, core.PayoutApprovalAutomatic
	require.NoError(t, f.svc.SaveSettings(f.ctx, operator, settings))
	saved, err := f.svc.Settings(f.ctx)
	require.NoError(t, err)
	require.Equal(t, settings, saved)

	for _, bad := range []func(*core.Settings){
		func(s *core.Settings) { s.CommissionBps = 10001 },
		func(s *core.Settings) { s.WindowRounds = 0 },
		func(s *core.Settings) { s.PayoutApproval = "never" },
		func(s *core.Settings) { s.TestIntervalSeconds = 1 },
		func(s *core.Settings) { s.TestFailuresToSuspend = 0 },
	} {
		attempt := saved
		bad(&attempt)
		requireKind(t, f.svc.SaveSettings(f.ctx, operator, attempt), core.Invalid, "")
	}
	unchanged, err := f.svc.Settings(f.ctx)
	require.NoError(t, err)
	require.Equal(t, saved, unchanged)
}
