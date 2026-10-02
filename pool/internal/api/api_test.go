package api_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Livepeer-Open-Pool/pool/internal/api"
	"github.com/Livepeer-Open-Pool/pool/internal/auth"
	"github.com/Livepeer-Open-Pool/pool/internal/core"
	"github.com/Livepeer-Open-Pool/pool/internal/testkit"
)

const (
	poolSecret    = "the-pool-secret"
	sessionSecret = "a-session-signing-secret-of-enough-length"
	adminEmail    = "ops@open-pool.com"
	appOrigin     = "https://app.open-pool.com"
)

// env is a pool service served over HTTP, as the sites and the orchestrator reach it.
type env struct {
	t     *testing.T
	url   string
	orch  *testkit.Orchestrator
	mail  *testkit.Mailbox
	clock *testkit.Clock
}

func newEnv(t *testing.T) *env {
	t.Helper()
	e := &env{t: t, orch: &testkit.Orchestrator{}, mail: &testkit.Mailbox{}, clock: testkit.NewClock()}
	db := testkit.OpenDB(t)
	tokens, err := auth.NewTokens(sessionSecret, 12*time.Hour, e.clock.Now)
	require.NoError(t, err)
	service := core.New(db, e.orch, e.mail, core.Config{
		RegionID: "us-east", RegionName: "US East",
		PublicURL:       "https://us-east.api.open-pool.com",
		OrchestratorURL: "https://orch.us-east.open-pool.com:8935",
		AdminURL:        "https://admin.open-pool.com",
		RecoverSigner:   auth.RecoverSigner,
		Now:             e.clock.Now,
	}, nil)
	server := api.New(api.Options{
		Core:           service,
		Tokens:         tokens,
		Wallet:         auth.NewWallet(db, tokens, []string{"app.open-pool.com"}, 42161, e.clock.Now),
		Admin:          auth.NewAdmin(db, tokens, e.mail, adminEmail, e.clock.Now),
		PoolSecret:     poolSecret,
		AllowedOrigins: []string{"https://open-pool.com", appOrigin, "https://admin.open-pool.com"},
		SignIn:         api.SignInInfo{Domains: []string{"app.open-pool.com"}, ChainID: 42161},
	})
	srv := httptest.NewServer(server.Handler())
	t.Cleanup(srv.Close)
	e.url = srv.URL
	return e
}

// call sends a JSON request and decodes a JSON answer into out when given.
func (e *env) call(method, path, token string, body, out any) int {
	e.t.Helper()
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		require.NoError(e.t, err)
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, e.url+path, reader)
	require.NoError(e.t, err)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	res, err := http.DefaultClient.Do(req)
	require.NoError(e.t, err)
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	require.NoError(e.t, err)
	if out != nil && len(data) > 0 {
		require.NoError(e.t, json.Unmarshal(data, out), string(data))
	}
	return res.StatusCode
}

// ok is call that requires the status.
func (e *env) ok(status int, method, path, token string, body, out any) {
	e.t.Helper()
	var failure struct {
		Error string `json:"error"`
	}
	target := out
	if target == nil {
		target = &failure
	}
	got := e.call(method, path, token, body, target)
	require.Equal(e.t, status, got, "%s %s: %s", method, path, failure.Error)
}

// refused is call that requires the status and returns the error text.
func (e *env) refused(status int, method, path, token string, body any) string {
	e.t.Helper()
	var failure struct {
		Error string `json:"error"`
	}
	require.Equal(e.t, status, e.call(method, path, token, body, &failure), "%s %s", method, path)
	return failure.Error
}

type session struct {
	Token   string `json:"token"`
	Role    string `json:"role"`
	Subject string `json:"subject"`
}

func (e *env) signInWallet(wallet *testkit.Wallet) string {
	e.t.Helper()
	var challenge struct {
		Nonce string `json:"nonce"`
	}
	e.ok(http.StatusOK, "POST", "/v1/auth/wallet/nonce", "", nil, &challenge)
	message := wallet.SignInMessage("app.open-pool.com", 42161, challenge.Nonce, e.clock.Now())
	var s session
	e.ok(http.StatusOK, "POST", "/v1/auth/wallet/verify", "", map[string]string{"message": message, "signature": wallet.Sign(e.t, message)}, &s)
	require.Equal(e.t, wallet.Address, s.Subject)
	return s.Token
}

func (e *env) signInAdmin() string {
	e.t.Helper()
	e.clock.Advance(time.Minute) // codes are sent at most once a minute
	e.ok(http.StatusNoContent, "POST", "/v1/auth/admin/code", "", map[string]string{"email": adminEmail}, nil)
	var s session
	e.ok(http.StatusOK, "POST", "/v1/auth/admin/verify", "", map[string]string{"email": adminEmail, "code": e.mail.LastCode()}, &s)
	require.Equal(e.t, "admin", s.Role)
	return s.Token
}

// catalog has the operator set the pool up: a GPU, two apps, the terms.
func (e *env) catalog(admin string) (gpu core.GPU, vllm, flux core.App) {
	e.t.Helper()
	e.ok(http.StatusCreated, "POST", "/v1/admin/gpus", admin, core.GPUInput{Name: "NVIDIA RTX 4090", VRAMMB: 24564}, &gpu)
	definition := func(name string) core.AppInput {
		return core.AppInput{
			Name: name, Mode: core.ModeSingleShot, Price: core.Price{Amount: "0.50", Unit: "hour"}, RequiredVersion: "1.0.0",
			Container:       core.Container{Image: testkit.PinnedImage, Port: 8000, HealthPath: "/health"},
			DefaultCapacity: 2, MaxCapacity: 4, Description: "An app.",
		}
	}
	e.ok(http.StatusCreated, "POST", "/v1/admin/apps", admin, definition("vllm/qwen2.5-7b-instruct"), &vllm)
	e.ok(http.StatusCreated, "POST", "/v1/admin/apps", admin, definition("flux/schnell"), &flux)
	e.ok(http.StatusOK, "POST", "/v1/admin/terms", admin, map[string]string{"scope": "global", "body": "Be good."}, nil)
	return gpu, vllm, flux
}

// admit calls the admission webhook as the orchestrator does.
func (e *env) admit(secret, credential, runnerID, app string) (int, core.AdmissionResponse) {
	e.t.Helper()
	var resp core.AdmissionResponse
	status := e.call("POST", "/v1/orchestrator/admit", secret, core.AdmissionRequest{
		Event: "register", OrchAddress: "0xAA", RemoteAddr: "203.0.113.7", Credential: credential,
		Runner: core.AdmissionRunner{RunnerID: runnerID, Tunnel: true, Version: "1.0.0", Status: "ready", Mode: core.ModeSingleShot, App: app, Capacity: 2},
	}, &resp)
	return status, resp
}

func TestAMemberJoinsAndTheirMachineIsAdmittedForItsAssignedAppOnly(t *testing.T) {
	e := newEnv(t)
	admin := e.signInAdmin()
	gpu, vllm, _ := e.catalog(admin)

	// Anyone can see what the pool offers and its terms.
	var pool struct {
		Region             struct{ ID, Name string }
		TakingApplications bool `json:"taking_applications"`
		AppsOffered        int  `json:"apps_offered"`
	}
	e.ok(http.StatusOK, "GET", "/v1/public/pool", "", nil, &pool)
	require.Equal(t, "us-east", pool.Region.ID)
	require.True(t, pool.TakingApplications)
	require.Equal(t, 2, pool.AppsOffered)
	var apps struct {
		Apps []map[string]any `json:"apps"`
	}
	e.ok(http.StatusOK, "GET", "/v1/public/apps", "", nil, &apps)
	require.Len(t, apps.Apps, 2)
	require.NotContains(t, apps.Apps[0], "container", "how an app is run and tested is not public")
	require.NotContains(t, apps.Apps[0], "test")

	// Alice signs in with her wallet and applies.
	alice := testkit.NewWallet(t)
	token := e.signInWallet(alice)
	draft := map[string]any{
		"display_name": "Alice",
		"machines":     []map[string]any{{"label": "box-1", "gpus": []map[string]any{{"gpu_id": gpu.ID, "count": 2}}}},
	}
	var toSign struct {
		Message string `json:"message"`
	}
	e.ok(http.StatusOK, "POST", "/v1/applications/message", token, draft, &toSign)
	draft["signature"] = alice.Sign(t, toSign.Message)
	var application core.Application
	e.ok(http.StatusCreated, "POST", "/v1/applications", token, draft, &application)
	require.Equal(t, core.ApplicationPending, application.Status)

	var me struct {
		Address      string             `json:"address"`
		Member       *core.Member       `json:"member"`
		Applications []core.Application `json:"applications"`
	}
	e.ok(http.StatusOK, "GET", "/v1/me", token, nil, &me)
	require.Equal(t, alice.Address, me.Address)
	require.Nil(t, me.Member)
	require.Len(t, me.Applications, 1)
	// Not a member yet: the member pages are closed to her.
	require.Equal(t, "this address is not a member in this region", e.refused(http.StatusForbidden, "GET", "/v1/me/machines", token, nil))

	// The operator sees it waiting and approves it, assigning the machine one app.
	var status struct {
		PendingApplications int `json:"pending_applications"`
	}
	e.ok(http.StatusOK, "GET", "/v1/admin/status", admin, nil, &status)
	require.Equal(t, 1, status.PendingApplications)
	var member core.Member
	e.ok(http.StatusOK, "POST", "/v1/admin/applications/"+application.ID+"/approve", admin, map[string]any{
		"machines": []map[string]any{{"machine": 0, "assignments": []map[string]any{{"app_id": vllm.ID}}}},
	}, &member)
	require.Equal(t, core.MemberActive, member.Status)

	// Alice sees her machine and creates its credential, shown once.
	var machines struct {
		Machines []core.Machine `json:"machines"`
	}
	e.ok(http.StatusOK, "GET", "/v1/me/machines", token, nil, &machines)
	require.Len(t, machines.Machines, 1)
	machine := machines.Machines[0]
	require.Equal(t, "vllm/qwen2.5-7b-instruct", machine.Assignments[0].AppName)
	var credential core.NewCredential
	e.ok(http.StatusCreated, "POST", "/v1/me/machines/"+machine.ID+"/credential", token, nil, &credential)
	require.NotEmpty(t, credential.Secret)
	require.Len(t, credential.Commands, 1)
	require.Contains(t, credential.Commands[0], "-app vllm/qwen2.5-7b-instruct")

	// The orchestrator asks whether her runner may register.
	code, resp := e.admit(poolSecret, credential.Secret, "runner_aaaa", "vllm/qwen2.5-7b-instruct")
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, core.AdmissionResponse{
		Allow: true, MemberID: member.ID, PayoutAddress: alice.Address, MaxRunners: 1, State: core.StateProbation,
	}, resp)

	// For an app the machine was not assigned, the answer is no, with the reason.
	code, resp = e.admit(poolSecret, credential.Secret, "runner_bbbb", "flux/schnell")
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, core.AdmissionResponse{Allow: false, Reason: "this machine is not assigned flux/schnell"}, resp)
	_, resp = e.admit(poolSecret, "opc_made_up", "runner_cccc", "vllm/qwen2.5-7b-instruct")
	require.Equal(t, core.AdmissionResponse{Allow: false, Reason: "unknown credential"}, resp)

	// Only the orchestrator holding the pool secret is answered at all.
	code, _ = e.admit("", credential.Secret, "runner_dddd", "vllm/qwen2.5-7b-instruct")
	require.Equal(t, http.StatusUnauthorized, code)
	code, _ = e.admit("wrong-secret", credential.Secret, "runner_dddd", "vllm/qwen2.5-7b-instruct")
	require.Equal(t, http.StatusUnauthorized, code)

	// Once the orchestrator lists the runner, member and operator both see it.
	e.orch.Register(core.OrchRunner{RunnerID: "runner_aaaa", MemberID: member.ID, App: "vllm/qwen2.5-7b-instruct", State: core.StateProbation})
	var runners struct {
		Runners []core.Runner `json:"runners"`
	}
	e.ok(http.StatusOK, "GET", "/v1/me/runners", token, nil, &runners)
	require.Len(t, runners.Runners, 1)
	require.Equal(t, machine.ID, runners.Runners[0].MachineID)
	e.ok(http.StatusNoContent, "POST", "/v1/admin/runners/runner_aaaa/state", admin, map[string]string{"state": "active", "reason": "checked"}, nil)
	e.ok(http.StatusOK, "GET", "/v1/admin/runners", admin, nil, &runners)
	require.Equal(t, core.StateActive, runners.Runners[0].State)

	// The prices the operator set are the orchestrator's.
	require.Equal(t, core.Price{Amount: "0.50", Unit: "hour"}, e.orch.PushedPrices()["vllm/qwen2.5-7b-instruct"])
}

func TestEachKindOfCallerOnlyReachesItsOwnEndpoints(t *testing.T) {
	e := newEnv(t)
	admin := e.signInAdmin()
	wallet := e.signInWallet(testkit.NewWallet(t))

	for _, path := range []string{"/v1/admin/members", "/v1/admin/settings", "/v1/admin/audit", "/v1/admin/runners"} {
		require.Equal(t, "not signed in", e.refused(http.StatusUnauthorized, "GET", path, "", nil), path)
		e.refused(http.StatusUnauthorized, "GET", path, wallet, nil)
		e.refused(http.StatusUnauthorized, "GET", path, "not-a-token", nil)
	}
	// The operator's session is not a member's.
	e.refused(http.StatusUnauthorized, "GET", "/v1/me", admin, nil)
	e.refused(http.StatusUnauthorized, "GET", "/v1/me", "", nil)
	e.refused(http.StatusUnauthorized, "POST", "/v1/applications", "", map[string]string{})

	// A session ends when it expires.
	e.ok(http.StatusOK, "GET", "/v1/me", wallet, nil, nil)
	e.clock.Advance(12 * time.Hour)
	e.refused(http.StatusUnauthorized, "GET", "/v1/me", wallet, nil)
}

func TestSignInFailuresSayNothingUseful(t *testing.T) {
	e := newEnv(t)

	// Asking for a code never reveals whether the address is the admin's.
	e.ok(http.StatusNoContent, "POST", "/v1/auth/admin/code", "", map[string]string{"email": "someone@example.com"}, nil)
	require.Zero(t, e.mail.Codes())
	e.ok(http.StatusNoContent, "POST", "/v1/auth/admin/code", "", map[string]string{"email": adminEmail}, nil)
	require.Equal(t, 1, e.mail.Codes())
	e.refused(http.StatusTooManyRequests, "POST", "/v1/auth/admin/code", "", map[string]string{"email": adminEmail})

	require.Equal(t, "not signed in", e.refused(http.StatusUnauthorized, "POST", "/v1/auth/admin/verify", "", map[string]string{"email": adminEmail, "code": "nope"}))
	require.Equal(t, "not signed in", e.refused(http.StatusUnauthorized, "POST", "/v1/auth/wallet/verify", "", map[string]string{"message": "hello", "signature": "0x00"}))
	e.refused(http.StatusBadRequest, "POST", "/v1/auth/wallet/verify", "", "not an object")
}

func TestErrorsCarryAStatusAndAReason(t *testing.T) {
	e := newEnv(t)
	admin := e.signInAdmin()

	require.Equal(t, "GPU memory must be positive", e.refused(http.StatusBadRequest, "POST", "/v1/admin/gpus", admin, core.GPUInput{Name: "x"}))
	require.Equal(t, "member mbr_missing does not exist", e.refused(http.StatusNotFound, "GET", "/v1/admin/members/mbr_missing", admin, nil))
	e.ok(http.StatusCreated, "POST", "/v1/admin/gpus", admin, core.GPUInput{Name: "NVIDIA RTX 4090", VRAMMB: 1}, nil)
	e.refused(http.StatusConflict, "POST", "/v1/admin/gpus", admin, core.GPUInput{Name: "NVIDIA RTX 4090", VRAMMB: 1})

	// The orchestrator being down is the pool service's problem to report, not hide.
	e.orch.Down = io.ErrUnexpectedEOF
	require.Contains(t, e.refused(http.StatusServiceUnavailable, "GET", "/v1/admin/runners", admin, nil), "the orchestrator did not answer")
	var status struct {
		Orchestrator struct {
			Reachable bool   `json:"reachable"`
			Error     string `json:"error"`
		} `json:"orchestrator"`
	}
	e.ok(http.StatusOK, "GET", "/v1/admin/status", admin, nil, &status)
	require.False(t, status.Orchestrator.Reachable)
	require.NotEmpty(t, status.Orchestrator.Error)
}

func TestOperatorManagesSettingsAndTerms(t *testing.T) {
	e := newEnv(t)
	admin := e.signInAdmin()

	var settings core.Settings
	e.ok(http.StatusOK, "GET", "/v1/admin/settings", admin, nil, &settings)
	require.Equal(t, core.DefaultSettings(), settings)
	// A partial body changes only what it names.
	e.ok(http.StatusOK, "PUT", "/v1/admin/settings", admin, map[string]any{"commission_bps": 2000}, &settings)
	require.Equal(t, 2000, settings.CommissionBps)
	require.Equal(t, 7, settings.WindowRounds)
	e.refused(http.StatusBadRequest, "PUT", "/v1/admin/settings", admin, map[string]any{"commission_bps": 20000})

	var terms core.CurrentTerms
	e.ok(http.StatusOK, "GET", "/v1/public/terms", "", nil, &terms)
	require.Nil(t, terms.Global)
	e.ok(http.StatusOK, "POST", "/v1/admin/terms", admin, map[string]string{"scope": "global", "body": "Be good."}, nil)
	e.ok(http.StatusOK, "POST", "/v1/admin/terms", admin, map[string]string{"scope": "regional", "body": "Be on time."}, nil)
	e.ok(http.StatusOK, "GET", "/v1/public/terms", "", nil, &terms)
	require.Equal(t, "Be good.", terms.Global.Body)
	require.Equal(t, "Be on time.", terms.Regional.Body)

	var audit struct {
		Entries []core.AuditEntry `json:"entries"`
	}
	e.ok(http.StatusOK, "GET", "/v1/admin/audit", admin, nil, &audit)
	require.Equal(t, "admin:"+adminEmail, audit.Entries[0].Actor)
	require.Equal(t, "terms.publish", audit.Entries[0].Action)
}

func TestBrowsersOnTheSitesMayCallTheAPI(t *testing.T) {
	e := newEnv(t)
	preflight := func(origin string) *http.Response {
		req, err := http.NewRequest(http.MethodOptions, e.url+"/v1/me", nil)
		require.NoError(t, err)
		req.Header.Set("Origin", origin)
		req.Header.Set("Access-Control-Request-Method", "GET")
		req.Header.Set("Access-Control-Request-Headers", "authorization")
		res, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		res.Body.Close()
		return res
	}

	res := preflight(appOrigin)
	require.Equal(t, http.StatusNoContent, res.StatusCode)
	require.Equal(t, appOrigin, res.Header.Get("Access-Control-Allow-Origin"))
	require.Contains(t, res.Header.Get("Access-Control-Allow-Headers"), "Authorization")
	require.True(t, strings.Contains(res.Header.Get("Access-Control-Allow-Methods"), "DELETE"))

	// A page on any other site gets no permission.
	res = preflight("https://evil.example")
	require.Empty(t, res.Header.Get("Access-Control-Allow-Origin"))

	req, err := http.NewRequest(http.MethodGet, e.url+"/v1/public/pool", nil)
	require.NoError(t, err)
	req.Header.Set("Origin", "https://open-pool.com")
	res, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	res.Body.Close()
	require.Equal(t, "https://open-pool.com", res.Header.Get("Access-Control-Allow-Origin"))

	res, err = http.Get(e.url + "/healthz")
	require.NoError(t, err)
	res.Body.Close()
	require.Equal(t, http.StatusOK, res.StatusCode)
}
