package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/stretchr/testify/require"

	"github.com/livepeer/go-livepeer/ai/runner"
	"github.com/livepeer/go-livepeer/ai/runner/tunnelclient"
	"github.com/livepeer/go-livepeer/core"
	"github.com/livepeer/go-livepeer/eth"
	"github.com/livepeer/go-livepeer/monitor"
	"github.com/livepeer/go-livepeer/pm"
	"github.com/segmentio/kafka-go"
)

// The pool service (pool/, its own Go module) and this orchestrator share no
// code: they agree on an admission webhook, four CLI-port endpoints and a
// price table. This test runs the real pool service as a process beside a
// real orchestrator and takes one member from application to serving work,
// so a drift in that agreement on either side fails here.

const openPoolTestSecret = "the-pool-secret-shared-by-both"

// openPool is the pool service running as a subprocess.
type openPool struct {
	t   *testing.T
	url string
	log *syncBuffer
}

func buildOpenPool(t *testing.T) string {
	t.Helper()
	poolDir, err := filepath.Abs("../pool")
	require.NoError(t, err)
	if _, err := os.Stat(filepath.Join(poolDir, "go.mod")); err != nil {
		t.Skip("the pool service is not in this checkout")
	}
	bin := filepath.Join(t.TempDir(), "openpool")
	build := exec.Command("go", "build", "-o", bin, "./cmd/openpool")
	build.Dir = poolDir
	out, err := build.CombinedOutput()
	require.NoError(t, err, "building the pool service: %s", out)
	return bin
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	return l.Addr().String()
}

func startOpenPool(t *testing.T, bin, listen, orchURL, cliURL string) *openPool {
	t.Helper()
	p := &openPool{t: t, url: "http://" + listen, log: &syncBuffer{}}
	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(),
		"OPENPOOL_REGION_ID=test-region",
		"OPENPOOL_REGION_NAME=Test Region",
		"OPENPOOL_LISTEN="+listen,
		"OPENPOOL_PUBLIC_URL="+p.url,
		"OPENPOOL_DB_PATH="+filepath.Join(t.TempDir(), "pool.db"),
		"OPENPOOL_ORCHESTRATOR_URL="+orchURL,
		"OPENPOOL_ORCHESTRATOR_CLI_URL="+cliURL,
		"OPENPOOL_POOL_SECRET="+openPoolTestSecret,
		"OPENPOOL_SESSION_SECRET=a-session-signing-secret-of-enough-length",
		"OPENPOOL_ADMIN_EMAIL=ops@open-pool.com",
		"OPENPOOL_SIGN_IN_DOMAINS=app.open-pool.com",
		"OPENPOOL_RECONCILE_INTERVAL=200ms",
		"OPENPOOL_PAYOUT_INTERVAL=200ms",
		"OPENPOOL_SETTLE_DELAY=300ms",
	)
	cmd.Stderr = p.log
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		_ = cmd.Process.Signal(os.Interrupt)
		done := make(chan struct{})
		go func() { _ = cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = cmd.Process.Kill()
		}
	})
	require.Eventually(t, func() bool {
		res, err := http.Get(p.url + "/healthz")
		if err != nil {
			return false
		}
		res.Body.Close()
		return res.StatusCode == http.StatusOK
	}, 20*time.Second, 50*time.Millisecond, "the pool service did not start: %s", p.log.String())
	return p
}

// eventForwarder stands in for Kafka: it is the orchestrator event
// producer's writer, and hands each published event to the pool service's
// upload endpoint, byte for byte as it would have been written to the topic.
type eventForwarder struct {
	url string
}

func (f eventForwarder) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	var body bytes.Buffer
	for _, msg := range msgs {
		body.Write(msg.Value)
		body.WriteByte('\n')
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.url+"/v1/orchestrator/events", &body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+openPoolTestSecret)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("pool service answered %d", res.StatusCode)
	}
	return nil
}

// roundClient is an Ethereum client that is in whatever round the test says.
type roundClient struct {
	*eth.StubClient
	round atomic.Int64
}

func (c *roundClient) CurrentRound() (*big.Int, error) { return big.NewInt(c.round.Load()), nil }

// call sends JSON and decodes the JSON answer into out, requiring the status.
func (p *openPool) call(status int, method, path, token string, body, out any) {
	p.t.Helper()
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		require.NoError(p.t, err)
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, p.url+path, reader)
	require.NoError(p.t, err)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	res, err := http.DefaultClient.Do(req)
	require.NoError(p.t, err)
	defer res.Body.Close()
	data, _ := io.ReadAll(res.Body)
	require.Equal(p.t, status, res.StatusCode, "%s %s: %s", method, path, data)
	if out != nil {
		require.NoError(p.t, json.Unmarshal(data, out), string(data))
	}
}

var openPoolCodePattern = regexp.MustCompile(`code=(\d{6})`)

// signInAdmin signs the operator in. Email is not configured, so the pool
// service writes the sign-in code to its log.
func (p *openPool) signInAdmin() string {
	p.t.Helper()
	p.call(http.StatusNoContent, "POST", "/v1/auth/admin/code", "", map[string]string{"email": "ops@open-pool.com"}, nil)
	var code string
	require.Eventually(p.t, func() bool {
		match := openPoolCodePattern.FindStringSubmatch(p.log.String())
		if match == nil {
			return false
		}
		code = match[1]
		return true
	}, 5*time.Second, 20*time.Millisecond)
	var session struct {
		Token string `json:"token"`
	}
	p.call(http.StatusOK, "POST", "/v1/auth/admin/verify", "", map[string]string{"email": "ops@open-pool.com", "code": code}, &session)
	return session.Token
}

func TestOpenPoolServiceAndOrchestratorAgree(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs the pool service")
	}
	bin := buildOpenPool(t)
	poolAddr := freeAddr(t)

	// A real orchestrator in pool mode, whose admission webhook is the pool service.
	node, err := core.NewLivepeerNode(nil, t.TempDir(), nil)
	require.NoError(t, err)
	node.LiveRunnerPoolSecret = openPoolTestSecret
	chain := &roundClient{StubClient: &eth.StubClient{}}
	chain.round.Store(4100)
	node.Eth = chain
	// The orchestrator's events reach the pool service as they would through Kafka.
	producer := monitor.NewOrchestratorKafkaProducer(eventForwarder{url: "http://" + poolAddr}, "orchestrator-events", 0)
	monitor.SetOrchestratorKafkaProducer(producer)
	producerCtx, stopProducer := context.WithCancel(context.Background())
	go producer.Run(producerCtx)
	t.Cleanup(func() {
		stopProducer()
		monitor.SetOrchestratorKafkaProducer(nil)
	})
	lp := &lphttp{orchestrator: newStubOrchestrator(), node: node, transRPC: http.NewServeMux()}
	admitter, err := runner.NewLiveRunnerWebhookAdmitter("http://"+poolAddr+"/v1/orchestrator/admit", openPoolTestSecret, 5*time.Second)
	require.NoError(t, err)
	manager := runner.NewLiveRunnerRegistry(runner.LiveRunnerRegistryConfig{
		Host: testLiveRunnerHost{orchestrator: lp.orchestrator}, Admitter: admitter, OrchAddress: "0x00000000000000000000000000000000000000AA",
		Events: monitor.SendOrchestratorEvent, Round: func() int64 { return 4100 },
	})
	t.Cleanup(manager.Stop)
	node.LiveRunnerManager = manager
	require.NoError(t, startAIServer(lp))
	orch := httptest.NewServer(lp)
	t.Cleanup(orch.Close)
	cli := httptest.NewServer((&LivepeerServer{LivepeerNode: node}).cliWebServerHandlers("addr"))
	t.Cleanup(cli.Close)

	pool := startOpenPool(t, bin, poolAddr, orch.URL, cli.URL)
	admin := pool.signInAdmin()

	// The operator defines the pool: a GPU, two apps, the terms.
	var gpu struct {
		ID string `json:"id"`
	}
	pool.call(http.StatusCreated, "POST", "/v1/admin/gpus", admin, map[string]any{"name": "NVIDIA RTX 4090", "vram_mb": 24564}, &gpu)
	appDefinition := func(name string) map[string]any {
		return map[string]any{
			"name": name, "mode": "single-shot", "price": map[string]string{"price": "0.50", "unit": "hour"}, "required_version": "1.0.0",
			"container": map[string]any{
				"image": "vllm/vllm-openai@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", "port": 8000,
			},
			"default_capacity": 2, "max_capacity": 4,
		}
	}
	var vllm struct {
		ID string `json:"id"`
	}
	// The app's test: its model list must answer and name the model.
	vllmDefinition := appDefinition("vllm/qwen2.5-7b-instruct")
	vllmDefinition["test"] = map[string]any{"method": "GET", "path": "/v1/models", "expect_status": 200, "expect_contains": []string{"qwen"}, "timeout_ms": 5000}
	pool.call(http.StatusCreated, "POST", "/v1/admin/apps", admin, vllmDefinition, &vllm)
	pool.call(http.StatusCreated, "POST", "/v1/admin/apps", admin, appDefinition("flux/schnell"), nil)
	pool.call(http.StatusOK, "POST", "/v1/admin/terms", admin, map[string]string{"scope": "global", "body": "Be good."}, nil)

	// The catalog's prices are now the orchestrator's price table.
	require.Eventually(t, func() bool { return len(manager.Prices().Apps) == 2 }, 5*time.Second, 20*time.Millisecond)
	require.Equal(t, runner.LiveRunnerPriceInfo{Price: "0.50", Currency: "usd", Unit: "hour"}, manager.Prices().Apps["vllm/qwen2.5-7b-instruct"])

	// A member signs in with a wallet, applies, and is approved with one app assigned.
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	address := crypto.PubkeyToAddress(key.PublicKey).Hex()
	sign := func(message string) string {
		sig, err := crypto.Sign(accounts.TextHash([]byte(message)), key)
		require.NoError(t, err)
		sig[crypto.RecoveryIDOffset] += 27
		return hexutil.Encode(sig)
	}
	var challenge struct {
		Nonce string `json:"nonce"`
	}
	pool.call(http.StatusOK, "POST", "/v1/auth/wallet/nonce", "", nil, &challenge)
	signIn := fmt.Sprintf("app.open-pool.com wants you to sign in with your Ethereum account:\n%s\n\nSign in to Open Pool.\n\n"+
		"URI: https://app.open-pool.com\nVersion: 1\nChain ID: 42161\nNonce: %s\nIssued At: %s",
		address, challenge.Nonce, time.Now().UTC().Format(time.RFC3339))
	var session struct {
		Token string `json:"token"`
	}
	pool.call(http.StatusOK, "POST", "/v1/auth/wallet/verify", "", map[string]string{"message": signIn, "signature": sign(signIn)}, &session)
	member := session.Token

	draft := map[string]any{
		"display_name": "Alice",
		"machines":     []map[string]any{{"label": "box-1", "gpus": []map[string]any{{"gpu_id": gpu.ID, "count": 2}}}},
	}
	var toSign struct {
		Message string `json:"message"`
	}
	pool.call(http.StatusOK, "POST", "/v1/applications/message", member, draft, &toSign)
	draft["signature"] = sign(toSign.Message)
	var application struct {
		ID string `json:"id"`
	}
	pool.call(http.StatusCreated, "POST", "/v1/applications", member, draft, &application)
	var approved struct {
		ID string `json:"id"`
	}
	pool.call(http.StatusOK, "POST", "/v1/admin/applications/"+application.ID+"/approve", admin, map[string]any{
		"machines": []map[string]any{{"machine": 0, "assignments": []map[string]any{{"app_id": vllm.ID}}}},
	}, &approved)

	var machines struct {
		Machines []struct {
			ID string `json:"id"`
		} `json:"machines"`
	}
	pool.call(http.StatusOK, "GET", "/v1/me/machines", member, nil, &machines)
	require.Len(t, machines.Machines, 1)
	var credential struct {
		Secret string `json:"secret"`
	}
	pool.call(http.StatusCreated, "POST", "/v1/me/machines/"+machines.Machines[0].ID+"/credential", member, nil, &credential)

	// The member's machine starts the runner command with its credential. The
	// app behind it answers well until it is broken.
	var broken atomic.Bool
	var sawProbeSecret atomic.Bool
	startRunner := func(app string, log io.Writer) *tunnelclient.Client {
		target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Livepeer-Probe-Secret") != "" {
				sawProbeSecret.Store(true)
			}
			if broken.Load() {
				http.Error(w, "CUDA out of memory", http.StatusInternalServerError)
				return
			}
			_, _ = io.WriteString(w, `{"data":[{"id":"qwen2.5-7b-instruct"}]}`)
		}))
		t.Cleanup(target.Close)
		client, err := tunnelclient.New(tunnelclient.Config{
			Orchestrator: orch.URL, Credential: credential.Secret, Target: target.URL,
			App: app, Mode: "single-shot", Version: "1.0.0", Capacity: 2,
			Logger: slog.New(slog.NewTextHandler(log, nil)),
		})
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { _ = client.Run(ctx); close(done) }()
		t.Cleanup(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
			}
		})
		return client
	}
	client := startRunner("vllm/qwen2.5-7b-instruct", io.Discard)
	runnerID := waitPoolRunnerConnected(t, lp, client)

	// The orchestrator admitted it under the member the pool service named.
	listed := manager.PoolRunners()
	require.Len(t, listed, 1)
	require.Equal(t, approved.ID, listed[0].MemberID)
	require.Equal(t, address, listed[0].PayoutAddress)
	state := func() string {
		runners := manager.PoolRunners()
		if len(runners) != 1 {
			return ""
		}
		return runners[0].State
	}

	// Nobody promotes it by hand: the pool service tests it through the
	// probe route, it passes, and it goes from probation to active.
	require.Eventually(t, func() bool { return state() == runner.LiveRunnerStateActive }, 15*time.Second, 50*time.Millisecond)
	require.False(t, sawProbeSecret.Load(), "the runner never sees the pool secret")
	require.Len(t, discoveredRunnerURLs(t, orch.URL), 1)
	res, err := http.Get(orch.URL + "/apps/" + runnerID + "/app/v1/models")
	require.NoError(t, err)
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()
	require.Equal(t, http.StatusOK, res.StatusCode)
	require.Contains(t, string(body), "qwen2.5-7b-instruct")

	// The pool service sees the same runner through the CLI port, traced to its machine.
	var runners struct {
		Runners []struct {
			RunnerID   string `json:"runner_id"`
			MachineID  string `json:"machine_id"`
			LastTestOK bool   `json:"last_test_ok"`
		} `json:"runners"`
	}
	pool.call(http.StatusOK, "GET", "/v1/admin/runners", admin, nil, &runners)
	require.Len(t, runners.Runners, 1)
	require.Equal(t, runnerID, runners.Runners[0].RunnerID)
	require.Equal(t, machines.Machines[0].ID, runners.Runners[0].MachineID)
	require.True(t, runners.Runners[0].LastTestOK)

	// A customer pays for a session on it. The orchestrator's events about
	// that, and about a ticket it redeemed, become the member's earnings.
	const payer = "0x00000000000000000000000000000000000000c1"
	sessionID, _, err := manager.ReservePaidSession(runnerID, "manifest-1", payer)
	require.NoError(t, err)
	require.NoError(t, manager.RecordSessionFee(runnerID, sessionID, payer, 10, big.NewRat(600, 1), big.NewRat(400, 1)))
	require.NoError(t, manager.RecordSessionFee(runnerID, sessionID, payer, 10, big.NewRat(400, 1), big.NewRat(0, 1)))
	require.NoError(t, manager.ReleaseSessionWithReason(runnerID, sessionID, runner.LiveRunnerReleaseCompleted))
	require.NoError(t, monitor.SendOrchestratorEvent(pm.TicketRedeemedEvent, "0x00000000000000000000000000000000000000AA", map[string]interface{}{
		"orch_address": "0x00000000000000000000000000000000000000AA", "sender": payer, "face_value_wei": "2000000000000000000",
		"tx_hash": "0x01", "round": int64(4101), "creation_round": int64(4100),
	}))
	var earnings struct {
		Earnings []struct {
			Round    int64  `json:"round"`
			App      string `json:"app"`
			FeeWei   string `json:"fee_wei"`
			Sessions int64  `json:"sessions"`
		} `json:"earnings"`
	}
	require.Eventually(t, func() bool {
		pool.call(http.StatusOK, "GET", "/v1/me/earnings", member, nil, &earnings)
		return len(earnings.Earnings) == 1 && earnings.Earnings[0].FeeWei == "1000"
	}, 15*time.Second, 100*time.Millisecond)
	require.Equal(t, int64(4100), earnings.Earnings[0].Round, "booked in the round the orchestrator stamped")
	require.Equal(t, "vllm/qwen2.5-7b-instruct", earnings.Earnings[0].App)
	require.Equal(t, int64(1), earnings.Earnings[0].Sessions, "the pool's own tests are not counted as work")

	// The window's seven rounds pass. The pool service closes it and works
	// out the payment: the 2 ETH redeemed, less the 15% commission, all to
	// the one member who earned fees.
	chain.round.Store(4107)
	var batch struct {
		Status     string `json:"status"`
		PayableWei string `json:"payable_wei"`
		Items      []struct {
			Address   string `json:"address"`
			AmountWei string `json:"amount_wei"`
		} `json:"items"`
	}
	require.Eventually(t, func() bool {
		var batches struct {
			Batches []struct {
				ID     string `json:"id"`
				Status string `json:"status"`
			} `json:"batches"`
		}
		pool.call(http.StatusOK, "GET", "/v1/admin/batches", admin, nil, &batches)
		for _, b := range batches.Batches {
			if b.ID == "batch_4100" && b.Status == "ready" {
				return true
			}
		}
		return false
	}, 15*time.Second, 100*time.Millisecond)
	pool.call(http.StatusOK, "GET", "/v1/admin/batches/batch_4100", admin, nil, &batch)
	require.Equal(t, "1700000000000000000", batch.PayableWei)
	require.Len(t, batch.Items, 1)
	require.Equal(t, address, batch.Items[0].Address)
	require.Equal(t, "1700000000000000000", batch.Items[0].AmountWei)
	pool.call(http.StatusOK, "POST", "/v1/admin/batches/batch_4100/approve", admin, nil, &batch)
	require.Equal(t, "approved", batch.Status)

	// The app breaks. Three failed tests in a row suspend the runner, and the
	// member can read why.
	broken.Store(true)
	var result struct {
		OK     bool   `json:"ok"`
		Detail string `json:"detail"`
	}
	for i := 0; i < 3; i++ {
		pool.call(http.StatusOK, "POST", "/v1/admin/runners/"+runnerID+"/test", admin, nil, &result)
		require.False(t, result.OK)
	}
	require.Equal(t, "answered 500, expected 200: CUDA out of memory", result.Detail)
	require.Equal(t, runner.LiveRunnerStateSuspended, state())
	require.Empty(t, discoveredRunnerURLs(t, orch.URL))
	var tests struct {
		Tests []struct {
			OK bool `json:"ok"`
		} `json:"tests"`
	}
	pool.call(http.StatusOK, "GET", "/v1/me/tests", member, nil, &tests)
	require.Len(t, tests.Tests, 4)

	// It is fixed. Three passes bring it back without anyone stepping in.
	broken.Store(false)
	for i := 0; i < 3; i++ {
		pool.call(http.StatusOK, "POST", "/v1/admin/runners/"+runnerID+"/test", admin, nil, &result)
		require.True(t, result.OK)
	}
	require.Equal(t, runner.LiveRunnerStateActive, state())

	// The same credential cannot serve an app the machine was not assigned;
	// the runner command's log says why.
	refusedLog := &syncBuffer{}
	startRunner("flux/schnell", refusedLog)
	require.Eventually(t, func() bool {
		return bytes.Contains([]byte(refusedLog.String()), []byte("403 this machine is not assigned flux/schnell"))
	}, 15*time.Second, 50*time.Millisecond, refusedLog.String())
	require.Len(t, manager.PoolRunners(), 1)

	// The operator unassigns the app: the runner is drained and, being idle,
	// removed; the machine is refused when it tries to come back.
	pool.call(http.StatusOK, "PUT", "/v1/admin/machines/"+machines.Machines[0].ID+"/assignments", admin, map[string]any{"assignments": []any{}}, nil)
	require.Eventually(t, func() bool { return len(manager.PoolRunners()) == 0 }, 15*time.Second, 50*time.Millisecond)
	time.Sleep(3 * time.Second) // the runner command retries after a second or two
	require.Empty(t, manager.PoolRunners())

	// A ban is accepted whether or not the member has runners registered.
	pool.call(http.StatusOK, "POST", "/v1/admin/members/"+approved.ID+"/ban", admin, map[string]string{"reason": "contract test"}, nil)
}
