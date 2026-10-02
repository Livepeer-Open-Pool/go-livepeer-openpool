package e2e

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/accounts/keystore"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/stretchr/testify/require"

	"github.com/livepeer/go-livepeer/cmd/devtool/devtool"
)

// TestOpenPool runs a whole pool on this machine and takes one member from
// signing up to being paid:
//
//	a Livepeer devnet           the protocol's contracts, and a mock ETH/USD price feed
//	an orchestrator             a livepeer process, on-chain, in pool mode
//	Kafka                       the orchestrator's events, read by the pool service
//	the pool service            pool/cmd/openpool, with a funded payout wallet
//	a member's machine          the agent's real image, which starts the app and its tunnel with Docker
//	a customer                  a livepeer remote signer with a deposit, paying with real tickets
//
// Nothing in it is stubbed: the member signs with a real key, the machine
// registers through the real tunnel, the pool's test of it goes through the
// orchestrator, the customer's ticket is redeemed on the chain, and the
// payout is a transaction the member's balance shows.
//
// It needs Docker and takes several minutes, so it runs only when asked to:
//
//	pool/e2e.sh        (or: OPENPOOL_E2E=1 go test ./test/e2e -run TestOpenPool -v -timeout 30m)
func TestOpenPool(t *testing.T) {
	if os.Getenv("OPENPOOL_E2E") == "" {
		t.Skip("set OPENPOOL_E2E=1, or run pool/e2e.sh, to run a whole pool on this machine")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h := newHarness(t)
	host := localIP()
	require.NotEqual(t, "127.0.0.1", host, "containers reach the orchestrator and the pool service at this machine's own address, which must not be loopback")

	// What everything listens on.
	const (
		orchPort, orchCLIPort     = 18935, 17935
		signerPort, signerCLIPort = 18936, 17936
		poolPort, kafkaPort       = 18090, 19092
		registryPort              = 15000
		poolSecret                = "the-pool-secret-of-the-end-to-end-pool"
		adminEmail                = "ops@open-pool.test"
		topic                     = "openpool-e2e"
		appName                   = "echo"
	)
	orchURL := fmt.Sprintf("https://%s:%d", host, orchPort)
	orchCLI := fmt.Sprintf("http://127.0.0.1:%d", orchCLIPort)
	signerURL := fmt.Sprintf("http://127.0.0.1:%d", signerPort)
	signerCLI := fmt.Sprintf("http://127.0.0.1:%d", signerCLIPort)
	poolURL := fmt.Sprintf("http://%s:%d", host, poolPort)

	// The images a member's machine runs, built from this repository.
	h.step("building the pool service, livepeer, and the agent and tunnel images")
	root := h.repoRoot()
	poolBin := filepath.Join(h.dir, "openpool")
	h.run(filepath.Join(root, "pool"), "go", "build", "-o", poolBin, "./cmd/openpool")
	livepeerBin := filepath.Join(h.dir, "livepeer")
	h.run(root, "go", "build", "-o", livepeerBin, "./cmd/livepeer")
	h.docker("build", "-q", "-f", filepath.Join(root, "pool/docker/Dockerfile.agent"), "--build-arg", "VERSION=e2e", "-t", "openpool-agent:e2e", filepath.Join(root, "pool"))
	h.docker("build", "-q", "-f", filepath.Join(root, "docker/Dockerfile.runner-tunnel"), "-t", "openpool-tunnel:e2e", root)

	// An image is named by digest in what a machine is told to run, and a
	// digest comes from a registry: the tunnel is pushed to one started here.
	h.container("openpool-e2e-registry", "-p", fmt.Sprintf("127.0.0.1:%d:5000", registryPort), "registry:2")
	tunnelRepo := fmt.Sprintf("localhost:%d/livepeer-runner-tunnel", registryPort)
	h.docker("tag", "openpool-tunnel:e2e", tunnelRepo+":e2e")
	h.eventually("the registry to take the tunnel image", 30*time.Second, func() bool {
		return exec.Command("docker", "push", "-q", tunnelRepo+":e2e").Run() == nil
	})
	tunnelImage := h.digestOf(tunnelRepo+":e2e", tunnelRepo)
	h.docker("pull", "-q", "mendhak/http-https-echo:latest")
	echoImage := h.digestOf("mendhak/http-https-echo:latest", "mendhak/http-https-echo")

	h.step("starting Kafka and the Livepeer devnet")
	h.container("openpool-e2e-kafka", "-p", fmt.Sprintf("127.0.0.1:%d:%d", kafkaPort, kafkaPort), "redpandadata/redpanda:latest",
		"redpanda", "start", "--mode", "dev-container", "--smp", "1",
		"--kafka-addr", fmt.Sprintf("PLAINTEXT://0.0.0.0:%d", kafkaPort), "--advertise-kafka-addr", fmt.Sprintf("PLAINTEXT://127.0.0.1:%d", kafkaPort))
	h.eventually("Kafka to take the topic", time.Minute, func() bool {
		return exec.Command("docker", "exec", "openpool-e2e-kafka", "rpk", "topic", "create", topic, "--brokers", fmt.Sprintf("127.0.0.1:%d", kafkaPort)).Run() == nil
	})
	geth := setupGeth(t, ctx)
	t.Cleanup(func() { terminateGeth(t, context.Background(), geth) })
	chain := h.dialChain(geth.URI)
	priceFeed := chain.deploy(mockPriceFeedCreation(3000))
	h.logf("mock ETH/USD price feed at %s: one ETH is 3000 dollars", priceFeed.Hex())

	// Three accounts on the chain: the orchestrator's, the customer's, and
	// the pool's payout wallet. The member's is never funded: what it holds
	// at the end is what the pool paid it.
	orch := h.livepeerAccount(geth.URI, "orchestrator", true)
	signer := h.livepeerAccount(geth.URI, "signer", false)
	payoutKeystore, payoutAddress := h.newKeystore("payout")
	chain.send(payoutAddress, ether(5))
	member, err := crypto.GenerateKey()
	require.NoError(t, err)
	memberAddress := crypto.PubkeyToAddress(member.PublicKey)
	require.Zero(t, chain.balance(memberAddress).Sign())

	// A ticket all but always wins: its face value is one wei more than its
	// expected value, the closest a sender accepts. One paid request is then
	// one redeemed ticket, and the test does not wait on luck.
	const (
		ticketEV    = "10000000000000000" // 0.01 ETH
		ticketValue = "10000000000000001"
	)
	h.step("starting the orchestrator in pool mode, on-chain")
	h.process("orchestrator", nil, livepeerBin,
		"-orchestrator", "-orchSecret", "an-orch-secret-no-runner-uses", "-useLiveRunners",
		"-network", "devnet", "-ethUrl", geth.URI, "-ethController", orch.controller, "-ethAcctAddr", orch.address, "-ethPassword", "",
		"-dataDir", orch.datadir, "-serviceAddr", fmt.Sprintf("%s:%d", host, orchPort), "-httpAddr", fmt.Sprintf("0.0.0.0:%d", orchPort),
		"-liveRunnerAddr", orchURL, "-cliAddr", fmt.Sprintf("127.0.0.1:%d", orchCLIPort), "-enableCliTxRoutes",
		"-liveRunnerAuthWebhookUrl", poolURL+"/v1/orchestrator/admit", "-liveRunnerPoolSecret", poolSecret,
		"-monitor", "-kafkaBootstrapServers", fmt.Sprintf("127.0.0.1:%d", kafkaPort), "-kafkaOrchestratorTopic", topic,
		"-pricePerUnit", "1", "-priceFeedAddr", priceFeed.Hex(), "-ticketEV", ticketEV, "-maxFaceValue", ticketValue,
		"-initializeRound", "-initializeRoundMaxDelay", "1s", "-blockPollingInterval", "1", "-v", "4")
	h.eventually("the orchestrator's CLI port", 2*time.Minute, func() bool { return h.status(orchCLI+"/status") == 200 })
	h.eventually("the orchestrator to be activated on-chain", 3*time.Minute, func() bool {
		_, ok := httpPostWithParams(orchCLI+"/activateOrchestrator", url.Values{
			"pricePerUnit": {"1"}, "pixelsPerUnit": {"1"}, "blockRewardCut": {"30"}, "feeShare": {"100"},
			"serviceURI": {orchURL}, "amount": {"50"},
		})
		return ok
	})
	waitForNextRound(t, orch.dev.Client)

	h.step("starting the customer's signer and funding its deposit")
	h.process("signer", nil, livepeerBin,
		"-remoteSigner", "-network", "devnet", "-ethUrl", geth.URI, "-ethController", signer.controller, "-ethAcctAddr", signer.address, "-ethPassword", "",
		"-dataDir", signer.datadir, "-httpAddr", fmt.Sprintf("127.0.0.1:%d", signerPort), "-cliAddr", fmt.Sprintf("127.0.0.1:%d", signerCLIPort), "-enableCliTxRoutes",
		"-priceFeedAddr", priceFeed.Hex(), "-maxTicketEV", ticketValue, "-maxTotalEV", "1000000000000000000", "-blockPollingInterval", "1", "-v", "4")
	h.eventually("the signer's CLI port", 2*time.Minute, func() bool { return h.status(signerCLI+"/status") == 200 })
	// The protocol refuses a deposit in the moment between a round starting
	// and its being initialized; a refused one is tried again.
	h.eventually("the signer's deposit and reserve", 2*time.Minute, func() bool {
		_, funded := httpPostWithParams(signerCLI+"/fundDepositAndReserve", url.Values{"depositAmount": {ether(1).String()}, "reserveAmount": {ether(1).String()}})
		return funded
	})

	h.step("starting the pool service")
	poolLog := h.process("pool", []string{
		"OPENPOOL_REGION_ID=local", "OPENPOOL_REGION_NAME=Local",
		fmt.Sprintf("OPENPOOL_LISTEN=0.0.0.0:%d", poolPort), "OPENPOOL_PUBLIC_URL=" + poolURL,
		"OPENPOOL_DB_PATH=" + filepath.Join(h.dir, "openpool.db"), "OPENPOOL_COMPOSE_KEY_FILE=" + filepath.Join(h.dir, "compose.key"),
		"OPENPOOL_ORCHESTRATOR_URL=" + orchURL, "OPENPOOL_ORCHESTRATOR_CLI_URL=" + orchCLI, "OPENPOOL_ORCHESTRATOR_INSECURE_TLS=true",
		"OPENPOOL_POOL_SECRET=" + poolSecret, "OPENPOOL_SESSION_SECRET=a-session-signing-secret-for-the-end-to-end-pool",
		"OPENPOOL_SIGN_IN_DOMAINS=app.open-pool.test", "OPENPOOL_ADMIN_EMAIL=" + adminEmail,
		"OPENPOOL_TUNNEL_IMAGE=" + tunnelImage, "OPENPOOL_AGENT_IMAGE=openpool-agent:e2e",
		fmt.Sprintf("OPENPOOL_KAFKA_BROKERS=127.0.0.1:%d", kafkaPort), "OPENPOOL_KAFKA_TOPIC=" + topic,
		"OPENPOOL_ETH_RPC_URL=" + geth.URI, "OPENPOOL_PAYOUT_KEYSTORE=" + payoutKeystore, "OPENPOOL_PAYOUT_PASSWORD=",
		"OPENPOOL_CHAIN_ID=" + chain.id.String(),
		// The first payout window opens on the first payout tick, with the
		// window length in force then: late enough for the operator to have set it.
		"OPENPOOL_RECONCILE_INTERVAL=1s", "OPENPOOL_PAYOUT_INTERVAL=3s", "OPENPOOL_SETTLE_DELAY=3s",
	}, poolBin)
	pool := &poolClient{t: t, base: poolURL + "/v1"}
	h.eventually("the pool service", time.Minute, func() bool { return h.status(poolURL+"/healthz") == 200 })

	// The operator signs in with the emailed code, which a service with no
	// email configured writes to its log, and sets the pool up.
	h.step("the operator sets the pool up")
	pool.call("POST", "/auth/admin/code", "", map[string]string{"email": adminEmail}, nil)
	var code string
	h.eventually("the sign-in code", 30*time.Second, func() bool {
		match := regexp.MustCompile(`sign-in code written to the log instead.*code=(\d{6})`).FindStringSubmatch(poolLog())
		if match != nil {
			code = match[1]
		}
		return match != nil
	})
	var session struct{ Token string }
	pool.call("POST", "/auth/admin/verify", "", map[string]string{"email": adminEmail, "code": code}, &session)
	admin := session.Token
	// Payout windows of two rounds, which on the devnet is under two minutes.
	pool.call("PUT", "/admin/settings", admin, map[string]any{"window_rounds": 2, "test_interval_seconds": 30}, nil)
	var gpu, app struct{ ID string }
	pool.call("POST", "/admin/gpus", admin, map[string]any{"name": "NVIDIA GeForce RTX 4090", "vram_mb": 24564}, &gpu)
	pool.call("POST", "/admin/apps", admin, map[string]any{
		"name": appName, "mode": "single-shot", "price": map[string]string{"price": "0.03", "unit": "fixed"}, "required_version": "1.0.0",
		// A web server that answers with what it was asked: it stands in for a model server and needs no GPU.
		"container":        map[string]any{"image": echoImage, "port": 8080, "health_path": "/", "no_gpu": true},
		"eligible_gpus":    []string{gpu.ID},
		"default_capacity": 2, "max_capacity": 2,
		"test":        map[string]any{"method": "GET", "path": "/are-you-there", "expect_status": 200, "expect_contains": []string{"/are-you-there"}, "timeout_ms": 10000},
		"description": "Answers with what it was asked.",
	}, &app)
	pool.call("POST", "/admin/terms", admin, map[string]string{"scope": "global", "body": "Be good."}, nil)

	// The member signs in with their wallet, applies, and is approved with
	// their machine assigned the app.
	h.step("a member signs up and is approved")
	memberToken := pool.walletSignIn(member, "app.open-pool.test")
	draft := map[string]any{"display_name": "Alice", "machines": []map[string]any{{"label": "box-1", "gpus": []map[string]any{{"gpu_id": gpu.ID, "count": 1}}}}}
	var toSign struct{ Message string }
	pool.call("POST", "/applications/message", memberToken, draft, &toSign)
	draft["signature"] = personalSign(t, member, toSign.Message)
	var application struct{ ID string }
	pool.call("POST", "/applications", memberToken, draft, &application)
	var approved struct{ ID string }
	pool.call("POST", "/admin/applications/"+application.ID+"/approve", admin, map[string]any{
		"machines": []map[string]any{{"machine": 0, "assignments": []map[string]any{{"app_id": app.ID}}}},
	}, &approved)
	var machines struct {
		Machines []struct {
			ID       string
			Reported *struct {
				AgentVersion string `json:"agent_version"`
				Error        string
				Containers   []struct{ Service, State string }
			}
		}
	}
	pool.call("GET", "/me/machines", memberToken, nil, &machines)
	var credential struct {
		AgentCommand string `json:"agent_command"`
	}
	pool.call("POST", "/me/machines/"+machines.Machines[0].ID+"/credential", memberToken, nil, &credential)

	// The member runs the one command they are shown. Two things in it are
	// about this machine and not about the pool: where Docker's socket is,
	// and whether there are NVIDIA GPUs to hand the agent.
	h.step("the member runs the install command")
	command := strings.Replace(credential.AgentCommand, "--name openpool-agent-local", "--name openpool-e2e-agent", 1)
	if socket, ok := strings.CutPrefix(os.Getenv("DOCKER_HOST"), "unix://"); ok {
		command = strings.Replace(command, "-v /var/run/docker.sock:", "-v "+socket+":", 1)
	}
	if !strings.Contains(h.dockerOutput("info", "--format", "{{json .Runtimes}}"), "nvidia") {
		command = strings.Replace(command, " --gpus all", "", 1)
	}
	require.Contains(t, command, "openpool-agent:e2e --pool "+poolURL)
	exec.Command("docker", "rm", "-f", "openpool-e2e-agent").Run()
	h.run(h.dir, "sh", "-c", command+" --interval 2s")
	project := h.agentProject(poolURL)
	t.Cleanup(func() {
		h.saveOutput("agent", "docker", "logs", "openpool-e2e-agent")
		exec.Command("docker", "rm", "-f", "openpool-e2e-agent").Run()
		exec.Command("docker", "compose", "-p", project, "down", "--remove-orphans", "--volumes").Run()
	})

	// The agent starts the app and its tunnel; the tunnel registers with the
	// orchestrator, which asks the pool; the pool tests the runner through
	// the orchestrator and lets it serve.
	h.eventually("the agent to report the app and its tunnel running", 3*time.Minute, func() bool {
		pool.call("GET", "/me/machines", memberToken, nil, &machines)
		report := machines.Machines[0].Reported
		if report == nil || len(report.Containers) != 2 {
			return false
		}
		require.Empty(t, report.Error, "the agent could not apply what the pool told it")
		return report.Containers[0].State == "running" && report.Containers[1].State == "running"
	})
	require.Equal(t, "e2e", machines.Machines[0].Reported.AgentVersion)
	var runners struct {
		Runners []struct {
			RunnerID   string `json:"runner_id"`
			State      string
			LastTestOK bool `json:"last_test_ok"`
		}
	}
	h.eventually("the pool to test the runner and let it serve", 3*time.Minute, func() bool {
		if pool.try("GET", "/me/runners", memberToken, nil, &runners) != 200 || len(runners.Runners) != 1 {
			return false
		}
		return runners.Runners[0].State == "active" && runners.Runners[0].LastTestOK
	})

	// A customer pays for one request. The orchestrator says what it costs
	// and how to pay; the signer makes the payment from its deposit; the
	// request is repeated with it.
	//
	// The orchestrator redeems a winning ticket forty blocks after it was
	// made, and a devnet round is only a little longer than that. Two things
	// follow. The redemption is in the round after the call, so the customer
	// calls in the first round of a two-round window, and both fall in it.
	// And a redemption that lands just as a round starts is sent before the
	// round is initialized, fails, and is given up on by the orchestrator: so
	// the customer calls a third of the way into the round, and the
	// redemption lands well inside the next one.
	h.step("a customer pays for a request")
	type window struct {
		ID                    string
		Status                string
		FirstRound, LastRound int64
		PotWei                string
	}
	windows := func() []window {
		var listed struct {
			Batches []struct {
				ID         string
				Status     string
				FirstRound int64  `json:"first_round"`
				LastRound  int64  `json:"last_round"`
				PotWei     string `json:"pot_wei"`
			}
		}
		pool.call("GET", "/admin/batches", admin, nil, &listed)
		out := make([]window, 0, len(listed.Batches))
		for _, b := range listed.Batches {
			out = append(out, window{b.ID, b.Status, b.FirstRound, b.LastRound, b.PotWei})
		}
		return out
	}
	roundLength, err := orch.dev.Client.RoundLength()
	require.NoError(t, err)
	h.eventually("a third of the way into the first round of a payout window", 6*time.Minute, func() bool {
		initialized, err := orch.dev.Client.LastInitializedRound()
		require.NoError(t, err)
		started, err := orch.dev.Client.CurrentRoundStartBlock()
		require.NoError(t, err)
		block, err := chain.eth.BlockNumber(ctx)
		require.NoError(t, err)
		into := int64(block) - started.Int64()
		if into < roundLength.Int64()/3 || into > roundLength.Int64()/2 {
			return false
		}
		for _, w := range windows() {
			if w.Status == "open" && w.FirstRound == initialized.Int64() {
				require.Equal(t, w.FirstRound+1, w.LastRound, "a window is two rounds")
				return true
			}
		}
		return false
	})
	customer := &http.Client{Timeout: time.Minute, Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}} //nolint:gosec // the orchestrator's own self-signed certificate
	appRoute := orchURL + "/pool/" + base64.RawURLEncoding.EncodeToString([]byte(appName)) + "/app/hello?from=a-customer"
	unpaid, err := http.NewRequest("POST", appRoute, strings.NewReader(`{"prompt":"hi"}`))
	require.NoError(t, err)
	unpaid.Header.Set("Livepeer-Payer-Address", signer.address)
	challengeRes, err := customer.Do(unpaid)
	require.NoError(t, err)
	challengeBody, _ := io.ReadAll(challengeRes.Body)
	challengeRes.Body.Close()
	require.Equal(t, http.StatusPaymentRequired, challengeRes.StatusCode, string(challengeBody))
	var challenge struct {
		PaymentParams string `json:"payment_params"`
		ManifestID    string `json:"manifest_id"`
	}
	require.NoError(t, json.Unmarshal(challengeBody, &challenge))
	params, err := base64.StdEncoding.DecodeString(challenge.PaymentParams)
	require.NoError(t, err)

	paymentRequest, err := json.Marshal(map[string]any{"orchestrator": params, "ManifestID": challenge.ManifestID, "app": appName, "type": "fixed"})
	require.NoError(t, err)
	paymentRes, err := http.Post(signerURL+"/generate-live-payment", "application/json", bytes.NewReader(paymentRequest))
	require.NoError(t, err)
	paymentBody, _ := io.ReadAll(paymentRes.Body)
	paymentRes.Body.Close()
	require.Equal(t, http.StatusOK, paymentRes.StatusCode, string(paymentBody))
	var payment struct{ Payment, SegCreds string }
	require.NoError(t, json.Unmarshal(paymentBody, &payment))

	paid, err := http.NewRequest("POST", appRoute, strings.NewReader(`{"prompt":"hi"}`))
	require.NoError(t, err)
	paid.Header.Set("Content-Type", "application/json")
	paid.Header.Set("Livepeer-Payment", payment.Payment)
	paid.Header.Set("Livepeer-Segment", payment.SegCreds)
	answerRes, err := customer.Do(paid)
	require.NoError(t, err)
	answerBody, _ := io.ReadAll(answerRes.Body)
	answerRes.Body.Close()
	require.Equal(t, http.StatusOK, answerRes.StatusCode, string(answerBody))
	// The member's app answered: it says what it was asked, and by which path.
	var answer struct {
		Path  string
		Query map[string]string
		JSON  map[string]string `json:"json"`
	}
	require.NoError(t, json.Unmarshal(answerBody, &answer), string(answerBody))
	require.Equal(t, "/hello", answer.Path)
	require.Equal(t, "a-customer", answer.Query["from"])
	require.Equal(t, "hi", answer.JSON["prompt"])

	// What the orchestrator published about the session reached the pool
	// through Kafka and became the member's earnings; the ticket it redeemed
	// on the chain became the window's pot.
	h.step("the pool books the member's fee and the orchestrator's redeemed ticket")
	var earnings struct {
		Earnings []struct {
			Round    int64
			App      string
			FeeWei   string `json:"fee_wei"`
			Sessions int
		}
	}
	h.eventually("the member's earnings", time.Minute, func() bool {
		pool.call("GET", "/me/earnings", memberToken, nil, &earnings)
		return len(earnings.Earnings) == 1 && earnings.Earnings[0].FeeWei != "0"
	})
	require.Equal(t, appName, earnings.Earnings[0].App)
	require.Equal(t, 1, earnings.Earnings[0].Sessions)
	// Three cents at 3000 dollars an ETH.
	require.Equal(t, "10000000000000", earnings.Earnings[0].FeeWei)

	feeRound := earnings.Earnings[0].Round
	batch := func() window {
		for _, w := range windows() {
			if w.FirstRound <= feeRound && feeRound <= w.LastRound {
				return w
			}
		}
		t.Fatalf("no payout window holds round %d", feeRound)
		return window{}
	}
	require.Equal(t, feeRound, batch().FirstRound, "the fee is booked in the round the customer called in, the window's first")

	// The window's rounds end, it is closed with the ticket the orchestrator
	// redeemed in it as its pot, the operator approves, and the pool pays
	// the member from its wallet.
	h.step("the window closes and the operator approves the payout")
	h.eventually("the payout window to close", 5*time.Minute, func() bool { return batch().Status != "open" })
	require.Equal(t, "ready", batch().Status, "a window with a pot and a member who earned waits for approval")
	require.Equal(t, ticketValue, batch().PotWei, "the pot is the ticket the orchestrator redeemed on the chain")
	batchID := batch().ID
	pool.call("POST", "/admin/batches/"+batchID+"/approve", admin, nil, nil)
	var detail struct {
		Status string
		Items  []struct {
			Address   string
			AmountWei string `json:"amount_wei"`
			Status    string
			TxHash    string `json:"tx_hash"`
			Error     string
		}
	}
	h.eventually("the payment to be confirmed on the chain", 3*time.Minute, func() bool {
		pool.call("GET", "/admin/batches/"+batchID, admin, nil, &detail)
		if len(detail.Items) == 1 && detail.Items[0].Error != "" {
			t.Fatalf("the payment failed: %s", detail.Items[0].Error)
		}
		return detail.Status == "sent"
	})
	require.Len(t, detail.Items, 1)
	require.Equal(t, strings.ToLower(memberAddress.Hex()), strings.ToLower(detail.Items[0].Address))
	require.Equal(t, "confirmed", detail.Items[0].Status)
	// The pot was one ticket of 0.01 ETH and a wei. The pool keeps 15%; the
	// member, the only one who earned, gets the rest, rounded down to a wei.
	require.Equal(t, "8500000000000000", detail.Items[0].AmountWei)
	require.Equal(t, "8500000000000000", chain.balance(memberAddress).String(), "what the member's address holds is what the pool paid it")
	h.logf("paid: transaction %s sent %s wei to %s", detail.Items[0].TxHash, detail.Items[0].AmountWei, memberAddress.Hex())

	var payouts struct {
		Payouts []struct {
			Status    string
			AmountWei string `json:"amount_wei"`
			TxHash    string `json:"tx_hash"`
		}
	}
	pool.call("GET", "/me/payouts", memberToken, nil, &payouts)
	require.Len(t, payouts.Payouts, 1)
	require.Equal(t, "confirmed", payouts.Payouts[0].Status)
	require.Equal(t, detail.Items[0].TxHash, payouts.Payouts[0].TxHash)

	// The operator unassigns the app; the machine stops running it without
	// the member doing anything.
	h.step("the operator unassigns the app and the machine stops running it")
	pool.call("PUT", "/admin/machines/"+machines.Machines[0].ID+"/assignments", admin, map[string]any{"assignments": []any{}}, nil)
	h.eventually("the agent to take the app down", 2*time.Minute, func() bool {
		return h.dockerOutput("ps", "--all", "--quiet", "--filter", "label=com.docker.compose.project="+project) == ""
	})
}

// --- the harness ---

type harness struct {
	t     *testing.T
	dir   string
	start time.Time
}

func newHarness(t *testing.T) *harness {
	dir := os.Getenv("OPENPOOL_E2E_DIR")
	if dir == "" {
		dir = t.TempDir()
	} else {
		require.NoError(t, os.MkdirAll(dir, 0o755))
	}
	return &harness{t: t, dir: dir, start: time.Now()}
}

func (h *harness) logf(format string, args ...any) {
	h.t.Helper()
	h.t.Logf("[%4.0fs] %s", time.Since(h.start).Seconds(), fmt.Sprintf(format, args...))
}

func (h *harness) step(what string) { h.t.Helper(); h.logf("== %s", what) }

func (h *harness) repoRoot() string {
	root, err := filepath.Abs("../..")
	require.NoError(h.t, err)
	return root
}

// run runs a command to its end and fails the test with its output if it fails.
func (h *harness) run(dir, name string, args ...string) {
	h.t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	require.NoError(h.t, err, "%s %s:\n%s", name, strings.Join(args, " "), out)
}

func (h *harness) docker(args ...string) { h.t.Helper(); h.run(h.dir, "docker", args...) }

func (h *harness) dockerOutput(args ...string) string {
	h.t.Helper()
	out, err := exec.Command("docker", args...).CombinedOutput()
	require.NoError(h.t, err, "docker %s:\n%s", strings.Join(args, " "), out)
	return strings.TrimSpace(string(out))
}

// container starts a container that lives as long as the test.
func (h *harness) container(name string, args ...string) {
	h.t.Helper()
	exec.Command("docker", "rm", "-f", name).Run()
	h.docker(append([]string{"run", "-d", "--name", name}, args...)...)
	h.t.Cleanup(func() {
		h.saveOutput(name, "docker", "logs", name)
		exec.Command("docker", "rm", "-f", name).Run()
	})
}

// digestOf is the image as its repository names it by digest.
func (h *harness) digestOf(image, repository string) string {
	h.t.Helper()
	for _, digest := range strings.Fields(h.dockerOutput("image", "inspect", "--format", `{{join .RepoDigests " "}}`, image)) {
		if strings.HasPrefix(digest, repository+"@sha256:") {
			return digest
		}
	}
	h.t.Fatalf("%s has no digest in %s", image, repository)
	return ""
}

// saveOutput keeps a command's output beside the processes' logs.
func (h *harness) saveOutput(name, command string, args ...string) {
	out, _ := exec.Command(command, args...).CombinedOutput()
	_ = os.WriteFile(filepath.Join(h.dir, name+".log"), out, 0o644)
}

// process starts a program that lives as long as the test, with its output
// in a log file. It returns a function that reads the log so far.
func (h *harness) process(name string, env []string, bin string, args ...string) func() string {
	h.t.Helper()
	logPath := filepath.Join(h.dir, name+".log")
	logFile, err := os.Create(logPath)
	require.NoError(h.t, err)
	cmd := exec.Command(bin, args...)
	cmd.Dir = h.dir
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if env != nil {
		cmd.Env = append([]string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME")}, env...)
	}
	require.NoError(h.t, cmd.Start(), name)
	exited := make(chan struct{})
	go func() { _ = cmd.Wait(); close(exited) }()
	h.t.Cleanup(func() {
		_ = cmd.Process.Signal(os.Interrupt)
		select {
		case <-exited:
		case <-time.After(15 * time.Second):
			_ = cmd.Process.Kill()
		}
		logFile.Close()
		if h.t.Failed() {
			h.t.Logf("the end of %s:\n%s", logPath, tail(logPath, 40))
		}
	})
	return func() string {
		data, _ := os.ReadFile(logPath)
		return string(data)
	}
}

func tail(path string, lines int) string {
	data, _ := os.ReadFile(path)
	all := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(all) > lines {
		all = all[len(all)-lines:]
	}
	return strings.Join(all, "\n")
}

// eventually waits until check holds.
func (h *harness) eventually(what string, limit time.Duration, check func() bool) {
	h.t.Helper()
	deadline := time.Now().Add(limit)
	for !check() {
		if time.Now().After(deadline) {
			h.t.Fatalf("waited %s for %s", limit, what)
		}
		time.Sleep(time.Second)
	}
	h.logf("%s: ok", what)
}

func (h *harness) status(url string) int {
	res, err := http.Get(url)
	if err != nil {
		return 0
	}
	res.Body.Close()
	return res.StatusCode
}

// agentProject is the Compose project the agent keeps this pool's containers in.
func (h *harness) agentProject(poolURL string) string {
	h.t.Helper()
	var project string
	h.eventually("the agent's containers", 3*time.Minute, func() bool {
		for _, name := range strings.Fields(h.dockerOutput("ps", "--all", "--format", `{{.Label "com.docker.compose.project"}}`)) {
			if strings.HasPrefix(name, "openpool-") {
				project = name
			}
		}
		return project != ""
	})
	return project
}

// livepeerNode is a livepeer process's account on the devnet.
type livepeerNode struct {
	address, datadir, controller string
	dev                          *devtool.Devtool
}

// livepeerAccount makes a funded account for a livepeer process: ETH from
// the devnet's mining account, and LPT to stake when it is an orchestrator.
func (h *harness) livepeerAccount(endpoint, name string, orchestrator bool) *livepeerNode {
	h.t.Helper()
	datadir := filepath.Join(h.dir, name)
	keystoreDir := filepath.Join(datadir, "keystore")
	cfg := devtool.NewDevtoolConfig()
	cfg.Endpoint = endpoint
	cfg.Account = devtool.CreateKey(keystoreDir)
	cfg.KeystoreDir = keystoreDir
	dev, err := devtool.Init(cfg)
	require.NoError(h.t, err)
	if orchestrator {
		require.NoError(h.t, dev.RequestTokens())
	}
	require.NoError(h.t, dev.InitializeRound())
	return &livepeerNode{address: cfg.Account, datadir: datadir, controller: dev.EthController, dev: &dev}
}

// newKeystore makes a wallet with an empty password and returns its key file.
func (h *harness) newKeystore(name string) (string, ethcommon.Address) {
	h.t.Helper()
	dir := filepath.Join(h.dir, name)
	account, err := keystore.NewKeyStore(dir, keystore.LightScryptN, keystore.LightScryptP).NewAccount("")
	require.NoError(h.t, err)
	return account.URL.Path, account.Address
}

// chainClient sends from the devnet's mining account, which the node holds unlocked.
type chainClient struct {
	t     *testing.T
	rpc   *rpc.Client
	eth   *ethclient.Client
	miner ethcommon.Address
	id    *big.Int
}

func (h *harness) dialChain(endpoint string) *chainClient {
	h.t.Helper()
	client, err := rpc.Dial(endpoint)
	require.NoError(h.t, err)
	// The node takes a moment to answer after its container starts.
	var accounts []ethcommon.Address
	h.eventually("the devnet to answer", time.Minute, func() bool {
		return client.Call(&accounts, "eth_accounts") == nil && len(accounts) > 0
	})
	eth := ethclient.NewClient(client)
	id, err := eth.ChainID(context.Background())
	require.NoError(h.t, err)
	return &chainClient{t: h.t, rpc: client, eth: eth, miner: accounts[0], id: id}
}

func ether(n int64) *big.Int { return new(big.Int).Mul(big.NewInt(n), big.NewInt(1e18)) }

func (c *chainClient) transact(tx map[string]any) map[string]any {
	c.t.Helper()
	tx["from"] = c.miner
	var hash ethcommon.Hash
	require.NoError(c.t, c.rpc.Call(&hash, "eth_sendTransaction", tx))
	var receipt map[string]any
	deadline := time.Now().Add(time.Minute)
	for receipt == nil {
		require.True(c.t, time.Now().Before(deadline), "transaction %s was not mined", hash.Hex())
		time.Sleep(500 * time.Millisecond)
		require.NoError(c.t, c.rpc.Call(&receipt, "eth_getTransactionReceipt", hash))
	}
	require.Equal(c.t, "0x1", receipt["status"], "transaction %s failed", hash.Hex())
	return receipt
}

// deploy puts a contract on the chain and returns its address.
func (c *chainClient) deploy(code []byte) ethcommon.Address {
	c.t.Helper()
	receipt := c.transact(map[string]any{"data": hexutil.Bytes(code), "gas": hexutil.Uint64(1_000_000)})
	return ethcommon.HexToAddress(receipt["contractAddress"].(string))
}

func (c *chainClient) send(to ethcommon.Address, amount *big.Int) {
	c.t.Helper()
	c.transact(map[string]any{"to": to, "value": (*hexutil.Big)(amount)})
}

func (c *chainClient) balance(address ethcommon.Address) *big.Int {
	c.t.Helper()
	balance, err := c.eth.BalanceAt(context.Background(), address, nil)
	require.NoError(c.t, err)
	return balance
}

// poolClient calls the pool service's API as a site would.
type poolClient struct {
	t    *testing.T
	base string
}

// try makes a call and returns the status; a 2xx answer is decoded into out.
func (p *poolClient) try(method, path, token string, body, out any) int {
	p.t.Helper()
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		require.NoError(p.t, err)
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, p.base+path, reader)
	require.NoError(p.t, err)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	res, err := http.DefaultClient.Do(req)
	require.NoError(p.t, err)
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	require.NoError(p.t, err)
	if res.StatusCode/100 == 2 && out != nil && len(data) > 0 {
		require.NoError(p.t, json.Unmarshal(data, out), "%s %s: %s", method, path, data)
	}
	if res.StatusCode/100 != 2 {
		p.t.Logf("%s %s answered %d: %s", method, path, res.StatusCode, data)
	}
	return res.StatusCode
}

// call is try that requires success.
func (p *poolClient) call(method, path, token string, body, out any) {
	p.t.Helper()
	status := p.try(method, path, token, body, out)
	require.Equal(p.t, 2, status/100, "%s %s answered %d", method, path, status)
}

// personalSign signs a text the way a wallet's personal_sign does.
func personalSign(t *testing.T, key *ecdsa.PrivateKey, message string) string {
	t.Helper()
	signature, err := crypto.Sign(accounts.TextHash([]byte(message)), key)
	require.NoError(t, err)
	signature[64] += 27
	return hexutil.Encode(signature)
}

// walletSignIn signs in as the member site does: a one-time value from the
// service, in an EIP-4361 message the wallet signs.
func (p *poolClient) walletSignIn(key *ecdsa.PrivateKey, domain string) string {
	p.t.Helper()
	var challenge struct {
		Nonce  string
		SignIn struct {
			ChainID int `json:"chain_id"`
		} `json:"sign_in"`
	}
	p.call("POST", "/auth/wallet/nonce", "", nil, &challenge)
	message := fmt.Sprintf("%s wants you to sign in with your Ethereum account:\n%s\n\nSign in to Open Pool.\n\nURI: https://%s\nVersion: 1\nChain ID: %d\nNonce: %s\nIssued At: %s",
		domain, crypto.PubkeyToAddress(key.PublicKey).Hex(), domain, challenge.SignIn.ChainID, challenge.Nonce, time.Now().UTC().Format(time.RFC3339))
	var session struct{ Token string }
	p.call("POST", "/auth/wallet/verify", "", map[string]string{"message": message, "signature": personalSign(p.t, key, message)}, &session)
	return session.Token
}
