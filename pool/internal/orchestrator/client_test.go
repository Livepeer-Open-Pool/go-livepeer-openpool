package orchestrator_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Livepeer-Open-Pool/pool/internal/core"
	"github.com/Livepeer-Open-Pool/pool/internal/orchestrator"
)

type request struct {
	method, path, contentType, body string
}

// fakeCLI is the orchestrator's CLI port: it records requests and answers
// each path with the status and body set for it.
type fakeCLI struct {
	mu       sync.Mutex
	requests []request
	answers  map[string]answer
}

type answer struct {
	status int
	body   string
}

func newFakeCLI(t *testing.T) (*fakeCLI, *orchestrator.Client) {
	t.Helper()
	cli := &fakeCLI{answers: map[string]answer{}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		cli.mu.Lock()
		cli.requests = append(cli.requests, request{r.Method, r.URL.Path, r.Header.Get("Content-Type"), string(body)})
		a, ok := cli.answers[r.URL.Path]
		cli.mu.Unlock()
		if !ok {
			a = answer{http.StatusNoContent, ""}
		}
		w.WriteHeader(a.status)
		_, _ = io.WriteString(w, a.body)
	}))
	t.Cleanup(srv.Close)
	client, err := orchestrator.New(srv.URL)
	require.NoError(t, err)
	return cli, client
}

func (c *fakeCLI) last(t *testing.T) request {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	require.NotEmpty(t, c.requests)
	return c.requests[len(c.requests)-1]
}

func TestClientSpeaksTheOrchestratorsCLIEndpoints(t *testing.T) {
	cli, client := newFakeCLI(t)
	ctx := context.Background()

	cli.answers["/liveRunner/runners"] = answer{http.StatusOK, `{"runners":[{"runner_id":"runner_aaaa","member_id":"mbr_1","payout_address":"0xb1",
		"state":"probation","status":"ready","ready":true,"app":"echo","mode":"single-shot","version":"1.0.0","capacity":2,"capacity_registered":2,
		"capacity_used":1,"tunnel":true,"tunnel_connected":true,"remote_addr":"203.0.113.7","registered_at_ms":1790000000000,
		"last_heartbeat_ms":1790000012000,"session_ids":["s1"],"static":false}]}`}
	runners, err := client.Runners(ctx)
	require.NoError(t, err)
	require.Equal(t, []core.OrchRunner{{
		RunnerID: "runner_aaaa", MemberID: "mbr_1", PayoutAddress: "0xb1", State: "probation", Status: "ready", Ready: true,
		App: "echo", Mode: "single-shot", Version: "1.0.0", Capacity: 2, CapacityRegistered: 2, CapacityUsed: 1,
		Tunnel: true, TunnelConnected: true, RemoteAddr: "203.0.113.7", RegisteredAtMs: 1790000000000, LastHeartbeatMs: 1790000012000,
		SessionIDs: []string{"s1"},
	}}, runners)
	require.Equal(t, request{"GET", "/liveRunner/runners", "", ""}, cli.last(t))

	require.NoError(t, client.SetState(ctx, "runner_aaaa", "active", "probe passed"))
	require.Equal(t, request{"POST", "/liveRunner/setState", "application/x-www-form-urlencoded", "reason=probe+passed&runner_id=runner_aaaa&state=active"}, cli.last(t))

	require.NoError(t, client.EvictRunner(ctx, "runner_aaaa", "slow"))
	require.Equal(t, "reason=slow&runner_id=runner_aaaa", cli.last(t).body)
	require.NoError(t, client.EvictMember(ctx, "mbr_1", "banned"))
	require.Equal(t, request{"POST", "/liveRunner/evict", "application/x-www-form-urlencoded", "member_id=mbr_1&reason=banned"}, cli.last(t))

	require.NoError(t, client.SetPrices(ctx, map[string]core.Price{"vllm/qwen2.5-7b-instruct": {Amount: "0.50", Unit: "hour"}}))
	sent := cli.last(t)
	require.Equal(t, "/liveRunner/prices", sent.path)
	require.Equal(t, "application/json", sent.contentType)
	var table map[string]map[string]map[string]string
	require.NoError(t, json.Unmarshal([]byte(sent.body), &table))
	require.Equal(t, map[string]string{"price": "0.50", "currency": "usd", "unit": "hour"}, table["apps"]["vllm/qwen2.5-7b-instruct"])

	cli.answers["/currentRound"] = answer{http.StatusOK, "4100"}
	round, err := client.CurrentRound(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(4100), round)
}

func TestClientReportsWhatTheOrchestratorRefused(t *testing.T) {
	cli, client := newFakeCLI(t)
	ctx := context.Background()
	kind := func(err error) core.ErrorKind {
		var coreErr *core.Error
		require.ErrorAs(t, err, &coreErr)
		return coreErr.Kind
	}

	cli.answers["/liveRunner/setState"] = answer{http.StatusNotFound, "runner not found\n"}
	err := client.SetState(ctx, "runner_gone", "active", "")
	require.Equal(t, core.NotFound, kind(err))
	require.EqualError(t, err, "runner not found")

	cli.answers["/liveRunner/prices"] = answer{http.StatusBadRequest, `apps["echo"]: price_info.unit must be hour, 720p, or fixed`}
	err = client.SetPrices(ctx, map[string]core.Price{"echo": {Amount: "1", Unit: "minute"}})
	require.Equal(t, core.Invalid, kind(err))

	// Anything else is the orchestrator failing, not a decision.
	cli.answers["/liveRunner/runners"] = answer{http.StatusInternalServerError, "boom"}
	_, err = client.Runners(ctx)
	require.Error(t, err)
	var coreErr *core.Error
	require.NotErrorAs(t, err, &coreErr)

	cli.answers["/liveRunner/runners"] = answer{http.StatusOK, "not json"}
	_, err = client.Runners(ctx)
	require.Error(t, err)
	cli.answers["/currentRound"] = answer{http.StatusOK, "soon"}
	_, err = client.CurrentRound(ctx)
	require.Error(t, err)
}

func TestNewRejectsWhatIsNotAURL(t *testing.T) {
	for _, bad := range []string{"", "127.0.0.1:7935", "ftp://host", "/path"} {
		_, err := orchestrator.New(bad)
		require.Error(t, err, bad)
	}
}
