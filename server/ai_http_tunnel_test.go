package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"

	"github.com/livepeer/go-livepeer/ai/runner/tunnelclient"
)

// tunnelFixture is an orchestrator, a local app, and the runner-side client
// joining them through a tunnel, all in process.
type tunnelFixture struct {
	lp     *lphttp
	orch   *httptest.Server
	app    *httptest.Server
	client *tunnelclient.Client
	cancel context.CancelFunc
}

func newTunnelFixture(t *testing.T, appHandler http.Handler, capacity int) *tunnelFixture {
	t.Helper()
	lp := newLiveRunnerHTTP(t, true)
	orch := httptest.NewServer(lp)
	t.Cleanup(orch.Close)
	app := httptest.NewServer(appHandler)
	t.Cleanup(app.Close)

	client, err := tunnelclient.New(tunnelclient.Config{
		Orchestrator: orch.URL,
		OrchSecret:   lp.orchestrator.RegistrationSecret(),
		Target:       app.URL,
		App:          "test/tunnel",
		Mode:         "single-shot",
		Capacity:     capacity,
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
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
	f := &tunnelFixture{lp: lp, orch: orch, app: app, client: client, cancel: cancel}
	f.waitListed(t, "")
	return f
}

// waitListed waits until discovery lists exactly one runner and returns its id.
func (f *tunnelFixture) waitListed(t *testing.T, notID string) string {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if id := f.listedRunner(t); id != "" && id != notID {
			return id
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("runner never appeared in discovery")
	return ""
}

func (f *tunnelFixture) waitUnlisted(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if f.listedRunner(t) == "" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("runner never left discovery")
}

func (f *tunnelFixture) listedRunner(t *testing.T) string {
	t.Helper()
	res, err := http.Get(f.orch.URL + "/discovery")
	require.NoError(t, err)
	defer res.Body.Close()
	var entries []liveRunnerDiscoveryEntry
	require.NoError(t, json.NewDecoder(res.Body).Decode(&entries))
	if len(entries) == 0 || len(entries[0].Runners) == 0 {
		return ""
	}
	// http://localhost:1234/apps/<id>/app
	parts := strings.Split(entries[0].Runners[0].URL, "/")
	return parts[len(parts)-2]
}

func (f *tunnelFixture) appURL(runnerID, path string) string {
	return f.orch.URL + "/apps/" + runnerID + "/app" + path
}

func TestTunnelProxyForwardsRequestsIntact(t *testing.T) {
	var mu sync.Mutex
	var got struct {
		method, path, query, route, ctype, body string
	}
	f := newTunnelFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		got.method, got.path, got.query = r.Method, r.URL.Path, r.URL.RawQuery
		got.route, got.ctype, got.body = r.Header.Get("Livepeer-Runner-Route"), r.Header.Get("Content-Type"), string(body)
		mu.Unlock()
		w.Header().Set("X-App", "yes")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("app says " + string(body)))
	}), 4)
	runnerID := f.waitListed(t, "")

	res, err := http.Post(f.appURL(runnerID, "/v1/chat/completions?stream=false"), "application/json", strings.NewReader(`{"model":"m"}`))
	require.NoError(t, err)
	defer res.Body.Close()
	out, _ := io.ReadAll(res.Body)
	require.Equal(t, http.StatusCreated, res.StatusCode)
	require.Equal(t, "yes", res.Header.Get("X-App"))
	require.Equal(t, `app says {"model":"m"}`, string(out))
	mu.Lock()
	seen := got
	mu.Unlock()
	require.Equal(t, http.MethodPost, seen.method)
	require.Equal(t, "/v1/chat/completions", seen.path)
	require.Equal(t, "stream=false", seen.query)
	require.Equal(t, runnerID, seen.route)
	require.Equal(t, "application/json", seen.ctype)
	require.Equal(t, `{"model":"m"}`, seen.body)

	res, err = http.Get(f.appURL(runnerID, "/v1/models"))
	require.NoError(t, err)
	res.Body.Close()
	require.Equal(t, http.StatusCreated, res.StatusCode)
}

func TestTunnelProxyStreamsSSEIncrementally(t *testing.T) {
	release := make(chan struct{})
	f := newTunnelFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		_, _ = io.WriteString(w, "data: first\n\n")
		flusher.Flush()
		<-release
		_, _ = io.WriteString(w, "data: second\n\n")
		flusher.Flush()
	}), 4)
	runnerID := f.waitListed(t, "")

	res, err := http.Get(f.appURL(runnerID, "/v1/chat/completions"))
	require.NoError(t, err)
	defer res.Body.Close()
	require.Equal(t, http.StatusOK, res.StatusCode)
	buf := make([]byte, 64)
	n, err := res.Body.Read(buf)
	require.NoError(t, err)
	require.Equal(t, "data: first\n\n", string(buf[:n]), "the first event arrives before the handler finishes")
	close(release)
	rest, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, "data: second\n\n", string(rest))
}

func TestTunnelProxyCarriesA50MBMultipartUpload(t *testing.T) {
	var mu sync.Mutex
	var gotSum string
	var gotSize int64
	f := newTunnelFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reader, err := r.MultipartReader()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		h := sha256.New()
		for {
			part, err := reader.NextPart()
			if err == io.EOF {
				break
			}
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if part.FormName() == "file" {
				n, _ := io.Copy(h, part)
				mu.Lock()
				gotSize = n
				mu.Unlock()
			}
		}
		mu.Lock()
		gotSum = hex.EncodeToString(h.Sum(nil))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}), 4)
	runnerID := f.waitListed(t, "")

	const size = 50 << 20
	payload := make([]byte, size)
	_, err := rand.Read(payload)
	require.NoError(t, err)
	want := sha256.Sum256(payload)

	pr, pw := io.Pipe()
	writer := multipart.NewWriter(pw)
	go func() {
		part, _ := writer.CreateFormFile("file", "audio.wav")
		_, _ = part.Write(payload)
		_ = writer.Close()
		_ = pw.Close()
	}()
	req, err := http.NewRequest(http.MethodPost, f.appURL(runnerID, "/v1/audio/transcriptions"), pr)
	require.NoError(t, err)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	res.Body.Close()
	require.Equal(t, http.StatusOK, res.StatusCode)
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, int64(size), gotSize)
	require.Equal(t, hex.EncodeToString(want[:]), gotSum)
}

func TestTunnelProxyServes64ConcurrentRequests(t *testing.T) {
	f := newTunnelFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		_, _ = io.WriteString(w, r.URL.Query().Get("n"))
	}), 64)
	runnerID := f.waitListed(t, "")

	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := http.Get(f.appURL(runnerID, fmt.Sprintf("/v1/x?n=%d", i)))
			if err != nil {
				errs <- err
				return
			}
			defer res.Body.Close()
			body, _ := io.ReadAll(res.Body)
			if res.StatusCode != http.StatusOK || string(body) != fmt.Sprint(i) {
				errs <- fmt.Errorf("request %d: status %d body %q", i, res.StatusCode, body)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestTunnelProxyPassesWebSocketUpgrades(t *testing.T) {
	upgrader := websocket.Upgrader{}
	f := newTunnelFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()
		for {
			mt, msg, err := ws.ReadMessage()
			if err != nil {
				return
			}
			_ = ws.WriteMessage(mt, append([]byte("echo: "), msg...))
		}
	}), 4)
	runnerID := f.waitListed(t, "")

	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(f.appURL(runnerID, "/v1/realtime"), "http"), nil)
	require.NoError(t, err)
	defer ws.Close()
	require.NoError(t, ws.WriteMessage(websocket.TextMessage, []byte("hello")))
	_, msg, err := ws.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, "echo: hello", string(msg))
}

func TestTunnelDropAndReconnectKeepTheRegistration(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	f := newTunnelFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/stream" {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: first\n\n")
			w.(http.Flusher).Flush()
			<-release
			return
		}
		_, _ = io.WriteString(w, "ok")
	}), 4)
	runnerID := f.waitListed(t, "")

	// A stream in flight when the tunnel drops ends with an error to its client.
	res, err := http.Get(f.appURL(runnerID, "/stream"))
	require.NoError(t, err)
	buf := make([]byte, 64)
	_, err = res.Body.Read(buf)
	require.NoError(t, err)
	f.client.DropTunnel()
	_, err = io.ReadAll(res.Body)
	require.Error(t, err, "the in-flight stream fails")
	res.Body.Close()
	once.Do(func() { close(release) })

	// Gone from discovery, then back under the same id.
	f.waitUnlisted(t)
	again := f.waitListed(t, "")
	require.Equal(t, runnerID, again, "reconnect keeps the runner id")
	res, err = http.Get(f.appURL(runnerID, "/after"))
	require.NoError(t, err)
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()
	require.Equal(t, "ok", string(body))
}

func TestTunnelExpiredRegistrationReBootstraps(t *testing.T) {
	f := newTunnelFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}), 4)
	runnerID := f.waitListed(t, "")
	_, secret := f.client.Registration()

	// The orchestrator forgets the runner (as expiry after a long outage would).
	manager, ok := f.lp.liveRunnerManager()
	require.True(t, ok)
	require.NoError(t, manager.Unregister(runnerID, secret))
	f.waitUnlisted(t)

	// The next heartbeat is refused, the client re-registers under a new id.
	again := f.waitListed(t, runnerID)
	require.NotEqual(t, runnerID, again)
	res, err := http.Get(f.appURL(again, "/v1/x"))
	require.NoError(t, err)
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()
	require.Equal(t, "ok", string(body))
}

func TestTunnelRouteRefusesBadCredentials(t *testing.T) {
	lp := newLiveRunnerHTTP(t, true)
	orch := httptest.NewServer(lp)
	defer orch.Close()

	res, err := http.Get(orch.URL + "/runners/nobody/tunnel")
	require.NoError(t, err)
	res.Body.Close()
	require.Equal(t, http.StatusNotFound, res.StatusCode)

	body, _ := json.Marshal(map[string]any{"app": "test/app", "capacity": 1, "tunnel": true, "mode": "single-shot"})
	req, _ := http.NewRequest(http.MethodPost, orch.URL+"/runners/heartbeat", bytes.NewReader(body))
	req.Header.Set("Authorization", lp.orchestrator.RegistrationSecret())
	res, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	var reg struct {
		RunnerID string `json:"runner_id"`
	}
	require.NoError(t, json.NewDecoder(res.Body).Decode(&reg))
	res.Body.Close()

	req, _ = http.NewRequest(http.MethodGet, orch.URL+"/runners/"+reg.RunnerID+"/tunnel", nil)
	req.Header.Set("Authorization", "wrong")
	res, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	res.Body.Close()
	require.Equal(t, http.StatusUnauthorized, res.StatusCode)
}
