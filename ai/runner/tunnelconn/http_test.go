package tunnelconn

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hashicorp/yamux"
	"github.com/stretchr/testify/require"
)

// HTTP served by the dialing side over streams the accepting side opens:
// the exact arrangement of orchestrator (opens) and runner command (serves).
func TestHTTPOverTunnelStreams(t *testing.T) {
	upgrader := websocket.Upgrader{Subprotocols: []string{Protocol}}
	accepted := make(chan *yamux.Session, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		require.NoError(t, err)
		session, err := yamux.Server(NewConn(ws), YamuxConfig(nil))
		require.NoError(t, err)
		accepted <- session
	}))
	defer srv.Close()
	dialer := websocket.Dialer{Subprotocols: []string{Protocol}}
	ws, _, err := dialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	require.NoError(t, err)
	client, err := yamux.Client(NewConn(ws), YamuxConfig(nil))
	require.NoError(t, err)
	server := <-accepted

	app := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "hello "+r.URL.Path)
	})}
	go func() { _ = app.Serve(client) }()
	defer app.Close()

	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return server.OpenStream()
		},
		DisableKeepAlives: true,
	}
	httpClient := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	for i := 0; i < 3; i++ {
		res, err := httpClient.Get("http://tunnel/x")
		require.NoError(t, err, "request %d", i)
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		require.Equal(t, "hello /x", string(body))
		res, err = httpClient.Post("http://tunnel/y", "text/plain", strings.NewReader("body"))
		require.NoError(t, err, "post %d", i)
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		require.Equal(t, "hello /y", string(body))
	}
}
