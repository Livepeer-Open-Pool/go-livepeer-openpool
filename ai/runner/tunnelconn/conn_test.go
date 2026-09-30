package tunnelconn

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/hashicorp/yamux"
	"github.com/stretchr/testify/require"
)

// A yamux session over the WebSocket adapter carries streams both ways.
func TestConnCarriesYamuxStreams(t *testing.T) {
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
	require.Equal(t, Protocol, ws.Subprotocol())

	// The server opens a stream; the client accepts it and echoes.
	go func() {
		stream, err := client.Accept()
		if err != nil {
			return
		}
		defer stream.Close()
		_, _ = io.Copy(stream, stream)
	}()
	stream, err := server.OpenStream()
	require.NoError(t, err)
	payload := strings.Repeat("tunnel bytes ", 10_000) // larger than one frame
	go func() {
		_, _ = stream.Write([]byte(payload))
		_ = stream.Close() // yamux half-close: the read side stays open until the peer closes
	}()
	echoed, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.Equal(t, payload, string(echoed))

	require.NoError(t, client.Close())
	<-server.CloseChan()
}
