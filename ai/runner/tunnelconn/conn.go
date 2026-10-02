// Package tunnelconn carries a Live Runner tunnel: a yamux session over the
// binary frames of one WebSocket, shared by the orchestrator (the accepting
// side, which opens streams) and the runner-side command (the dialing side,
// which serves them). It is deliberately small and imports none of the node.
package tunnelconn

import (
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hashicorp/yamux"
)

const (
	// Protocol is the WebSocket subprotocol both sides offer.
	Protocol = "livepeer-runner-tunnel/1"
	// KeepAliveInterval is how often each side pings the other over the session.
	KeepAliveInterval = 10 * time.Second
	// KeepAliveTimeout bounds the wait for a ping's answer (and any other
	// session write); with the interval above, a silent drop is noticed
	// within 30 seconds.
	KeepAliveTimeout = 20 * time.Second
)

// YamuxConfig is the session configuration both sides use.
func YamuxConfig(logOutput io.Writer) *yamux.Config {
	cfg := yamux.DefaultConfig()
	cfg.EnableKeepAlive = true
	cfg.KeepAliveInterval = KeepAliveInterval
	cfg.ConnectionWriteTimeout = KeepAliveTimeout
	if logOutput == nil {
		logOutput = io.Discard
	}
	cfg.LogOutput = logOutput
	return cfg
}

// Conn adapts a WebSocket to net.Conn: every Write is one binary message,
// Read drains messages in order. yamux drives the deadlines and framing.
type Conn struct {
	ws        *websocket.Conn
	reader    io.Reader
	writeMu   sync.Mutex
	closeOnce sync.Once
	closeErr  error
}

// NewConn wraps an upgraded or dialed WebSocket.
func NewConn(ws *websocket.Conn) *Conn {
	return &Conn{ws: ws}
}

func (c *Conn) Read(p []byte) (int, error) {
	for {
		if c.reader == nil {
			messageType, r, err := c.ws.NextReader()
			if err != nil {
				return 0, translateWSError(err)
			}
			if messageType != websocket.BinaryMessage {
				// Text frames are not part of the protocol; skip them.
				_, _ = io.Copy(io.Discard, r)
				continue
			}
			c.reader = r
		}
		n, err := c.reader.Read(p)
		if errors.Is(err, io.EOF) {
			c.reader = nil
			if n == 0 {
				continue
			}
			return n, nil
		}
		if err != nil {
			return n, translateWSError(err)
		}
		return n, nil
	}
}

func (c *Conn) Write(p []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := c.ws.WriteMessage(websocket.BinaryMessage, p); err != nil {
		return 0, translateWSError(err)
	}
	return len(p), nil
}

// Close sends a close frame on a best-effort basis, then closes the socket.
func (c *Conn) Close() error {
	c.closeOnce.Do(func() {
		c.writeMu.Lock()
		_ = c.ws.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
		c.writeMu.Unlock()
		c.closeErr = c.ws.Close()
	})
	return c.closeErr
}

func (c *Conn) LocalAddr() net.Addr  { return c.ws.LocalAddr() }
func (c *Conn) RemoteAddr() net.Addr { return c.ws.RemoteAddr() }

func (c *Conn) SetDeadline(t time.Time) error {
	if err := c.ws.SetReadDeadline(t); err != nil {
		return err
	}
	return c.ws.SetWriteDeadline(t)
}

func (c *Conn) SetReadDeadline(t time.Time) error  { return c.ws.SetReadDeadline(t) }
func (c *Conn) SetWriteDeadline(t time.Time) error { return c.ws.SetWriteDeadline(t) }

func translateWSError(err error) error {
	var closeErr *websocket.CloseError
	if errors.As(err, &closeErr) {
		return io.EOF
	}
	if errors.Is(err, net.ErrClosed) {
		return io.EOF
	}
	return err
}

var _ net.Conn = (*Conn)(nil)
