// Package tunnelclient is the runner side of a Live Runner tunnel: it
// registers an app with an orchestrator as a tunneled runner, opens the
// tunnel, serves the streams the orchestrator opens on it by proxying to a
// local HTTP server, heartbeats, and reconnects. It imports nothing from the
// node so the command built on it stays small.
package tunnelclient

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hashicorp/yamux"

	"github.com/livepeer/go-livepeer/ai/runner/tunnelconn"
)

const (
	defaultHeartbeatInterval = 5 * time.Second
	minBackoff               = time.Second
	maxBackoff               = 30 * time.Second
	stableTunnelAge          = time.Minute
	healthTimeout            = 2 * time.Second
	handshakeTimeout         = 10 * time.Second
	unregisterTimeout        = 2 * time.Second
)

// Config describes one app to expose through a tunnel.
type Config struct {
	// Orchestrator is the orchestrator's service address, https://host:port.
	Orchestrator string
	// OrchSecret is the registration secret (-orchSecret on the orchestrator).
	OrchSecret string
	// Target is the local app, for example http://127.0.0.1:8000.
	Target string
	// Health is a path on Target that answers 200 while the app can take
	// work; empty reports ready without checking.
	Health string

	App      string
	Mode     string
	Label    string
	Version  string
	Metadata string
	Capacity int
	Price    string
	Currency string
	Unit     string
	// InsecureTLS skips certificate verification toward the orchestrator,
	// which serves a self-signed certificate by default.
	InsecureTLS bool

	Logger *slog.Logger
}

type priceInfo struct {
	Price    json.Number `json:"price"`
	Currency string      `json:"currency,omitempty"`
	Unit     string      `json:"unit,omitempty"`
}

type heartbeatRequest struct {
	RunnerID string    `json:"runner_id,omitempty"`
	Label    string    `json:"label,omitempty"`
	Version  string    `json:"version,omitempty"`
	Metadata string    `json:"metadata,omitempty"`
	Status   string    `json:"status,omitempty"`
	Mode     string    `json:"mode,omitempty"`
	App      string    `json:"app"`
	Capacity int       `json:"capacity"`
	Price    priceInfo `json:"price_info"`
	Tunnel   bool      `json:"tunnel"`
}

type heartbeatResponse struct {
	RunnerID          string `json:"runner_id"`
	HeartbeatInterval string `json:"heartbeat_interval"`
	HeartbeatTTL      string `json:"heartbeat_ttl"`
	HeartbeatSecret   string `json:"heartbeat_secret,omitempty"`
}

// Client runs one tunneled registration.
type Client struct {
	cfg    Config
	log    *slog.Logger
	http   *http.Client
	dialer *websocket.Dialer
	proxy  http.Handler
	orch   *url.URL
	target *url.URL

	mu       sync.Mutex
	runnerID string
	secret   string
	interval time.Duration
	session  *yamux.Session
}

// errReregister says the orchestrator no longer knows this registration.
var errReregister = errors.New("registration rejected; re-registering")

// New validates the configuration and prepares the client.
func New(cfg Config) (*Client, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	orch, err := url.Parse(strings.TrimSpace(cfg.Orchestrator))
	if err != nil || orch.Host == "" || (orch.Scheme != "http" && orch.Scheme != "https") {
		return nil, fmt.Errorf("orchestrator must be an http(s) URL")
	}
	target, err := url.Parse(strings.TrimSpace(cfg.Target))
	if err != nil || target.Host == "" || (target.Scheme != "http" && target.Scheme != "https") {
		return nil, fmt.Errorf("target must be an http(s) URL")
	}
	if strings.TrimSpace(cfg.OrchSecret) == "" {
		return nil, fmt.Errorf("orchSecret is required")
	}
	if strings.TrimSpace(cfg.App) == "" {
		return nil, fmt.Errorf("app is required")
	}
	if cfg.Mode == "" {
		cfg.Mode = "single-shot"
	}
	if cfg.Capacity <= 0 {
		cfg.Capacity = 1
	}
	if cfg.Health != "" && !strings.HasPrefix(cfg.Health, "/") {
		return nil, fmt.Errorf("health must be a root-relative path")
	}
	tlsConfig := &tls.Config{InsecureSkipVerify: cfg.InsecureTLS} //nolint:gosec // orchestrators self-sign by default
	proxy := httputil.NewSingleHostReverseProxy(target)
	director := proxy.Director
	logger := cfg.Logger
	proxy.Director = func(req *http.Request) {
		director(req)
		req.Host = target.Host
		logger.Debug("proxying to target", "method", req.Method, "path", req.URL.Path)
	}
	return &Client{
		cfg:    cfg,
		log:    cfg.Logger,
		http:   &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{TLSClientConfig: tlsConfig}},
		dialer: &websocket.Dialer{TLSClientConfig: tlsConfig, HandshakeTimeout: handshakeTimeout, Subprotocols: []string{tunnelconn.Protocol}},
		proxy:  proxy,
		orch:   orch,
		target: target,
	}, nil
}

// RunnerID is the current registration's id, empty before the first one.
func (c *Client) RunnerID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.runnerID
}

// Registration returns the current runner id and heartbeat secret. Tests use
// it to act on the registration from the orchestrator's side.
func (c *Client) Registration() (runnerID, heartbeatSecret string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.runnerID, c.secret
}

// DropTunnel closes the current tunnel session without unregistering, the
// way a network fault would; Run reconnects it.
func (c *Client) DropTunnel() {
	c.mu.Lock()
	session := c.session
	c.mu.Unlock()
	if session != nil {
		_ = session.Close()
	}
}

// Run registers, keeps the tunnel up, and heartbeats until ctx ends. It
// returns nil on a clean stop after unregistering.
func (c *Client) Run(ctx context.Context) error {
	backoff := minBackoff
	for {
		if ctx.Err() != nil {
			return nil
		}
		if c.RunnerID() == "" {
			if err := c.register(ctx); err != nil {
				c.log.Warn("registration failed", "err", err, "retry_in", backoff)
				if !sleep(ctx, backoff) {
					return nil
				}
				backoff = nextBackoff(backoff)
				continue
			}
			backoff = minBackoff
		}
		started := time.Now()
		err := c.serveTunnel(ctx)
		if ctx.Err() != nil {
			c.unregister()
			return nil
		}
		if errors.Is(err, errReregister) {
			c.setRegistration("", "", 0)
			continue
		}
		if time.Since(started) > stableTunnelAge {
			backoff = minBackoff
		}
		c.log.Warn("tunnel ended", "err", err, "retry_in", backoff)
		if !sleep(ctx, backoff) {
			c.unregister()
			return nil
		}
		backoff = nextBackoff(backoff)
	}
}

func (c *Client) register(ctx context.Context) error {
	resp, status, err := c.heartbeat(ctx, "", c.cfg.OrchSecret)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("orchestrator answered %d", status)
	}
	if resp.RunnerID == "" || resp.HeartbeatSecret == "" {
		return fmt.Errorf("registration response lacks runner_id or heartbeat_secret")
	}
	interval := defaultHeartbeatInterval
	if d, err := time.ParseDuration(resp.HeartbeatInterval); err == nil && d > 0 {
		interval = d
	}
	c.setRegistration(resp.RunnerID, resp.HeartbeatSecret, interval)
	c.log.Info("registered", "runner_id", resp.RunnerID, "app", c.cfg.App, "orchestrator", c.orch.String())
	return nil
}

func (c *Client) setRegistration(runnerID, secret string, interval time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.runnerID, c.secret, c.interval = runnerID, secret, interval
}

func (c *Client) registration() (string, string, time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.runnerID, c.secret, c.interval
}

// heartbeat posts one heartbeat; a bootstrap uses the orch secret and no id.
func (c *Client) heartbeat(ctx context.Context, runnerID, auth string) (*heartbeatResponse, int, error) {
	body := heartbeatRequest{
		RunnerID: runnerID,
		Label:    c.cfg.Label,
		Version:  c.cfg.Version,
		Metadata: c.cfg.Metadata,
		Status:   c.status(ctx),
		Mode:     c.cfg.Mode,
		App:      c.cfg.App,
		Capacity: c.cfg.Capacity,
		Price:    priceInfo{Price: json.Number(c.cfg.Price), Currency: c.cfg.Currency, Unit: c.cfg.Unit},
		Tunnel:   true,
	}
	data, err := json.Marshal(body)
	if err != nil {
		return nil, 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.orch.JoinPath("runners", "heartbeat").String(), bytes.NewReader(data))
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", auth)
	res, err := c.http.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer res.Body.Close()
	payload, _ := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if res.StatusCode != http.StatusOK {
		return nil, res.StatusCode, fmt.Errorf("heartbeat: %d %s", res.StatusCode, strings.TrimSpace(string(payload)))
	}
	var resp heartbeatResponse
	if err := json.Unmarshal(payload, &resp); err != nil {
		return nil, res.StatusCode, fmt.Errorf("heartbeat response: %w", err)
	}
	return &resp, res.StatusCode, nil
}

// status is what the heartbeat reports: ready while the local health path
// answers 200 (or when no health path is configured).
func (c *Client) status(ctx context.Context) string {
	if c.cfg.Health == "" {
		return "ready"
	}
	ctx, cancel := context.WithTimeout(ctx, healthTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.target.JoinPath(c.cfg.Health).String(), nil)
	if err != nil {
		return "unavailable"
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return "unavailable"
	}
	_, _ = io.Copy(io.Discard, res.Body)
	res.Body.Close()
	if res.StatusCode == http.StatusOK {
		return "ready"
	}
	return "unavailable"
}

// serveTunnel connects one tunnel and serves it until it ends. Heartbeats
// run alongside; a 401 or 404 on either path returns errReregister.
func (c *Client) serveTunnel(ctx context.Context) error {
	runnerID, secret, interval := c.registration()
	wsURL := *c.orch
	switch wsURL.Scheme {
	case "https":
		wsURL.Scheme = "wss"
	default:
		wsURL.Scheme = "ws"
	}
	wsURL = *wsURL.JoinPath("runners", runnerID, "tunnel")
	header := http.Header{"Authorization": []string{secret}}
	ws, res, err := c.dialer.DialContext(ctx, wsURL.String(), header)
	if err != nil {
		if res != nil && (res.StatusCode == http.StatusUnauthorized || res.StatusCode == http.StatusNotFound) {
			return errReregister
		}
		return fmt.Errorf("tunnel dial: %w", err)
	}
	conn := tunnelconn.NewConn(ws)
	session, err := yamux.Client(conn, tunnelconn.YamuxConfig(io.Discard))
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("tunnel session: %w", err)
	}
	c.log.Info("tunnel connected", "runner_id", runnerID, "orchestrator", c.orch.String())
	c.mu.Lock()
	c.session = session
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		if c.session == session {
			c.session = nil
		}
		c.mu.Unlock()
	}()

	serveCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-serveCtx.Done():
				return
			case <-ticker.C:
			}
			_, status, err := c.heartbeat(serveCtx, runnerID, secret)
			if status == http.StatusUnauthorized || status == http.StatusNotFound {
				cancel(errReregister)
				return
			}
			if err != nil && serveCtx.Err() == nil {
				c.log.Warn("heartbeat failed", "runner_id", runnerID, "err", err)
			}
		}
	}()
	go func() {
		select {
		case <-serveCtx.Done():
		case <-session.CloseChan():
		}
		_ = session.Close()
	}()

	server := &http.Server{Handler: c.proxy}
	err = server.Serve(session)
	_ = server.Close()
	if cause := context.Cause(serveCtx); errors.Is(cause, errReregister) {
		return errReregister
	}
	if ctx.Err() != nil {
		return nil
	}
	if errors.Is(err, http.ErrServerClosed) || err == nil {
		return errors.New("tunnel closed")
	}
	return err
}

func (c *Client) unregister() {
	runnerID, secret, _ := c.registration()
	if runnerID == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), unregisterTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.orch.JoinPath("runners", runnerID, "unregister").String(), nil)
	if err != nil {
		return
	}
	req.Header.Set("Authorization", secret)
	res, err := c.http.Do(req)
	if err != nil {
		c.log.Warn("unregister failed", "runner_id", runnerID, "err", err)
		return
	}
	_, _ = io.Copy(io.Discard, res.Body)
	res.Body.Close()
	c.log.Info("unregistered", "runner_id", runnerID, "status", res.StatusCode)
	c.setRegistration("", "", 0)
}

func nextBackoff(d time.Duration) time.Duration {
	d *= 2
	if d > maxBackoff {
		return maxBackoff
	}
	return d
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
