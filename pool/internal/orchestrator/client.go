// Package orchestrator talks to the orchestrator node this pool service
// belongs to, over the node's CLI port. The endpoints are go-livepeer's
// (doc/live-runner-pool.md there); nothing here imports its code.
package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Livepeer-Open-Pool/pool/internal/core"
)

// Client calls the orchestrator's CLI port.
type Client struct {
	base *url.URL
	http *http.Client
}

// New returns a client for the CLI port at cliURL, e.g. http://127.0.0.1:7935.
func New(cliURL string) (*Client, error) {
	u, err := url.Parse(strings.TrimSpace(cliURL))
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, fmt.Errorf("orchestrator CLI URL %q must be an http(s) URL", cliURL)
	}
	return &Client{base: u, http: &http.Client{Timeout: 10 * time.Second}}, nil
}

// do sends a request and returns the body of a 2xx answer. A 404 or 400 is
// returned as a core error of the matching kind, with the orchestrator's text.
func (c *Client) do(ctx context.Context, method, path, contentType string, body io.Reader) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.base.JoinPath(path).String(), body)
	if err != nil {
		return nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	res, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(res.Body, 16<<20))
	if err != nil {
		return nil, err
	}
	text := strings.TrimSpace(string(payload))
	switch {
	case res.StatusCode >= 200 && res.StatusCode < 300:
		return payload, nil
	case res.StatusCode == http.StatusNotFound:
		return nil, &core.Error{Kind: core.NotFound, Message: text}
	case res.StatusCode == http.StatusBadRequest:
		return nil, &core.Error{Kind: core.Invalid, Message: text}
	default:
		return nil, fmt.Errorf("orchestrator answered %d: %s", res.StatusCode, text)
	}
}

func (c *Client) postForm(ctx context.Context, path string, form url.Values) error {
	_, err := c.do(ctx, http.MethodPost, path, "application/x-www-form-urlencoded", strings.NewReader(form.Encode()))
	return err
}

// Runners lists every runner registered on the orchestrator, in every state.
func (c *Client) Runners(ctx context.Context) ([]core.OrchRunner, error) {
	payload, err := c.do(ctx, http.MethodGet, "liveRunner/runners", "", nil)
	if err != nil {
		return nil, err
	}
	var out struct {
		Runners []core.OrchRunner `json:"runners"`
	}
	if err := json.Unmarshal(payload, &out); err != nil {
		return nil, fmt.Errorf("orchestrator runner list: %w", err)
	}
	return out.Runners, nil
}

// SetState moves a runner between probation, active and suspended.
func (c *Client) SetState(ctx context.Context, runnerID, state, reason string) error {
	return c.postForm(ctx, "liveRunner/setState", url.Values{"runner_id": {runnerID}, "state": {state}, "reason": {reason}})
}

// EvictRunner removes one runner.
func (c *Client) EvictRunner(ctx context.Context, runnerID, reason string) error {
	return c.postForm(ctx, "liveRunner/evict", url.Values{"runner_id": {runnerID}, "reason": {reason}})
}

// EvictMember removes every runner of a member.
func (c *Client) EvictMember(ctx context.Context, memberID, reason string) error {
	return c.postForm(ctx, "liveRunner/evict", url.Values{"member_id": {memberID}, "reason": {reason}})
}

// SetPrices replaces the orchestrator's price table.
func (c *Client) SetPrices(ctx context.Context, prices map[string]core.Price) error {
	type price struct {
		Price    string `json:"price"`
		Currency string `json:"currency"`
		Unit     string `json:"unit"`
	}
	table := struct {
		Apps map[string]price `json:"apps"`
	}{Apps: make(map[string]price, len(prices))}
	for app, p := range prices {
		table.Apps[app] = price{Price: p.Amount, Currency: "usd", Unit: p.Unit}
	}
	body, err := json.Marshal(table)
	if err != nil {
		return err
	}
	_, err = c.do(ctx, http.MethodPost, "liveRunner/prices", "application/json", bytes.NewReader(body))
	return err
}

// CurrentRound is the Livepeer round the orchestrator is in.
func (c *Client) CurrentRound(ctx context.Context) (int64, error) {
	payload, err := c.do(ctx, http.MethodGet, "currentRound", "", nil)
	if err != nil {
		return 0, err
	}
	round, err := strconv.ParseInt(strings.TrimSpace(string(payload)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("orchestrator current round %q is not a number", payload)
	}
	return round, nil
}
