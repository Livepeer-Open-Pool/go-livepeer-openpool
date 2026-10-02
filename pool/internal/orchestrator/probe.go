package orchestrator

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Livepeer-Open-Pool/pool/internal/core"
)

// probeHeader carries the pool secret on a probe: an unpaid session on one
// named runner, in any state. The orchestrator strips it before the request
// reaches the runner, which therefore cannot tell a test from paid work.
const probeHeader = "Livepeer-Probe-Secret"

// maxProbeBody is how much of an answer a test reads to check it.
const maxProbeBody = 1 << 20

// Prober runs an app's test against a runner through the orchestrator's
// public port.
type Prober struct {
	base   *url.URL
	secret string
	http   *http.Client
}

// NewProber returns a prober for the orchestrator at serviceURL, presenting
// poolSecret. insecureTLS skips certificate verification, for an
// orchestrator that serves its default self-signed certificate.
func NewProber(serviceURL, poolSecret string, insecureTLS bool) (*Prober, error) {
	u, err := url.Parse(strings.TrimSpace(serviceURL))
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, fmt.Errorf("orchestrator URL %q must be an http(s) URL", serviceURL)
	}
	if poolSecret == "" {
		return nil, fmt.Errorf("probing needs the pool secret")
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: insecureTLS}} //nolint:gosec // opt-in, for self-signed orchestrators
	// No client timeout: each test sets its own limit through its context.
	return &Prober{base: u, secret: poolSecret, http: &http.Client{Transport: transport}}, nil
}

func failed(format string, args ...any) core.TestOutcome {
	return core.TestOutcome{Detail: fmt.Sprintf(format, args...)}
}

// Probe runs test against runner. A single-shot runner gets the request
// directly; a persistent one gets a session first, the request inside it,
// and the session is stopped afterwards.
func (p *Prober) Probe(ctx context.Context, runner core.OrchRunner, test core.Test) core.TestOutcome {
	limit := time.Duration(test.TimeoutMs) * time.Millisecond
	appBase := p.base.JoinPath("apps", runner.RunnerID, "app")
	sendSecret := true
	if runner.Mode == core.ModePersistent {
		session, err := p.openSession(ctx, runner.RunnerID)
		if err != nil {
			return failed("could not open a session: %v", err)
		}
		// Stop the session however the test goes; an abandoned one would
		// hold a slot until the orchestrator expires it.
		defer p.stopSession(runner.RunnerID, session)
		appBase = p.base.JoinPath("apps", runner.RunnerID, "session", session, "app")
		sendSecret = false // the session is already open; requests inside it are ordinary
	}

	target, err := url.Parse(strings.TrimRight(appBase.String(), "/") + test.Path)
	if err != nil {
		return failed("the test path is not usable: %v", err)
	}
	ctx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	var body io.Reader
	if test.Body != "" {
		body = strings.NewReader(test.Body)
	}
	req, err := http.NewRequestWithContext(ctx, test.Method, target.String(), body)
	if err != nil {
		return failed("the test request is not usable: %v", err)
	}
	for name, value := range test.Headers {
		req.Header.Set(name, value)
	}
	if sendSecret {
		req.Header.Set(probeHeader, p.secret)
	}

	started := time.Now()
	res, err := p.http.Do(req)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return core.TestOutcome{LatencyMs: time.Since(started).Milliseconds(), Detail: fmt.Sprintf("no answer within %s", limit)}
		}
		return core.TestOutcome{LatencyMs: time.Since(started).Milliseconds(), Detail: fmt.Sprintf("request failed: %v", err)}
	}
	defer res.Body.Close()
	answer, err := io.ReadAll(io.LimitReader(res.Body, maxProbeBody))
	outcome := core.TestOutcome{LatencyMs: time.Since(started).Milliseconds()}
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			outcome.Detail = fmt.Sprintf("answer not finished within %s", limit)
		} else {
			outcome.Detail = fmt.Sprintf("answer cut short: %v", err)
		}
		return outcome
	}
	if res.StatusCode != test.ExpectStatus {
		outcome.Detail = fmt.Sprintf("answered %d, expected %d: %s", res.StatusCode, test.ExpectStatus, excerpt(answer))
		return outcome
	}
	for _, want := range test.ExpectContains {
		if !strings.Contains(string(answer), want) {
			outcome.Detail = fmt.Sprintf("answer does not contain %q: %s", want, excerpt(answer))
			return outcome
		}
	}
	outcome.OK = true
	return outcome
}

// excerpt is the start of an answer, for a failure's detail.
func excerpt(answer []byte) string {
	text := strings.TrimSpace(string(answer))
	if len(text) > 200 {
		text = text[:200] + "…"
	}
	return text
}

func (p *Prober) openSession(ctx context.Context, runnerID string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.base.JoinPath("apps", runnerID, "session").String(), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set(probeHeader, p.secret)
	res, err := p.http.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	payload, _ := io.ReadAll(io.LimitReader(res.Body, maxProbeBody))
	if res.StatusCode != http.StatusOK {
		return "", fmt.Errorf("orchestrator answered %d: %s", res.StatusCode, excerpt(payload))
	}
	var session struct {
		SessionID string `json:"session_id"`
	}
	if err := json.Unmarshal(payload, &session); err != nil || session.SessionID == "" {
		return "", fmt.Errorf("orchestrator's session answer has no session id")
	}
	return session.SessionID, nil
}

func (p *Prober) stopSession(runnerID, sessionID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.base.JoinPath("apps", runnerID, "session", sessionID, "stop").String(), nil)
	if err != nil {
		return
	}
	if res, err := p.http.Do(req); err == nil {
		res.Body.Close()
	}
}
