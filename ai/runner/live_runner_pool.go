package runner

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"

	"github.com/livepeer/go-livepeer/core"
)

// Pool mode lets people other than the operator ("members") attach Live
// Runners. A pool service outside go-livepeer decides who may register and in
// which state; the registry keeps what that service answered for as long as
// the runner lives and stores nothing else about members. See
// doc/live-runner-pool.md.

// A runner's state says whether the orchestrator will give it work. It is
// separate from the status the runner reports, which says whether it can take
// any.
const (
	// LiveRunnerStateProbation runners are registered but invisible to customers.
	LiveRunnerStateProbation = "probation"
	// LiveRunnerStateActive runners take customer work.
	LiveRunnerStateActive = "active"
	// LiveRunnerStateSuspended runners keep their registration and finish the
	// sessions they have, but take no new work.
	LiveRunnerStateSuspended = "suspended"
)

const (
	// liveRunnerProbeSessionTTL bounds a probe session, so a pool service that
	// dies mid-test does not hold a runner's capacity for good.
	liveRunnerProbeSessionTTL         = 10 * time.Minute
	defaultLiveRunnerAdmissionTimeout = 5 * time.Second
	maxLiveRunnerAdmissionBody        = 64 << 10
	maxLiveRunnerMemberIDBytes        = 128
)

// LiveRunnerAdmitter decides whether a runner may join the pool. A returned
// error means no decision could be reached; the registration is then refused.
type LiveRunnerAdmitter interface {
	Admit(req LiveRunnerAdmissionRequest) (*LiveRunnerAdmissionResponse, error)
}

// LiveRunnerAdmissionRequest is what the admission webhook receives.
type LiveRunnerAdmissionRequest struct {
	Event       string                    `json:"event"`
	OrchAddress string                    `json:"orch_address"`
	RemoteAddr  string                    `json:"remote_addr"`
	Credential  string                    `json:"credential"`
	Runner      LiveRunnerAdmissionRunner `json:"runner"`
}

// LiveRunnerAdmissionRunner describes the runner asking to register, after
// the registry has normalized its heartbeat and assigned its id.
type LiveRunnerAdmissionRunner struct {
	RunnerID string         `json:"runner_id"`
	Tunnel   bool           `json:"tunnel"`
	Label    string         `json:"label"`
	Version  string         `json:"version"`
	Metadata string         `json:"metadata"`
	Status   string         `json:"status"`
	Mode     string         `json:"mode"`
	GPU      *LiveRunnerGPU `json:"gpu,omitempty"`
	App      string         `json:"app"`
	Capacity int            `json:"capacity"`
}

// LiveRunnerAdmissionResponse is the admission webhook's answer.
type LiveRunnerAdmissionResponse struct {
	Allow         bool   `json:"allow"`
	Reason        string `json:"reason,omitempty"`
	MemberID      string `json:"member_id,omitempty"`
	PayoutAddress string `json:"payout_address,omitempty"`
	MaxRunners    int    `json:"max_runners,omitempty"`
	State         string `json:"state,omitempty"`
}

// liveRunnerMember is what the pool service said about a runner's owner when
// the runner registered. It never changes afterwards.
type liveRunnerMember struct {
	id            string
	payoutAddress string
	maxRunners    int
	state         string // the state the runner starts in
}

// LiveRunnerWebhookAdmitter asks an HTTP endpoint for the admission decision.
type LiveRunnerWebhookAdmitter struct {
	url    string
	secret string
	client *http.Client
}

// NewLiveRunnerWebhookAdmitter returns an admitter that POSTs each
// registration to rawURL. A non-empty secret is sent as a bearer token so the
// pool service can tell the orchestrator from anyone else.
func NewLiveRunnerWebhookAdmitter(rawURL, secret string, timeout time.Duration) (*LiveRunnerWebhookAdmitter, error) {
	u, err := url.ParseRequestURI(strings.TrimSpace(rawURL))
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, fmt.Errorf("must be an absolute http or https URL")
	}
	if timeout <= 0 {
		timeout = defaultLiveRunnerAdmissionTimeout
	}
	return &LiveRunnerWebhookAdmitter{
		url:    u.String(),
		secret: secret,
		client: &http.Client{Timeout: timeout},
	}, nil
}

func (a *LiveRunnerWebhookAdmitter) Admit(req LiveRunnerAdmissionRequest) (*LiveRunnerAdmissionResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequest(http.MethodPost, a.url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if a.secret != "" {
		httpReq.Header.Set("Authorization", "Bearer "+a.secret)
	}
	res, err := a.client.Do(httpReq)
	if err != nil {
		// The error text of a failed request names the URL but never the body,
		// so the credential stays out of the logs.
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("admission webhook answered %d", res.StatusCode)
	}
	var out LiveRunnerAdmissionResponse
	if err := json.NewDecoder(io.LimitReader(res.Body, maxLiveRunnerAdmissionBody)).Decode(&out); err != nil {
		return nil, fmt.Errorf("admission webhook response: %w", err)
	}
	return &out, nil
}

// poolMode says whether registrations are decided by an admitter.
func (r *LiveRunnerRegistry) poolMode() bool {
	return r.admitter != nil
}

// admit runs the admission decision for a registration heartbeat. It must be
// called without the registry lock: the admitter makes a network call.
func (r *LiveRunnerRegistry) admit(req LiveRunnerHeartbeatRequest, credential, remoteAddr string) (*liveRunnerMember, error) {
	if strings.TrimSpace(credential) == "" {
		return nil, &RunnerError{StatusCode: http.StatusUnauthorized, Message: "registration requires a credential"}
	}
	if !req.Tunnel {
		// A member-supplied runner_url would let a member aim the orchestrator's
		// proxy at any address the orchestrator can reach.
		return nil, &RunnerError{StatusCode: http.StatusBadRequest, Message: "pool runners must register with a tunnel"}
	}
	resp, err := r.admitter.Admit(LiveRunnerAdmissionRequest{
		Event:       "register",
		OrchAddress: r.orchAddress,
		RemoteAddr:  remoteAddr,
		Credential:  credential,
		Runner: LiveRunnerAdmissionRunner{
			RunnerID: req.RunnerID,
			Tunnel:   req.Tunnel,
			Label:    req.Label,
			Version:  req.Version,
			Metadata: req.Metadata,
			Status:   req.Status,
			Mode:     req.Mode,
			GPU:      cloneLiveRunnerGPU(req.GPU),
			App:      req.App,
			Capacity: req.Capacity,
		},
	})
	if err != nil || resp == nil {
		slog.Error("live runner admission unavailable", "runner_id", req.RunnerID, "app", req.App, "remote_addr", remoteAddr, "err", err)
		return nil, &RunnerError{StatusCode: http.StatusServiceUnavailable, Message: "admission unavailable"}
	}
	if !resp.Allow {
		reason := strings.TrimSpace(resp.Reason)
		if reason == "" {
			reason = "registration denied"
		}
		slog.Info("live runner registration denied", "runner_id", req.RunnerID, "app", req.App, "remote_addr", remoteAddr, "reason", reason)
		return nil, &RunnerError{StatusCode: http.StatusForbidden, Message: reason}
	}
	member, err := memberFromAdmission(resp)
	if err != nil {
		slog.Error("live runner admission response is malformed", "runner_id", req.RunnerID, "app", req.App, "remote_addr", remoteAddr, "err", err)
		return nil, &RunnerError{StatusCode: http.StatusServiceUnavailable, Message: "admission unavailable"}
	}
	return member, nil
}

func memberFromAdmission(resp *LiveRunnerAdmissionResponse) (*liveRunnerMember, error) {
	member := &liveRunnerMember{
		id:            strings.TrimSpace(resp.MemberID),
		payoutAddress: strings.TrimSpace(resp.PayoutAddress),
		maxRunners:    resp.MaxRunners,
		state:         strings.TrimSpace(resp.State),
	}
	if member.id == "" {
		return nil, fmt.Errorf("member_id is required")
	}
	if len(member.id) > maxLiveRunnerMemberIDBytes {
		return nil, fmt.Errorf("member_id is longer than %d bytes", maxLiveRunnerMemberIDBytes)
	}
	if member.payoutAddress != "" && !ethcommon.IsHexAddress(member.payoutAddress) {
		return nil, fmt.Errorf("payout_address is not an address")
	}
	if member.maxRunners < 0 {
		return nil, fmt.Errorf("max_runners must not be negative")
	}
	if member.maxRunners == 0 {
		member.maxRunners = 1
	}
	if member.state == "" {
		member.state = LiveRunnerStateProbation
	}
	if !validLiveRunnerState(member.state) {
		return nil, fmt.Errorf("state %q is not one of probation, active, suspended", member.state)
	}
	return member, nil
}

func validLiveRunnerState(state string) bool {
	switch state {
	case LiveRunnerStateProbation, LiveRunnerStateActive, LiveRunnerStateSuspended:
		return true
	}
	return false
}

// newPoolRunnerID returns an id no registered runner has. Pool runners never
// choose their own.
func (r *LiveRunnerRegistry) newPoolRunnerID() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	for {
		id := "runner_" + randomStr(liveRunnerIDRandomBytes)
		if r.runners[id] == nil {
			return id
		}
	}
}

// memberWithinLimitLocked checks the member's runner limit. It requires r.mu,
// which is what makes two concurrent registrations count each other: the
// runner is added to the registry under the same lock.
func (r *LiveRunnerRegistry) memberWithinLimitLocked(member *liveRunnerMember) error {
	count := 0
	for _, other := range r.runners {
		// member is set before a runner is added and never changes, so it can
		// be read without the runner's lock.
		if other.member != nil && other.member.id == member.id {
			count++
		}
	}
	if count >= member.maxRunners {
		return &RunnerError{StatusCode: http.StatusTooManyRequests, Message: "member runner limit"}
	}
	return nil
}

// frozenFieldsLocked refuses a heartbeat that would change what a pool runner
// was admitted as. Only its status, its session ids, and a capacity up to the
// one it registered with may change. It requires runner.mu.
func (runner *liveRunner) frozenFieldsLocked(req LiveRunnerHeartbeatRequest) error {
	changed := ""
	switch {
	case req.App != runner.App:
		changed = "app"
	case req.Mode != runner.Mode:
		changed = "mode"
	case req.Version != runner.Version:
		changed = "version"
	case req.Label != runner.Label:
		changed = "label"
	case req.Proxy != runner.Proxy:
		changed = "proxy"
	case req.Metadata != runner.Metadata:
		changed = "metadata"
	case !sameLiveRunnerGPU(req.GPU, runner.GPU):
		changed = "gpu"
	}
	if changed != "" {
		return &RunnerError{StatusCode: http.StatusBadRequest, Message: changed + " cannot change after registration"}
	}
	if req.Capacity > runner.registeredCapacity {
		return &RunnerError{StatusCode: http.StatusBadRequest, Message: "capacity cannot exceed the registered capacity"}
	}
	return nil
}

func sameLiveRunnerGPU(a, b *LiveRunnerGPU) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// stateLocked is the runner's state; a runner outside pool mode is always
// active. It requires runner.mu.
func (runner *liveRunner) stateLocked() string {
	if runner.state == "" {
		return LiveRunnerStateActive
	}
	return runner.state
}

// newWorkAllowedLocked says whether a customer may start work on the runner.
// A runner on probation answers as if it did not exist. It requires runner.mu.
func (runner *liveRunner) newWorkAllowedLocked() error {
	switch runner.stateLocked() {
	case LiveRunnerStateProbation:
		return &RunnerError{StatusCode: http.StatusNotFound, Message: "runner not found"}
	case LiveRunnerStateSuspended:
		return &RunnerError{StatusCode: http.StatusConflict, Message: "runner suspended"}
	}
	return nil
}

// LiveRunnerPriceTable is the operator's price per app: the content of the
// -liveRunnerPrices file. A runner of a listed app is priced from the table,
// whatever its heartbeat says, and a pool runner may only serve a listed app.
type LiveRunnerPriceTable struct {
	Apps map[string]LiveRunnerPriceInfo `json:"apps"`
}

// ParseLiveRunnerPrices reads a price table, returning the first entry that
// does not validate.
func ParseLiveRunnerPrices(data []byte) (LiveRunnerPriceTable, error) {
	var table LiveRunnerPriceTable
	if err := json.Unmarshal(data, &table); err != nil {
		return LiveRunnerPriceTable{}, err
	}
	return normalizeLiveRunnerPrices(table)
}

func normalizeLiveRunnerPrices(table LiveRunnerPriceTable) (LiveRunnerPriceTable, error) {
	apps := make([]string, 0, len(table.Apps))
	for app := range table.Apps {
		apps = append(apps, app)
	}
	sort.Strings(apps) // so the first error reported does not depend on map order
	normalized := LiveRunnerPriceTable{Apps: make(map[string]LiveRunnerPriceInfo, len(apps))}
	for _, app := range apps {
		if app == "" || app != strings.TrimSpace(app) {
			return LiveRunnerPriceTable{}, fmt.Errorf("apps[%q]: app must be trimmed and not empty", app)
		}
		priceInfo, err := normalizeLiveRunnerPriceInfo(table.Apps[app])
		if err != nil {
			return LiveRunnerPriceTable{}, fmt.Errorf("apps[%q]: %v", app, err)
		}
		normalized.Apps[app] = priceInfo
	}
	return normalized, nil
}

// SetPricesJSON replaces the price table with the one in data, or changes
// nothing when data does not validate.
func (r *LiveRunnerRegistry) SetPricesJSON(data []byte) error {
	table, err := ParseLiveRunnerPrices(data)
	if err != nil {
		return &RunnerError{StatusCode: http.StatusBadRequest, Message: err.Error()}
	}
	r.setPrices(table)
	return nil
}

// SetPrices replaces the whole price table. Registered runners of a listed
// app take the new price at once, which matters for static runners, which
// never heartbeat; sessions already running keep the price they started at.
// A runner whose app left the table keeps the price it has.
func (r *LiveRunnerRegistry) SetPrices(table LiveRunnerPriceTable) error {
	table, err := normalizeLiveRunnerPrices(table)
	if err != nil {
		return &RunnerError{StatusCode: http.StatusBadRequest, Message: err.Error()}
	}
	r.setPrices(table)
	return nil
}

func (r *LiveRunnerRegistry) setPrices(table LiveRunnerPriceTable) {
	r.pricesMu.Lock()
	r.prices = table.Apps
	r.pricesMu.Unlock()

	r.mu.Lock()
	snapshot := make([]*liveRunner, 0, len(r.runners))
	for _, runner := range r.runners {
		snapshot = append(snapshot, runner)
	}
	r.mu.Unlock()
	for _, runner := range snapshot {
		runner.mu.Lock()
		if price, ok := table.Apps[runner.App]; ok && !runner.removed && !runner.offchain && runner.PriceInfo != price {
			runner.PriceInfo = price
			runner.updatePriceConverterLocked()
		}
		runner.mu.Unlock()
	}
	slog.Info("live runner prices set", "apps", len(table.Apps))
}

// Prices returns a copy of the price table.
func (r *LiveRunnerRegistry) Prices() LiveRunnerPriceTable {
	r.pricesMu.RLock()
	defer r.pricesMu.RUnlock()
	apps := make(map[string]LiveRunnerPriceInfo, len(r.prices))
	for app, price := range r.prices {
		apps[app] = price
	}
	return LiveRunnerPriceTable{Apps: apps}
}

// configuredPrice is the operator's price for an app, if it set one.
func (r *LiveRunnerRegistry) configuredPrice(app string) (LiveRunnerPriceInfo, bool) {
	r.pricesMu.RLock()
	defer r.pricesMu.RUnlock()
	price, ok := r.prices[app]
	return price, ok
}

// PoolMode says whether registrations are decided by an admitter.
func (r *LiveRunnerRegistry) PoolMode() bool {
	return r.poolMode()
}

// ProbeRunnerMode is RunnerMode for the pool service's probe, which reaches a
// runner in any state.
func (r *LiveRunnerRegistry) ProbeRunnerMode(runnerID string) (string, error) {
	return r.runnerMode(runnerID, true)
}

// ReserveProbeSession reserves an unpaid session for the pool service's test
// on a runner in any state. It is an ordinary session otherwise: it counts
// against capacity and is proxied and released the same way. Its id has the
// shape a customer's would, so the runner cannot tell a test from real work.
func (r *LiveRunnerRegistry) ReserveProbeSession(runnerID string) (string, string, error) {
	if r.offchain {
		// Off-chain customer sessions are unpaid too and get a generated id.
		return r.reserveSession(runnerID, true)
	}
	// On-chain a customer's session id is the payment's manifest id.
	return r.reserveSession(runnerID, true, string(core.RandomManifestID()))
}

// releaseExpiredProbeSessions releases probe sessions nobody stopped.
func (r *LiveRunnerRegistry) releaseExpiredProbeSessions(now time.Time) {
	r.mu.Lock()
	snapshot := make([]*liveRunner, 0, len(r.runners))
	for _, runner := range r.runners {
		snapshot = append(snapshot, runner)
	}
	r.mu.Unlock()
	for _, runner := range snapshot {
		runner.mu.Lock()
		for sessionID, session := range runner.sessions {
			if session.probe && now.Sub(session.createdAt) > liveRunnerProbeSessionTTL {
				slog.Warn("live runner probe session expired", "runner_id", runner.RunnerID, "session_id", sessionID)
				runner.releaseSessionLocked(sessionID)
			}
		}
		runner.mu.Unlock()
	}
}
