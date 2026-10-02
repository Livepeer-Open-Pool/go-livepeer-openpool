// Package testkit holds what the pool service's tests share: a stand-in
// orchestrator, a mailbox instead of email, wallets that really sign, and a
// clock that moves when told to.
package testkit

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/stretchr/testify/require"

	"github.com/Livepeer-Open-Pool/pool/internal/core"
	"github.com/Livepeer-Open-Pool/pool/internal/store"
)

// OpenDB opens a fresh database for a test.
func OpenDB(t *testing.T) *store.DB {
	t.Helper()
	db, err := store.Open(filepath.Join(t.TempDir(), "pool.db"))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

// Clock is a clock tests move by hand.
type Clock struct {
	mu  sync.Mutex
	now time.Time
}

// NewClock starts at a fixed, arbitrary moment.
func NewClock() *Clock {
	return &Clock{now: time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)}
}

// Now is the clock's time.
func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the clock forward.
func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// Wallet is an Ethereum key that signs the way a browser wallet does.
type Wallet struct {
	key     *ecdsa.PrivateKey
	Address string
}

// NewWallet generates a wallet.
func NewWallet(t *testing.T) *Wallet {
	t.Helper()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	return &Wallet{key: key, Address: crypto.PubkeyToAddress(key.PublicKey).Hex()}
}

// Sign signs message with personal_sign, returning the hex signature with
// the recovery byte as wallets send it (27 or 28).
func (w *Wallet) Sign(t *testing.T, message string) string {
	t.Helper()
	sig, err := crypto.Sign(accounts.TextHash([]byte(message)), w.key)
	require.NoError(t, err)
	sig[crypto.RecoveryIDOffset] += 27
	return hexutil.Encode(sig)
}

// SignInMessage is a "Sign-In with Ethereum" message for the wallet.
func (w *Wallet) SignInMessage(domain string, chainID int, nonce string, issuedAt time.Time) string {
	return fmt.Sprintf(`%s wants you to sign in with your Ethereum account:
%s

Sign in to Open Pool.

URI: https://%s
Version: 1
Chain ID: %d
Nonce: %s
Issued At: %s`, domain, w.Address, domain, chainID, nonce, issuedAt.UTC().Format(time.RFC3339))
}

// Mailbox collects the operator's email instead of sending it.
type Mailbox struct {
	mu      sync.Mutex
	Notices []Notice
	codes   []string
	// Fail, when set, is returned by every send.
	Fail error
}

// Notice is one admin notice.
type Notice struct {
	Subject string
	Body    string
}

// NotifyAdmin records a notice.
func (m *Mailbox) NotifyAdmin(ctx context.Context, subject, body string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Fail != nil {
		return m.Fail
	}
	m.Notices = append(m.Notices, Notice{subject, body})
	return nil
}

// SendSignInCode records a sign-in code.
func (m *Mailbox) SendSignInCode(ctx context.Context, email, code string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Fail != nil {
		return m.Fail
	}
	m.codes = append(m.codes, code)
	return nil
}

// LastCode is the most recent sign-in code, or "" when none was sent.
func (m *Mailbox) LastCode() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.codes) == 0 {
		return ""
	}
	return m.codes[len(m.codes)-1]
}

// Codes is how many sign-in codes were sent.
func (m *Mailbox) Codes() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.codes)
}

// AllNotices returns the notices sent so far.
func (m *Mailbox) AllNotices() []Notice {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]Notice(nil), m.Notices...)
}

// Orchestrator stands in for the orchestrator's CLI port: it holds a list
// of runners and remembers what it was told.
type Orchestrator struct {
	mu sync.Mutex
	// List is what Runners returns.
	List []core.OrchRunner
	// Prices is the last table pushed.
	Prices map[string]core.Price
	// EvictedRunners, EvictedMembers and States are calls received, in order.
	EvictedRunners []string
	EvictedMembers []string
	States         []string
	// Down, when set, is returned by every call.
	Down error
}

// Runners returns the list.
func (o *Orchestrator) Runners(ctx context.Context) ([]core.OrchRunner, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.Down != nil {
		return nil, o.Down
	}
	return append([]core.OrchRunner(nil), o.List...), nil
}

// SetState records the call and updates the listed runner.
func (o *Orchestrator) SetState(ctx context.Context, runnerID, state, reason string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.Down != nil {
		return o.Down
	}
	for i := range o.List {
		if o.List[i].RunnerID == runnerID {
			o.List[i].State = state
			o.States = append(o.States, runnerID+"="+state)
			return nil
		}
	}
	return &core.Error{Kind: core.NotFound, Message: "runner not found"}
}

// EvictRunner removes the runner from the list.
func (o *Orchestrator) EvictRunner(ctx context.Context, runnerID, reason string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.Down != nil {
		return o.Down
	}
	o.EvictedRunners = append(o.EvictedRunners, runnerID)
	for i := range o.List {
		if o.List[i].RunnerID == runnerID {
			o.List = append(o.List[:i], o.List[i+1:]...)
			return nil
		}
	}
	return &core.Error{Kind: core.NotFound, Message: "runner not found"}
}

// EvictMember removes the member's runners from the list.
func (o *Orchestrator) EvictMember(ctx context.Context, memberID, reason string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.Down != nil {
		return o.Down
	}
	o.EvictedMembers = append(o.EvictedMembers, memberID)
	kept, removed := o.List[:0:0], 0
	for _, r := range o.List {
		if r.MemberID == memberID {
			removed++
			continue
		}
		kept = append(kept, r)
	}
	o.List = kept
	if removed == 0 {
		return &core.Error{Kind: core.NotFound, Message: "member has no runners"}
	}
	return nil
}

// SetPrices records the table.
func (o *Orchestrator) SetPrices(ctx context.Context, prices map[string]core.Price) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.Down != nil {
		return o.Down
	}
	o.Prices = prices
	return nil
}

// Register adds a runner to the list, as the orchestrator does after the
// pool service admits it.
func (o *Orchestrator) Register(r core.OrchRunner) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.List = append(o.List, r)
}

// PushedPrices is the last price table pushed.
func (o *Orchestrator) PushedPrices() map[string]core.Price {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.Prices
}

// Evicted is the runner ids evicted so far.
func (o *Orchestrator) Evicted() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.EvictedRunners...)
}

// PinnedImage is a container image reference pinned by digest, for app definitions.
const PinnedImage = "vllm/vllm-openai@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
