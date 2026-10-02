// Package core is the pool's rules: the catalog the operator defines, the
// people who apply and join, their machines and what each is assigned, the
// credentials machines present, and the answer the orchestrator gets when a
// runner asks to register.
package core

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base32"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"

	"github.com/Livepeer-Open-Pool/pool/internal/store"
)

// Orchestrator is what the pool service asks of the orchestrator node it
// belongs to, through the node's CLI port.
type Orchestrator interface {
	Runners(ctx context.Context) ([]OrchRunner, error)
	SetState(ctx context.Context, runnerID, state, reason string) error
	EvictRunner(ctx context.Context, runnerID, reason string) error
	EvictMember(ctx context.Context, memberID, reason string) error
	SetPrices(ctx context.Context, prices map[string]Price) error
}

// Notifier tells the operator that something needs them.
type Notifier interface {
	NotifyAdmin(ctx context.Context, subject, body string) error
}

// Config is what the service needs to know about where it runs.
type Config struct {
	RegionID   string
	RegionName string
	// PublicURL is this service's own address, which agents and the
	// orchestrator's runners are told to use.
	PublicURL string
	// OrchestratorURL is the orchestrator's public service address, which
	// runners connect to.
	OrchestratorURL string
	// AdminURL is the admin site, for links in emails.
	AdminURL string
	// RecoverSigner returns the address that signed message, or an error.
	RecoverSigner func(message, signature string) (string, error)
	// Now is the clock; nil means time.Now.
	Now func() time.Time
}

// Service is the pool's rules over its database.
type Service struct {
	db     *store.DB
	orch   Orchestrator
	prober Prober
	mail   Notifier
	cfg    Config
	log    *slog.Logger

	// reconcileMu keeps one pass over the runners at a time.
	reconcileMu sync.Mutex
}

// New returns a Service. orch and mail may be nil in tests that do not reach them.
func New(db *store.DB, orch Orchestrator, mail Notifier, cfg Config, log *slog.Logger) *Service {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if log == nil {
		log = slog.Default()
	}
	return &Service{db: db, orch: orch, mail: mail, cfg: cfg, log: log}
}

// SetProber gives the service the means to test runners. Without one,
// Reconcile still drains and enforces versions and tests nothing.
func (s *Service) SetProber(prober Prober) { s.prober = prober }

// Region is the region this service serves.
func (s *Service) Region() (id, name string) { return s.cfg.RegionID, s.cfg.RegionName }

func (s *Service) now() time.Time { return s.cfg.Now().UTC() }

func (s *Service) nowMs() int64 { return s.now().UnixMilli() }

var idEncoding = base32.StdEncoding.WithPadding(base32.NoPadding)

// newID returns a random identifier with a prefix that says what it names.
func newID(prefix string) string {
	b := make([]byte, 10)
	if _, err := rand.Read(b); err != nil {
		panic("core: reading random bytes: " + err.Error())
	}
	return prefix + "_" + strings.ToLower(idEncoding.EncodeToString(b))
}

// newSecret returns a credential secret: 32 random bytes behind a prefix
// that makes a leaked one recognizable.
func newSecret() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic("core: reading random bytes: " + err.Error())
	}
	return "opc_" + strings.ToLower(idEncoding.EncodeToString(b))
}

func hashSecret(secret string) string {
	sum := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(sum[:])
}

// normalizeAddress returns addr in checksum form, or false when it is not an
// Ethereum address.
func normalizeAddress(addr string) (string, bool) {
	addr = strings.TrimSpace(addr)
	if !ethcommon.IsHexAddress(addr) {
		return "", false
	}
	return ethcommon.HexToAddress(addr).Hex(), true
}

func fromMs(ms int64) time.Time {
	return time.UnixMilli(ms).UTC()
}

// optionalTime is nil for the zero timestamp.
func optionalTime(ms int64) *time.Time {
	if ms == 0 {
		return nil
	}
	t := fromMs(ms)
	return &t
}

func mustJSON(v any) string {
	data, err := json.Marshal(v)
	if err != nil {
		panic("core: encoding stored JSON: " + err.Error())
	}
	return string(data)
}

// execer is satisfied by *sql.DB and *sql.Tx.
type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// audit records that actor did action to subject.
func (s *Service) audit(ctx context.Context, db execer, actor, action, subject, detail string) error {
	_, err := db.ExecContext(ctx, `INSERT INTO audit (at, actor, action, subject, detail) VALUES (?, ?, ?, ?, ?)`,
		s.nowMs(), actor, action, subject, detail)
	return err
}

// AuditEntry is one recorded action.
type AuditEntry struct {
	ID      int64     `json:"id"`
	At      time.Time `json:"at"`
	Actor   string    `json:"actor"`
	Action  string    `json:"action"`
	Subject string    `json:"subject"`
	Detail  string    `json:"detail"`
}

// Audit returns the most recent entries, newest first.
func (s *Service) Audit(ctx context.Context, limit int) ([]AuditEntry, error) {
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id, at, actor, action, subject, detail FROM audit ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	entries := []AuditEntry{}
	for rows.Next() {
		var e AuditEntry
		var at int64
		if err := rows.Scan(&e.ID, &at, &e.Actor, &e.Action, &e.Subject, &e.Detail); err != nil {
			return nil, err
		}
		e.At = fromMs(at)
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// notifyAdmin emails the operator. A failure is logged and not returned: the
// thing the email is about has already happened and is visible on the admin site.
func (s *Service) notifyAdmin(ctx context.Context, subject, body string) {
	if s.mail == nil {
		return
	}
	if err := s.mail.NotifyAdmin(ctx, subject, body); err != nil {
		s.log.Error("admin email not sent", "subject", subject, "err", err)
	}
}
