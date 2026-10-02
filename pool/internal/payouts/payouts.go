// Package payouts closes the pool's payout windows, works out what each
// member is owed for a window, and, once a batch is approved, pays it from
// the region's payout wallet.
package payouts

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"sync"
	"time"

	"github.com/Livepeer-Open-Pool/pool/internal/core"
	"github.com/Livepeer-Open-Pool/pool/internal/ledger"
	"github.com/Livepeer-Open-Pool/pool/internal/store"
)

// Batch statuses.
const (
	StatusOpen     = "open"     // the window is still running
	StatusReady    = "ready"    // built, waiting for the operator's approval
	StatusApproved = "approved" // approved, not yet being sent
	StatusSending  = "sending"  // payments are going out
	StatusBlocked  = "blocked"  // the payout wallet cannot cover what is left
	StatusFailed   = "failed"   // some payments failed and need the operator
	StatusSent     = "sent"     // every payment confirmed
	StatusEmpty    = "empty"    // the window closed with nothing to pay
)

// Payment statuses.
const (
	ItemPending   = "pending"   // not yet signed
	ItemSigned    = "signed"    // a transaction is stored, not known to be broadcast
	ItemBroadcast = "broadcast" // sent to the network, not yet confirmed
	ItemConfirmed = "confirmed"
	ItemFailed    = "failed"
)

// RoundSource says which Livepeer round it is.
type RoundSource interface {
	CurrentRound(ctx context.Context) (int64, error)
}

// Config is what the payout service needs to know about where it runs.
type Config struct {
	RegionName string
	AdminURL   string
	// SettleDelay is how long after a window's last round has passed the
	// window is closed, so events still in flight are counted in it.
	SettleDelay time.Duration
	// ConfirmWait is how long one pass waits for a payment to confirm before
	// leaving it for the next pass.
	ConfirmWait time.Duration
	Now         func() time.Time
}

// Service runs the pool's payouts.
type Service struct {
	db       *store.DB
	ledger   *ledger.Ledger
	settings func(context.Context) (core.Settings, error)
	rounds   RoundSource
	wallet   *Wallet // nil when no payout wallet is configured
	notify   core.Notifier
	cfg      Config
	log      *slog.Logger

	mu sync.Mutex // one pass at a time
}

// New returns a payout service. wallet may be nil, in which case batches are
// built and approved but nothing is sent.
func New(db *store.DB, books *ledger.Ledger, settings func(context.Context) (core.Settings, error), rounds RoundSource,
	wallet *Wallet, notify core.Notifier, cfg Config, log *slog.Logger) *Service {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.SettleDelay <= 0 {
		cfg.SettleDelay = 10 * time.Minute
	}
	if cfg.ConfirmWait <= 0 {
		cfg.ConfirmWait = 30 * time.Second
	}
	if log == nil {
		log = slog.Default()
	}
	return &Service{db: db, ledger: books, settings: settings, rounds: rounds, wallet: wallet, notify: notify, cfg: cfg, log: log}
}

func (s *Service) nowMs() int64 { return s.cfg.Now().UnixMilli() }

// Batch is the payments for one window.
type Batch struct {
	ID            string     `json:"id"`
	FirstRound    int64      `json:"first_round"`
	LastRound     int64      `json:"last_round"`
	Status        string     `json:"status"`
	PotWei        string     `json:"pot_wei"`
	CommissionBps int        `json:"commission_bps"`
	PayableWei    string     `json:"payable_wei"`
	TotalFeesWei  string     `json:"total_fees_wei"`
	OtherFeesWei  string     `json:"other_fees_wei"`
	Note          string     `json:"note,omitempty"`
	CreatedAt     time.Time  `json:"created_at"`
	ClosedAt      *time.Time `json:"closed_at,omitempty"`
	ApprovedAt    *time.Time `json:"approved_at,omitempty"`
	ApprovedBy    string     `json:"approved_by,omitempty"`
	SentAt        *time.Time `json:"sent_at,omitempty"`
	Items         []Item     `json:"items,omitempty"`
}

// Item is one member's payment in a batch.
type Item struct {
	BatchID     string `json:"batch_id"`
	MemberID    string `json:"member_id"`
	Address     string `json:"address"`
	DisplayName string `json:"display_name"`
	FeesWei     string `json:"fees_wei"`
	AmountWei   string `json:"amount_wei"`
	// Include says whether the payment is made. A member banned during the
	// window starts excluded.
	Include bool   `json:"include"`
	Banned  bool   `json:"banned"`
	Status  string `json:"status"`
	TxHash  string `json:"tx_hash,omitempty"`
	Error   string `json:"error,omitempty"`
}

const batchColumns = `id, first_round, last_round, status, pot_wei, commission_bps, payable_wei, total_fees_wei, other_fees_wei,
	note, created_at, closed_at, approved_at, approved_by, sent_at`

type rowScanner interface {
	Scan(dest ...any) error
}

func optionalTime(ms int64) *time.Time {
	if ms == 0 {
		return nil
	}
	t := time.UnixMilli(ms).UTC()
	return &t
}

func scanBatch(row rowScanner) (*Batch, error) {
	var b Batch
	var created, closed, approved, sent int64
	if err := row.Scan(&b.ID, &b.FirstRound, &b.LastRound, &b.Status, &b.PotWei, &b.CommissionBps, &b.PayableWei, &b.TotalFeesWei,
		&b.OtherFeesWei, &b.Note, &created, &closed, &approved, &b.ApprovedBy, &sent); err != nil {
		return nil, err
	}
	b.CreatedAt = time.UnixMilli(created).UTC()
	b.ClosedAt, b.ApprovedAt, b.SentAt = optionalTime(closed), optionalTime(approved), optionalTime(sent)
	return &b, nil
}

// Batches lists every batch, newest window first, without their payments.
func (s *Service) Batches(ctx context.Context) ([]Batch, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+batchColumns+` FROM batches ORDER BY first_round DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Batch{}
	for rows.Next() {
		b, err := scanBatch(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *b)
	}
	return out, rows.Err()
}

func (s *Service) items(ctx context.Context, batchID string) ([]Item, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT batch_id, member_id, address, display_name, fees_wei, amount_wei, include, banned, status, tx_hash, error
		FROM batch_items WHERE batch_id = ? ORDER BY member_id`, batchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Item{}
	for rows.Next() {
		var it Item
		if err := rows.Scan(&it.BatchID, &it.MemberID, &it.Address, &it.DisplayName, &it.FeesWei, &it.AmountWei, &it.Include, &it.Banned,
			&it.Status, &it.TxHash, &it.Error); err != nil {
			return nil, err
		}
		out = append(out, it)
	}
	return out, rows.Err()
}

// Batch returns one batch with its payments.
func (s *Service) Batch(ctx context.Context, id string) (*Batch, error) {
	b, err := scanBatch(s.db.QueryRowContext(ctx, `SELECT `+batchColumns+` FROM batches WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, &core.Error{Kind: core.NotFound, Message: fmt.Sprintf("batch %s does not exist", id)}
	}
	if err != nil {
		return nil, err
	}
	b.Items, err = s.items(ctx, id)
	return b, err
}

// MemberPayout is one member's payment as the member sees it.
type MemberPayout struct {
	BatchID     string `json:"batch_id"`
	FirstRound  int64  `json:"first_round"`
	LastRound   int64  `json:"last_round"`
	BatchStatus string `json:"batch_status"`
	FeesWei     string `json:"fees_wei"`
	AmountWei   string `json:"amount_wei"`
	Include     bool   `json:"include"`
	Status      string `json:"status"`
	TxHash      string `json:"tx_hash,omitempty"`
}

// MemberPayouts lists a member's payments, newest window first.
func (s *Service) MemberPayouts(ctx context.Context, memberID string) ([]MemberPayout, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT b.id, b.first_round, b.last_round, b.status, i.fees_wei, i.amount_wei, i.include, i.status, i.tx_hash
		FROM batch_items i JOIN batches b ON b.id = i.batch_id WHERE i.member_id = ? ORDER BY b.first_round DESC`, memberID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []MemberPayout{}
	for rows.Next() {
		var p MemberPayout
		if err := rows.Scan(&p.BatchID, &p.FirstRound, &p.LastRound, &p.BatchStatus, &p.FeesWei, &p.AmountWei, &p.Include, &p.Status, &p.TxHash); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// OpenWindow returns the rounds of the window still running, or false when
// the pool has not seen a round yet.
func (s *Service) OpenWindow(ctx context.Context) (first, last int64, ok bool, err error) {
	err = s.db.QueryRowContext(ctx, `SELECT first_round, last_round FROM batches WHERE status = ? ORDER BY first_round DESC LIMIT 1`, StatusOpen).
		Scan(&first, &last)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, 0, false, nil
	}
	return first, last, err == nil, err
}

// Tick is one pass: it closes windows whose rounds have passed, builds their
// batches, and sends the payments of approved batches. The service runs it
// on a timer; it is safe to call at any time.
func (s *Service) Tick(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	windowErr := s.closeWindows(ctx)
	if err := s.sendApproved(ctx); err != nil {
		return err
	}
	return windowErr
}

func (s *Service) closeWindows(ctx context.Context) error {
	if s.rounds == nil {
		return nil
	}
	round, err := s.rounds.CurrentRound(ctx)
	if err != nil {
		return &core.Error{Kind: core.Unavailable, Message: fmt.Sprintf("the orchestrator did not say which round it is: %v", err)}
	}
	settings, err := s.settings(ctx)
	if err != nil {
		return err
	}
	for {
		var id string
		var first, last, closableSince int64
		err := s.db.QueryRowContext(ctx, `SELECT id, first_round, last_round, closable_since FROM batches WHERE status = ? ORDER BY first_round DESC LIMIT 1`, StatusOpen).
			Scan(&id, &first, &last, &closableSince)
		if errors.Is(err, sql.ErrNoRows) {
			// The first window starts with the first round the pool sees.
			var latest sql.NullInt64
			if err := s.db.QueryRowContext(ctx, `SELECT MAX(last_round) FROM batches`).Scan(&latest); err != nil {
				return err
			}
			start := round
			if latest.Valid {
				start = latest.Int64 + 1
			}
			if err := s.openWindow(ctx, start, settings.WindowRounds, 0); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		if round <= last {
			return nil // still running
		}
		now := s.nowMs()
		if closableSince == 0 {
			if _, err := s.db.ExecContext(ctx, `UPDATE batches SET closable_since = ? WHERE id = ?`, now, id); err != nil {
				return err
			}
			return nil
		}
		if time.Duration(now-closableSince)*time.Millisecond < s.cfg.SettleDelay {
			return nil // let events still in flight arrive
		}
		if err := s.closeWindow(ctx, id, first, last, settings); err != nil {
			return err
		}
		// A service that was down through several windows closes them one
		// after another without waiting again for each.
		if err := s.openWindow(ctx, last+1, settings.WindowRounds, closableSince); err != nil {
			return err
		}
	}
}

func (s *Service) openWindow(ctx context.Context, first int64, rounds int, closableSince int64) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO batches (id, first_round, last_round, status, closable_since, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
		fmt.Sprintf("batch_%d", first), first, first+int64(rounds)-1, StatusOpen, closableSince, s.nowMs())
	return err
}

// closeWindow works out the window's payments:
//
//	payable        = pot × (1 − commission)
//	member payment = payable × member's fees ÷ all members' fees
//
// rounded down to the wei. Fees of runners that belong to no member are left
// out of the split and reported beside it.
func (s *Service) closeWindow(ctx context.Context, id string, first, last int64, settings core.Settings) error {
	pot, err := s.ledger.Redeemed(ctx, first, last)
	if err != nil {
		return err
	}
	fees, err := s.ledger.Fees(ctx, first, last)
	if err != nil {
		return err
	}
	total, other := new(big.Int), new(big.Int)
	for _, f := range fees {
		if f.Member {
			total.Add(total, f.Fees)
		} else {
			other.Add(other, f.Fees)
		}
	}
	payable := new(big.Int).Mul(pot, big.NewInt(int64(10000-settings.CommissionBps)))
	payable.Div(payable, big.NewInt(10000))

	type payment struct {
		memberID, address, name string
		banned                  bool
		fees, amount            *big.Int
	}
	var payments []payment
	if payable.Sign() > 0 && total.Sign() > 0 {
		for _, f := range fees {
			if !f.Member || f.Fees.Sign() == 0 {
				continue
			}
			p := payment{memberID: f.MemberID, fees: f.Fees}
			var status string
			if err := s.db.QueryRowContext(ctx, `SELECT address, display_name, status FROM members WHERE id = ?`, f.MemberID).
				Scan(&p.address, &p.name, &status); err != nil {
				return err
			}
			p.banned = status == core.MemberBanned
			p.amount = new(big.Int).Mul(payable, f.Fees)
			p.amount.Div(p.amount, total)
			if p.amount.Sign() > 0 {
				payments = append(payments, p)
			}
		}
	}

	status := StatusEmpty
	if len(payments) > 0 {
		status = StatusReady
	}
	now := s.nowMs()
	err = s.db.Tx(ctx, func(tx *sql.Tx) error {
		for _, p := range payments {
			if _, err := tx.ExecContext(ctx, `INSERT INTO batch_items (batch_id, member_id, address, display_name, fees_wei, amount_wei, include, banned, status, updated_at)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, id, p.memberID, p.address, p.name, p.fees.String(), p.amount.String(), !p.banned, p.banned, ItemPending, now); err != nil {
				return err
			}
		}
		_, err := tx.ExecContext(ctx, `UPDATE batches SET status = ?, pot_wei = ?, commission_bps = ?, payable_wei = ?, total_fees_wei = ?, other_fees_wei = ?, closed_at = ? WHERE id = ?`,
			status, pot.String(), settings.CommissionBps, payable.String(), total.String(), other.String(), now, id)
		return err
	})
	if err != nil {
		return err
	}
	s.log.Info("payout window closed", "batch", id, "rounds", fmt.Sprintf("%d-%d", first, last), "pot_wei", pot, "payable_wei", payable, "payments", len(payments))
	if status != StatusReady {
		return nil
	}
	if settings.PayoutApproval == core.PayoutApprovalAutomatic {
		return s.approve(ctx, "pool", id)
	}
	if s.notify != nil {
		subject := fmt.Sprintf("Open Pool %s: payouts for rounds %d to %d need approval", s.cfg.RegionName, first, last)
		body := fmt.Sprintf("The payout window for rounds %d to %d has closed in %s.\n\n%d payment(s), %s wei in total, are waiting for your approval.\n\nReview them: %s/batches/%s",
			first, last, s.cfg.RegionName, len(payments), payable, s.cfg.AdminURL, id)
		if err := s.notify.NotifyAdmin(ctx, subject, body); err != nil {
			s.log.Error("admin email not sent", "subject", subject, "err", err)
		}
	}
	return nil
}

func (s *Service) approve(ctx context.Context, actor, id string) error {
	res, err := s.db.ExecContext(ctx, `UPDATE batches SET status = ?, approved_at = ?, approved_by = ? WHERE id = ? AND status = ?`,
		StatusApproved, s.nowMs(), actor, id, StatusReady)
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err != nil || n == 0 {
		return &core.Error{Kind: core.Conflict, Message: "only a batch that is waiting for approval can be approved"}
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO audit (at, actor, action, subject, detail) VALUES (?, ?, 'batch.approve', ?, '')`, s.nowMs(), actor, id)
	return err
}

// Approve approves a batch for payment.
func (s *Service) Approve(ctx context.Context, actor, id string) (*Batch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.Batch(ctx, id); err != nil {
		return nil, err
	}
	if err := s.approve(ctx, actor, id); err != nil {
		return nil, err
	}
	return s.Batch(ctx, id)
}

// SetItemIncluded decides whether a member is paid in a batch that has not
// been approved yet. Money not paid stays in the payout wallet.
func (s *Service) SetItemIncluded(ctx context.Context, actor, batchID, memberID string, include bool) (*Batch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	batch, err := s.Batch(ctx, batchID)
	if err != nil {
		return nil, err
	}
	if batch.Status != StatusReady {
		return nil, &core.Error{Kind: core.Conflict, Message: "payments can only be switched on or off before the batch is approved"}
	}
	res, err := s.db.ExecContext(ctx, `UPDATE batch_items SET include = ?, updated_at = ? WHERE batch_id = ? AND member_id = ?`, include, s.nowMs(), batchID, memberID)
	if err != nil {
		return nil, err
	}
	if n, err := res.RowsAffected(); err != nil || n == 0 {
		return nil, &core.Error{Kind: core.NotFound, Message: fmt.Sprintf("batch %s has no payment for %s", batchID, memberID)}
	}
	detail := "excluded"
	if include {
		detail = "included"
	}
	if _, err := s.db.ExecContext(ctx, `INSERT INTO audit (at, actor, action, subject, detail) VALUES (?, ?, 'batch.item', ?, ?)`,
		s.nowMs(), actor, batchID, memberID+" "+detail); err != nil {
		return nil, err
	}
	return s.Batch(ctx, batchID)
}

// Retry puts a batch's failed payments back in line to be made again.
func (s *Service) Retry(ctx context.Context, actor, id string) (*Batch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	batch, err := s.Batch(ctx, id)
	if err != nil {
		return nil, err
	}
	if batch.Status != StatusFailed && batch.Status != StatusBlocked {
		return nil, &core.Error{Kind: core.Conflict, Message: "only a batch with failed payments, or one blocked for lack of funds, can be retried"}
	}
	err = s.db.Tx(ctx, func(tx *sql.Tx) error {
		// A failed payment's transaction can never be mined (it reverted, or
		// its nonce was used by another), so signing a new one cannot pay twice.
		if _, err := tx.ExecContext(ctx, `UPDATE batch_items SET status = ?, nonce = -1, tx_hash = '', raw_tx = '', error = '', updated_at = ? WHERE batch_id = ? AND status = ?`,
			ItemPending, s.nowMs(), id, ItemFailed); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE batches SET status = ?, note = '' WHERE id = ?`, StatusApproved, id); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO audit (at, actor, action, subject, detail) VALUES (?, ?, 'batch.retry', ?, '')`, s.nowMs(), actor, id)
		return err
	})
	if err != nil {
		return nil, err
	}
	return s.Batch(ctx, id)
}
