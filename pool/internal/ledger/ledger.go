// Package ledger turns the orchestrator's events into the pool's books:
// what each member's runners earned in each round, what the node redeemed,
// and how much demand each app saw. The events are go-livepeer's
// (doc/live-runner-pool.md there).
package ledger

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"time"

	"github.com/Livepeer-Open-Pool/pool/internal/store"
)

// Event types this ledger acts on. Others are accepted and ignored.
const (
	EventSessionReserved = "live_runner_session_reserved"
	EventSessionReleased = "live_runner_session_released"
	EventFeeDebited      = "live_runner_fee_debited"
	EventRequestsRefused = "live_runner_requests_refused"
	EventTicketRedeemed  = "orchestrator_ticket_redeemed"
)

// eventRetention is how long event ids are remembered for deduplication.
// Kafka redelivers within seconds or minutes, not weeks.
const eventRetention = 30 * 24 * time.Hour

// Ledger keeps the pool's books.
type Ledger struct {
	db  *store.DB
	now func() time.Time
	log *slog.Logger
}

// New returns a ledger over db.
func New(db *store.DB, now func() time.Time, log *slog.Logger) *Ledger {
	if now == nil {
		now = time.Now
	}
	if log == nil {
		log = slog.Default()
	}
	return &Ledger{db: db, now: now, log: log}
}

// event is the union of the fields the ledger reads from any event.
type event struct {
	Type      string `json:"event_type"`
	ID        string `json:"event_id"`
	TsMs      int64  `json:"ts_ms"`
	Round     int64  `json:"round"`
	RunnerID  string `json:"runner_id"`
	MemberID  string `json:"member_id"`
	App       string `json:"app"`
	SessionID string `json:"session_id"`
	Sender    string `json:"sender"`
	Paid      bool   `json:"paid"`
	Reason    string `json:"reason"`
	Duration  int64  `json:"duration_ms"`
	FeeWei    string `json:"fee_wei"`
	FeeTotal  string `json:"fee_wei_total"`
	FaceValue string `json:"face_value_wei"`
	TxHash    string `json:"tx_hash"`
	Count     int64  `json:"count"`
}

// ErrMalformed marks an event that can never be applied. The caller should
// not retry it.
var ErrMalformed = errors.New("malformed event")

func wei(s string) (*big.Int, error) {
	if s == "" {
		return new(big.Int), nil
	}
	n, ok := new(big.Int).SetString(s, 10)
	if !ok || n.Sign() < 0 {
		return nil, fmt.Errorf("%w: %q is not an amount of wei", ErrMalformed, s)
	}
	return n, nil
}

// Apply takes in one event, exactly as the orchestrator published it. An
// event already applied changes nothing, so redelivery is harmless. An error
// wrapping ErrMalformed means the event is unusable; any other error means
// it should be delivered again.
func (l *Ledger) Apply(ctx context.Context, raw []byte) error {
	var e event
	if err := json.Unmarshal(raw, &e); err != nil {
		return fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	if e.ID == "" || e.Type == "" {
		return fmt.Errorf("%w: no event_id or event_type", ErrMalformed)
	}
	switch e.Type {
	case EventSessionReserved, EventSessionReleased, EventFeeDebited, EventRequestsRefused, EventTicketRedeemed:
	default:
		return nil // registrations and state changes are not bookkeeping
	}
	if e.TsMs == 0 {
		e.TsMs = l.now().UnixMilli()
	}
	return l.db.Tx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `INSERT INTO events (event_id, at) VALUES (?, ?) ON CONFLICT (event_id) DO NOTHING`, e.ID, l.now().UnixMilli())
		if err != nil {
			return err
		}
		if fresh, err := res.RowsAffected(); err != nil || fresh == 0 {
			return err // seen before
		}
		switch e.Type {
		case EventSessionReserved:
			return l.sessionReserved(ctx, tx, e)
		case EventFeeDebited:
			return l.feeDebited(ctx, tx, e)
		case EventSessionReleased:
			return l.sessionReleased(ctx, tx, e)
		case EventRequestsRefused:
			return l.requestsRefused(ctx, tx, e)
		default:
			return l.ticketRedeemed(ctx, tx, e)
		}
	})
}

func hourOf(tsMs int64) int64 { return tsMs / int64(time.Hour/time.Millisecond) }

// bookRound is the round an amount is booked in. A window that has closed
// is settled, so an event that arrives late for one of its rounds is booked
// in the first round of the window still open, where it will be paid.
func bookRound(ctx context.Context, tx *sql.Tx, round int64) (int64, error) {
	var firstOpen sql.NullInt64
	err := tx.QueryRowContext(ctx, `SELECT first_round FROM batches WHERE status = 'open' ORDER BY first_round DESC LIMIT 1`).Scan(&firstOpen)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	if firstOpen.Valid && round != 0 && round < firstOpen.Int64 {
		return firstOpen.Int64, nil
	}
	return round, nil
}

// addWei adds amount to a decimal column of the row matched by where.
func addWei(ctx context.Context, tx *sql.Tx, table, column, where string, amount *big.Int, args ...any) error {
	var current string
	if err := tx.QueryRowContext(ctx, `SELECT `+column+` FROM `+table+` WHERE `+where, args...).Scan(&current); err != nil {
		return err
	}
	sum, err := wei(current)
	if err != nil {
		return err
	}
	sum.Add(sum, amount)
	_, err = tx.ExecContext(ctx, `UPDATE `+table+` SET `+column+` = ? WHERE `+where, append([]any{sum.String()}, args...)...)
	return err
}

func (l *Ledger) ensureSession(ctx context.Context, tx *sql.Tx, e event) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO sessions (session_id, runner_id, member_id, app, sender, paid, started_at)
		VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (session_id, runner_id) DO NOTHING`,
		e.SessionID, e.RunnerID, e.MemberID, e.App, e.Sender, e.Paid || e.Sender != "", e.TsMs)
	return err
}

func (l *Ledger) sessionReserved(ctx context.Context, tx *sql.Tx, e event) error {
	if e.SessionID == "" || e.RunnerID == "" {
		return fmt.Errorf("%w: session event without session_id or runner_id", ErrMalformed)
	}
	if err := l.ensureSession(ctx, tx, e); err != nil {
		return err
	}
	if !e.Paid {
		return nil // the pool's own tests are not work done for customers
	}
	round, err := bookRound(ctx, tx, e.Round)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO earnings (member_id, app, round, sessions) VALUES (?, ?, ?, 1)
		ON CONFLICT (member_id, app, round) DO UPDATE SET sessions = sessions + 1`, e.MemberID, e.App, round); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO app_hours (app, hour, sessions) VALUES (?, ?, 1)
		ON CONFLICT (app, hour) DO UPDATE SET sessions = sessions + 1`, e.App, hourOf(e.TsMs))
	return err
}

// credit books whatever of the session's value has not been booked yet. A
// session's value is the larger of the sum of its fee events and the total
// its released event reported, so fee events that were lost are made up for
// by the total, and neither is counted twice whatever order they arrive in.
func (l *Ledger) credit(ctx context.Context, tx *sql.Tx, e event) error {
	var feeEvents, releasedTotal, credited string
	if err := tx.QueryRowContext(ctx, `SELECT fee_events_wei, released_total_wei, credited_wei FROM sessions WHERE session_id = ? AND runner_id = ?`,
		e.SessionID, e.RunnerID).Scan(&feeEvents, &releasedTotal, &credited); err != nil {
		return err
	}
	value, err := wei(feeEvents)
	if err != nil {
		return err
	}
	total, err := wei(releasedTotal)
	if err != nil {
		return err
	}
	if total.Cmp(value) > 0 {
		value = total
	}
	done, err := wei(credited)
	if err != nil {
		return err
	}
	delta := new(big.Int).Sub(value, done)
	if delta.Sign() <= 0 {
		return nil
	}
	round, err := bookRound(ctx, tx, e.Round)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO earnings (member_id, app, round) VALUES (?, ?, ?) ON CONFLICT (member_id, app, round) DO NOTHING`,
		e.MemberID, e.App, round); err != nil {
		return err
	}
	if err := addWei(ctx, tx, "earnings", "fee_wei", "member_id = ? AND app = ? AND round = ?", delta, e.MemberID, e.App, round); err != nil {
		return err
	}
	hour := hourOf(e.TsMs)
	if _, err := tx.ExecContext(ctx, `INSERT INTO app_hours (app, hour) VALUES (?, ?) ON CONFLICT (app, hour) DO NOTHING`, e.App, hour); err != nil {
		return err
	}
	if err := addWei(ctx, tx, "app_hours", "fee_wei", "app = ? AND hour = ?", delta, e.App, hour); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE sessions SET credited_wei = ? WHERE session_id = ? AND runner_id = ?`, value.String(), e.SessionID, e.RunnerID)
	return err
}

func (l *Ledger) feeDebited(ctx context.Context, tx *sql.Tx, e event) error {
	if e.SessionID == "" || e.RunnerID == "" || e.MemberID == "" {
		return fmt.Errorf("%w: fee event without session_id, runner_id or member_id", ErrMalformed)
	}
	fee, err := wei(e.FeeWei)
	if err != nil {
		return err
	}
	e.Paid = true
	if err := l.ensureSession(ctx, tx, e); err != nil {
		return err
	}
	if err := addWei(ctx, tx, "sessions", "fee_events_wei", "session_id = ? AND runner_id = ?", fee, e.SessionID, e.RunnerID); err != nil {
		return err
	}
	return l.credit(ctx, tx, e)
}

func (l *Ledger) sessionReleased(ctx context.Context, tx *sql.Tx, e event) error {
	if e.SessionID == "" || e.RunnerID == "" {
		return fmt.Errorf("%w: session event without session_id or runner_id", ErrMalformed)
	}
	total, err := wei(e.FeeTotal)
	if err != nil {
		return err
	}
	if err := l.ensureSession(ctx, tx, e); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE sessions SET ended_at = ?, reason = ?, duration_ms = ?, released_total_wei = ? WHERE session_id = ? AND runner_id = ?`,
		e.TsMs, e.Reason, e.Duration, total.String(), e.SessionID, e.RunnerID); err != nil {
		return err
	}
	if e.MemberID == "" {
		return nil
	}
	return l.credit(ctx, tx, e)
}

func (l *Ledger) requestsRefused(ctx context.Context, tx *sql.Tx, e event) error {
	if e.App == "" || e.Count <= 0 {
		return fmt.Errorf("%w: refused-requests event without app or count", ErrMalformed)
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO app_hours (app, hour, refused) VALUES (?, ?, ?)
		ON CONFLICT (app, hour) DO UPDATE SET refused = refused + excluded.refused`, e.App, hourOf(e.TsMs), e.Count)
	return err
}

func (l *Ledger) ticketRedeemed(ctx context.Context, tx *sql.Tx, e event) error {
	value, err := wei(e.FaceValue)
	if err != nil {
		return err
	}
	if e.Round <= 0 {
		return fmt.Errorf("%w: redeemed-ticket event without a round", ErrMalformed)
	}
	round, err := bookRound(ctx, tx, e.Round)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO redemptions (event_id, round, face_value_wei, sender, tx_hash, at) VALUES (?, ?, ?, ?, ?, ?)`,
		e.ID, round, value.String(), e.Sender, e.TxHash, e.TsMs)
	return err
}

// Prune forgets event ids old enough that they will not be delivered again.
func (l *Ledger) Prune(ctx context.Context) error {
	_, err := l.db.ExecContext(ctx, `DELETE FROM events WHERE at < ?`, l.now().Add(-eventRetention).UnixMilli())
	return err
}
