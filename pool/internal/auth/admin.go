package auth

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/Livepeer-Open-Pool/pool/internal/store"
)

const (
	adminCodeTTL      = 10 * time.Minute
	adminCodeInterval = time.Minute
	adminCodeAttempts = 5
)

// CodeSender delivers a sign-in code to the operator.
type CodeSender interface {
	SendSignInCode(ctx context.Context, email, code string) error
}

// Admin signs the operator in with a one-time code sent to the admin email.
type Admin struct {
	db     *store.DB
	tokens *Tokens
	sender CodeSender
	email  string
	now    func() time.Time
}

// NewAdmin returns the operator sign-in for the one admin email.
func NewAdmin(db *store.DB, tokens *Tokens, sender CodeSender, email string, now func() time.Time) *Admin {
	if now == nil {
		now = time.Now
	}
	return &Admin{db: db, tokens: tokens, sender: sender, email: strings.ToLower(strings.TrimSpace(email)), now: now}
}

func (a *Admin) isAdmin(email string) bool {
	email = strings.ToLower(strings.TrimSpace(email))
	return a.email != "" && hmac.Equal([]byte(email), []byte(a.email))
}

func (a *Admin) hash(code string) string {
	mac := hmac.New(sha256.New, a.tokens.secret)
	mac.Write([]byte("admin-code:" + code))
	return hex.EncodeToString(mac.Sum(nil))
}

// RequestCode emails a sign-in code if email is the admin's. It reports
// success either way, so the endpoint does not reveal which address is the
// admin's. ErrTooSoon limits how often a code is sent.
func (a *Admin) RequestCode(ctx context.Context, email string) error {
	if !a.isAdmin(email) {
		return nil
	}
	now := a.now()
	var last int64
	err := a.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(created_at), 0) FROM admin_codes`).Scan(&last)
	if err != nil {
		return err
	}
	if last != 0 && now.Sub(time.UnixMilli(last)) < adminCodeInterval {
		return ErrTooSoon
	}
	n, err := rand.Int(rand.Reader, big.NewInt(1_000_000))
	if err != nil {
		return err
	}
	code := fmt.Sprintf("%06d", n.Int64())
	// One live code at a time: asking again retires the earlier one.
	if _, err := a.db.ExecContext(ctx, `DELETE FROM admin_codes`); err != nil {
		return err
	}
	if _, err := a.db.ExecContext(ctx, `INSERT INTO admin_codes (code_hash, created_at, expires_at) VALUES (?, ?, ?)`,
		a.hash(code), now.UnixMilli(), now.Add(adminCodeTTL).UnixMilli()); err != nil {
		return err
	}
	return a.sender.SendSignInCode(ctx, a.email, code)
}

// SignIn exchanges the emailed code for an admin session token. Every
// failure is ErrInvalid; a code is refused after a few wrong attempts.
func (a *Admin) SignIn(ctx context.Context, email, code string) (string, *Identity, error) {
	if !a.isAdmin(email) {
		return "", nil, ErrInvalid
	}
	now := a.now()
	var id int64
	var hash string
	var expires int64
	var attempts int
	err := a.db.QueryRowContext(ctx, `SELECT id, code_hash, expires_at, attempts FROM admin_codes WHERE used = 0 ORDER BY id DESC LIMIT 1`).
		Scan(&id, &hash, &expires, &attempts)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil, ErrInvalid
	}
	if err != nil {
		return "", nil, err
	}
	if now.UnixMilli() > expires || attempts >= adminCodeAttempts {
		return "", nil, ErrInvalid
	}
	if _, err := a.db.ExecContext(ctx, `UPDATE admin_codes SET attempts = attempts + 1 WHERE id = ?`, id); err != nil {
		return "", nil, err
	}
	if !hmac.Equal([]byte(a.hash(strings.TrimSpace(code))), []byte(hash)) {
		return "", nil, ErrInvalid
	}
	res, err := a.db.ExecContext(ctx, `UPDATE admin_codes SET used = 1 WHERE id = ? AND used = 0`, id)
	if err != nil {
		return "", nil, err
	}
	if used, err := res.RowsAffected(); err != nil || used != 1 {
		return "", nil, ErrInvalid
	}
	token, expiresAt := a.tokens.Issue(RoleAdmin, a.email)
	return token, &Identity{Role: RoleAdmin, Subject: a.email, ExpiresAt: expiresAt}, nil
}
