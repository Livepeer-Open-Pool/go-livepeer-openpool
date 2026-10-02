// Package auth signs people in: members with their Ethereum wallet, the
// operator with a code sent to the admin email. Either way the result is a
// signed session token every region accepts, because the regions share the
// signing secret.
package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

// Roles a session can have.
const (
	RoleWallet = "wallet" // an applicant or member, identified by address
	RoleAdmin  = "admin"  // the operator
)

// ErrInvalid means the credentials presented do not sign anyone in. It says
// no more than that, on purpose.
var ErrInvalid = errors.New("not signed in")

// ErrTooSoon means a sign-in code was asked for again too quickly.
var ErrTooSoon = errors.New("a code was sent moments ago; wait before asking for another")

// Identity is who a session token says the caller is.
type Identity struct {
	Role string
	// Subject is the wallet address for RoleWallet and the admin email for RoleAdmin.
	Subject   string
	ExpiresAt time.Time
}

// Tokens issues and verifies session tokens.
type Tokens struct {
	secret []byte
	ttl    time.Duration
	now    func() time.Time
}

// NewTokens returns a token issuer. secret must be the same in every region
// for a session from one to work in the others.
func NewTokens(secret string, ttl time.Duration, now func() time.Time) (*Tokens, error) {
	if len(secret) < 32 {
		return nil, errors.New("session signing secret must be at least 32 characters")
	}
	if now == nil {
		now = time.Now
	}
	return &Tokens{secret: []byte(secret), ttl: ttl, now: now}, nil
}

type tokenPayload struct {
	Version int    `json:"v"`
	Role    string `json:"role"`
	Subject string `json:"sub"`
	Expires int64  `json:"exp"`
}

var tokenEncoding = base64.RawURLEncoding

func (t *Tokens) sign(payload []byte) []byte {
	mac := hmac.New(sha256.New, t.secret)
	mac.Write(payload)
	return mac.Sum(nil)
}

// Issue returns a session token for the subject and when it expires.
func (t *Tokens) Issue(role, subject string) (string, time.Time) {
	expires := t.now().Add(t.ttl).UTC()
	payload, _ := json.Marshal(tokenPayload{Version: 1, Role: role, Subject: subject, Expires: expires.Unix()})
	return tokenEncoding.EncodeToString(payload) + "." + tokenEncoding.EncodeToString(t.sign(payload)), expires
}

// Verify returns who a token belongs to, or ErrInvalid.
func (t *Tokens) Verify(token string) (*Identity, error) {
	encodedPayload, encodedSig, ok := strings.Cut(token, ".")
	if !ok {
		return nil, ErrInvalid
	}
	payload, err := tokenEncoding.DecodeString(encodedPayload)
	if err != nil {
		return nil, ErrInvalid
	}
	sig, err := tokenEncoding.DecodeString(encodedSig)
	if err != nil || !hmac.Equal(sig, t.sign(payload)) {
		return nil, ErrInvalid
	}
	var p tokenPayload
	if err := json.Unmarshal(payload, &p); err != nil || p.Version != 1 {
		return nil, ErrInvalid
	}
	expires := time.Unix(p.Expires, 0).UTC()
	if !t.now().Before(expires) {
		return nil, ErrInvalid
	}
	if (p.Role != RoleWallet && p.Role != RoleAdmin) || p.Subject == "" {
		return nil, ErrInvalid
	}
	return &Identity{Role: p.Role, Subject: p.Subject, ExpiresAt: expires}, nil
}
