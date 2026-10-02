package auth

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/accounts"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/Livepeer-Open-Pool/pool/internal/store"
)

// RecoverSigner returns the address whose key signed message with
// personal_sign (EIP-191), in checksum form.
func RecoverSigner(message, signature string) (string, error) {
	sig, err := hexutil.Decode(signature)
	if err != nil || len(sig) != crypto.SignatureLength {
		return "", errors.New("signature must be 65 bytes of hex")
	}
	sig = append([]byte(nil), sig...)
	// Wallets put 27 or 28 in the recovery byte; the library wants 0 or 1.
	if sig[crypto.RecoveryIDOffset] >= 27 {
		sig[crypto.RecoveryIDOffset] -= 27
	}
	if sig[crypto.RecoveryIDOffset] > 1 {
		return "", errors.New("signature has an invalid recovery byte")
	}
	pub, err := crypto.SigToPub(accounts.TextHash([]byte(message)), sig)
	if err != nil {
		return "", errors.New("signature does not recover to a key")
	}
	return crypto.PubkeyToAddress(*pub).Hex(), nil
}

// SignInMessage is a parsed "Sign-In with Ethereum" message (EIP-4361).
type SignInMessage struct {
	Domain         string
	Address        string
	Statement      string
	URI            string
	Version        string
	ChainID        int
	Nonce          string
	IssuedAt       time.Time
	ExpirationTime *time.Time
}

const signInHeaderSuffix = " wants you to sign in with your Ethereum account:"

// ParseSignInMessage reads an EIP-4361 message. It is strict about the
// fields this service checks and ignores the optional ones it does not use.
func ParseSignInMessage(message string) (*SignInMessage, error) {
	lines := strings.Split(message, "\n")
	if len(lines) < 7 || !strings.HasSuffix(lines[0], signInHeaderSuffix) {
		return nil, errors.New("not a sign-in message")
	}
	m := &SignInMessage{Domain: strings.TrimSuffix(lines[0], signInHeaderSuffix), Address: lines[1]}
	if m.Domain == "" || strings.Contains(m.Domain, "://") {
		return nil, errors.New("sign-in message has no plain domain")
	}
	if !ethcommon.IsHexAddress(m.Address) || lines[2] != "" {
		return nil, errors.New("sign-in message has no address")
	}
	rest := lines[3:]
	if rest[0] != "" {
		// A statement, followed by the blank line that ends it.
		if len(rest) < 2 || rest[1] != "" {
			return nil, errors.New("sign-in message statement is not followed by a blank line")
		}
		m.Statement, rest = rest[0], rest[2:]
	} else {
		rest = rest[1:]
	}
	fields := map[string]string{}
	for _, line := range rest {
		if strings.HasPrefix(line, "- ") || line == "Resources:" {
			continue
		}
		key, value, ok := strings.Cut(line, ": ")
		if !ok {
			return nil, fmt.Errorf("sign-in message line %q is not a field", line)
		}
		if _, repeated := fields[key]; repeated {
			return nil, fmt.Errorf("sign-in message repeats %q", key)
		}
		fields[key] = value
	}
	var err error
	m.URI, m.Version, m.Nonce = fields["URI"], fields["Version"], fields["Nonce"]
	if m.URI == "" || m.Nonce == "" {
		return nil, errors.New("sign-in message lacks a URI or nonce")
	}
	if m.ChainID, err = strconv.Atoi(fields["Chain ID"]); err != nil {
		return nil, errors.New("sign-in message lacks a chain id")
	}
	if m.IssuedAt, err = time.Parse(time.RFC3339, fields["Issued At"]); err != nil {
		return nil, errors.New("sign-in message lacks an issue time")
	}
	if value, ok := fields["Expiration Time"]; ok {
		expires, err := time.Parse(time.RFC3339, value)
		if err != nil {
			return nil, errors.New("sign-in message has a malformed expiration time")
		}
		m.ExpirationTime = &expires
	}
	return m, nil
}

const (
	nonceTTL = 10 * time.Minute
	// maxSignInAge bounds how old a signed message may be, and how far ahead
	// of this server's clock the signer's may run.
	maxSignInAge  = 15 * time.Minute
	maxClockAhead = 5 * time.Minute
)

// Wallet signs members in with an Ethereum wallet.
type Wallet struct {
	db      *store.DB
	tokens  *Tokens
	domains map[string]bool
	chainID int
	now     func() time.Time
}

// NewWallet returns a wallet sign-in for messages addressed to one of
// domains on chainID.
func NewWallet(db *store.DB, tokens *Tokens, domains []string, chainID int, now func() time.Time) *Wallet {
	if now == nil {
		now = time.Now
	}
	allowed := map[string]bool{}
	for _, d := range domains {
		allowed[strings.TrimSpace(d)] = true
	}
	return &Wallet{db: db, tokens: tokens, domains: allowed, chainID: chainID, now: now}
}

// Nonce returns a one-time value the client puts in the message it signs,
// so a captured signature cannot be replayed.
func (w *Wallet) Nonce(ctx context.Context) (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	nonce := hex.EncodeToString(b)
	now := w.now()
	if _, err := w.db.ExecContext(ctx, `DELETE FROM nonces WHERE expires_at < ?`, now.UnixMilli()); err != nil {
		return "", err
	}
	if _, err := w.db.ExecContext(ctx, `INSERT INTO nonces (nonce, expires_at) VALUES (?, ?)`, nonce, now.Add(nonceTTL).UnixMilli()); err != nil {
		return "", err
	}
	return nonce, nil
}

// SignIn verifies a signed sign-in message and returns a session token for
// the address that signed it. Every failure is ErrInvalid.
func (w *Wallet) SignIn(ctx context.Context, message, signature string) (string, *Identity, error) {
	m, err := ParseSignInMessage(message)
	if err != nil {
		return "", nil, ErrInvalid
	}
	now := w.now()
	if !w.domains[m.Domain] || m.Version != "1" || m.ChainID != w.chainID {
		return "", nil, ErrInvalid
	}
	if m.IssuedAt.After(now.Add(maxClockAhead)) || now.Sub(m.IssuedAt) > maxSignInAge {
		return "", nil, ErrInvalid
	}
	if m.ExpirationTime != nil && !now.Before(*m.ExpirationTime) {
		return "", nil, ErrInvalid
	}
	signer, err := RecoverSigner(message, signature)
	if err != nil || !strings.EqualFold(signer, m.Address) {
		return "", nil, ErrInvalid
	}
	// The nonce is spent last and atomically, so only a message that is
	// otherwise good uses it up, and only once.
	res, err := w.db.ExecContext(ctx, `DELETE FROM nonces WHERE nonce = ? AND expires_at >= ?`, m.Nonce, now.UnixMilli())
	if err != nil {
		return "", nil, err
	}
	if spent, err := res.RowsAffected(); err != nil || spent != 1 {
		return "", nil, ErrInvalid
	}
	token, expires := w.tokens.Issue(RoleWallet, signer)
	return token, &Identity{Role: RoleWallet, Subject: signer, ExpiresAt: expires}, nil
}
