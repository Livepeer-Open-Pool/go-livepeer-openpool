package core

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"strings"
	"time"
)

// Terms scopes.
const (
	TermsGlobal   = "global"   // the whole pool
	TermsRegional = "regional" // this region, on top of the global terms
)

// Terms is one published version of the terms of service.
type Terms struct {
	Scope       string    `json:"scope"`
	Version     int       `json:"version"`
	Body        string    `json:"body"`
	Hash        string    `json:"hash"`
	PublishedAt time.Time `json:"published_at"`
}

// CurrentTerms is the terms in force: the global ones and, when the region
// has any, its own.
type CurrentTerms struct {
	Global   *Terms `json:"global"`
	Regional *Terms `json:"regional"`
}

func termsHash(body string) string {
	sum := sha256.Sum256([]byte(body))
	return hex.EncodeToString(sum[:])
}

func (s *Service) latestTerms(ctx context.Context, scope string) (*Terms, error) {
	var t Terms
	var published int64
	err := s.db.QueryRowContext(ctx, `SELECT scope, version, body, hash, published_at FROM terms WHERE scope = ? ORDER BY version DESC LIMIT 1`, scope).
		Scan(&t.Scope, &t.Version, &t.Body, &t.Hash, &published)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	t.PublishedAt = fromMs(published)
	return &t, nil
}

// Terms returns the terms in force.
func (s *Service) Terms(ctx context.Context) (CurrentTerms, error) {
	global, err := s.latestTerms(ctx, TermsGlobal)
	if err != nil {
		return CurrentTerms{}, err
	}
	regional, err := s.latestTerms(ctx, TermsRegional)
	if err != nil {
		return CurrentTerms{}, err
	}
	return CurrentTerms{Global: global, Regional: regional}, nil
}

// PublishTerms publishes a new version of the terms for a scope; it applies
// from now. Publishing the text already in force changes nothing, which lets
// the admin site bring every region level by publishing the same text to all.
func (s *Service) PublishTerms(ctx context.Context, actor, scope, body string) (*Terms, error) {
	if scope != TermsGlobal && scope != TermsRegional {
		return nil, invalidf("terms scope must be %q or %q", TermsGlobal, TermsRegional)
	}
	if strings.TrimSpace(body) == "" {
		return nil, invalidf("terms must not be empty")
	}
	current, err := s.latestTerms(ctx, scope)
	if err != nil {
		return nil, err
	}
	hash := termsHash(body)
	if current != nil && current.Hash == hash {
		return current, nil
	}
	version := 1
	if current != nil {
		version = current.Version + 1
	}
	if _, err := s.db.ExecContext(ctx, `INSERT INTO terms (scope, version, body, hash, published_at) VALUES (?, ?, ?, ?, ?)`,
		scope, version, body, hash, s.nowMs()); err != nil {
		return nil, err
	}
	if err := s.audit(ctx, s.db, actor, "terms.publish", scope, hash); err != nil {
		return nil, err
	}
	return s.latestTerms(ctx, scope)
}
