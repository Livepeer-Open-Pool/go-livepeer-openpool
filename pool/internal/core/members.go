package core

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"
)

// Member statuses.
const (
	MemberActive = "active"
	MemberBanned = "banned"
	MemberLeft   = "left"
)

// Member is an approved applicant. Its address is who it is and where it is paid.
type Member struct {
	ID          string    `json:"id"`
	Address     string    `json:"address"`
	DisplayName string    `json:"display_name"`
	Status      string    `json:"status"`
	Reason      string    `json:"reason,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

const memberColumns = `id, address, display_name, status, reason, created_at, updated_at`

func scanMember(row rowScanner) (*Member, error) {
	var m Member
	var created, updated int64
	if err := row.Scan(&m.ID, &m.Address, &m.DisplayName, &m.Status, &m.Reason, &created, &updated); err != nil {
		return nil, err
	}
	m.CreatedAt, m.UpdatedAt = fromMs(created), fromMs(updated)
	return &m, nil
}

// Member returns one member by id.
func (s *Service) Member(ctx context.Context, id string) (*Member, error) {
	m, err := scanMember(s.db.QueryRowContext(ctx, `SELECT `+memberColumns+` FROM members WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, notFoundf("member %s does not exist", id)
	}
	return m, err
}

// memberByAddress returns the member with address, or nil when there is none.
func (s *Service) memberByAddress(ctx context.Context, address string) (*Member, error) {
	m, err := scanMember(s.db.QueryRowContext(ctx, `SELECT `+memberColumns+` FROM members WHERE address = ?`, address))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return m, err
}

// MemberByAddress returns the member who signs in with address, or nil when
// that address is not a member in this region.
func (s *Service) MemberByAddress(ctx context.Context, address string) (*Member, error) {
	address, ok := normalizeAddress(address)
	if !ok {
		return nil, nil
	}
	return s.memberByAddress(ctx, address)
}

// Members lists members, newest first.
func (s *Service) Members(ctx context.Context) ([]Member, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+memberColumns+` FROM members ORDER BY created_at DESC, id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	members := []Member{}
	for rows.Next() {
		m, err := scanMember(rows)
		if err != nil {
			return nil, err
		}
		members = append(members, *m)
	}
	return members, rows.Err()
}

// BanMember bans a member: its machines are admitted no more, and its
// runners are removed from the orchestrator.
func (s *Service) BanMember(ctx context.Context, actor, id, reason string) (*Member, error) {
	member, err := s.Member(ctx, id)
	if err != nil {
		return nil, err
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return nil, invalidf("a ban needs a reason")
	}
	if member.Status == MemberBanned {
		return nil, conflictf("%s is already banned", member.DisplayName)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE members SET status = ?, reason = ?, updated_at = ? WHERE id = ?`, MemberBanned, reason, s.nowMs(), id); err != nil {
		return nil, err
	}
	if err := s.audit(ctx, s.db, actor, "member.ban", id, reason); err != nil {
		return nil, err
	}
	// The ban holds whether or not the orchestrator answers: a banned
	// member's runner that slips through is refused when it next registers.
	s.evictMemberRunners(ctx, id, "banned: "+reason)
	return s.Member(ctx, id)
}

// UnbanMember lifts a ban. The member's machines may register again.
func (s *Service) UnbanMember(ctx context.Context, actor, id string) (*Member, error) {
	member, err := s.Member(ctx, id)
	if err != nil {
		return nil, err
	}
	if member.Status != MemberBanned {
		return nil, conflictf("%s is not banned", member.DisplayName)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE members SET status = ?, reason = '', updated_at = ? WHERE id = ?`, MemberActive, s.nowMs(), id); err != nil {
		return nil, err
	}
	if err := s.audit(ctx, s.db, actor, "member.unban", id, ""); err != nil {
		return nil, err
	}
	return s.Member(ctx, id)
}
