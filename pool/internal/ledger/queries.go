package ledger

import (
	"context"
	"math/big"
	"sort"
	"time"
)

// Earning is what a member's runners earned for one app in one round.
type Earning struct {
	Round    int64  `json:"round"`
	App      string `json:"app"`
	FeeWei   string `json:"fee_wei"`
	Sessions int64  `json:"sessions"`
}

// MemberEarnings returns a member's earnings in rounds first to last,
// newest round first. A last of zero means no upper bound.
func (l *Ledger) MemberEarnings(ctx context.Context, memberID string, first, last int64) ([]Earning, error) {
	query, args := `SELECT round, app, fee_wei, sessions FROM earnings WHERE member_id = ? AND round >= ?`, []any{memberID, first}
	if last > 0 {
		query, args = query+` AND round <= ?`, append(args, last)
	}
	rows, err := l.db.QueryContext(ctx, query+` ORDER BY round DESC, app`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Earning{}
	for rows.Next() {
		var e Earning
		if err := rows.Scan(&e.Round, &e.App, &e.FeeWei, &e.Sessions); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// MemberFees is what one party's runners earned over a span of rounds.
// Member is false for a runner that belongs to no pool member.
type MemberFees struct {
	MemberID string
	Fees     *big.Int
	Sessions int64
	Member   bool
}

// Fees sums what each party earned in rounds first to last. A last of zero
// means no upper bound.
func (l *Ledger) Fees(ctx context.Context, first, last int64) ([]MemberFees, error) {
	query, args := `SELECT e.member_id, e.fee_wei, e.sessions, m.id IS NOT NULL
		FROM earnings e LEFT JOIN members m ON m.id = e.member_id WHERE e.round >= ?`, []any{first}
	if last > 0 {
		query, args = query+` AND e.round <= ?`, append(args, last)
	}
	rows, err := l.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	byMember := map[string]*MemberFees{}
	for rows.Next() {
		var memberID, fee string
		var sessions int64
		var isMember bool
		if err := rows.Scan(&memberID, &fee, &sessions, &isMember); err != nil {
			return nil, err
		}
		amount, err := wei(fee)
		if err != nil {
			return nil, err
		}
		entry := byMember[memberID]
		if entry == nil {
			entry = &MemberFees{MemberID: memberID, Fees: new(big.Int), Member: isMember}
			byMember[memberID] = entry
		}
		entry.Fees.Add(entry.Fees, amount)
		entry.Sessions += sessions
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]MemberFees, 0, len(byMember))
	for _, entry := range byMember {
		out = append(out, *entry)
	}
	sort.Slice(out, func(i, j int) bool {
		if c := out[i].Fees.Cmp(out[j].Fees); c != 0 {
			return c > 0
		}
		return out[i].MemberID < out[j].MemberID
	})
	return out, nil
}

// Redeemed is the face value of the tickets the node redeemed in rounds
// first to last: the pot for that span.
func (l *Ledger) Redeemed(ctx context.Context, first, last int64) (*big.Int, error) {
	rows, err := l.db.QueryContext(ctx, `SELECT face_value_wei FROM redemptions WHERE round >= ? AND round <= ?`, first, last)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	sum := new(big.Int)
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		amount, err := wei(value)
		if err != nil {
			return nil, err
		}
		sum.Add(sum, amount)
	}
	return sum, rows.Err()
}

// LeaderboardEntry is one member's standing.
type LeaderboardEntry struct {
	Address     string `json:"address"`
	DisplayName string `json:"display_name"`
	FeesWei     string `json:"fees_wei"`
	Sessions    int64  `json:"sessions"`
}

// Leaderboard ranks members by what they earned from round first on, most
// first. Banned members and runners that belong to no member are left out.
func (l *Ledger) Leaderboard(ctx context.Context, first int64, limit int) ([]LeaderboardEntry, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	fees, err := l.Fees(ctx, first, 0)
	if err != nil {
		return nil, err
	}
	out := []LeaderboardEntry{}
	for _, f := range fees {
		if !f.Member || len(out) >= limit {
			continue
		}
		var entry LeaderboardEntry
		var status string
		if err := l.db.QueryRowContext(ctx, `SELECT address, display_name, status FROM members WHERE id = ?`, f.MemberID).
			Scan(&entry.Address, &entry.DisplayName, &status); err != nil {
			return nil, err
		}
		if status == "banned" {
			continue
		}
		entry.FeesWei, entry.Sessions = f.Fees.String(), f.Sessions
		out = append(out, entry)
	}
	return out, nil
}

// AppDemand is how much work an app saw over a span of time.
type AppDemand struct {
	App      string `json:"app"`
	Sessions int64  `json:"sessions"`
	FeesWei  string `json:"fees_wei"`
	// Refused is how many requests were turned away for lack of capacity.
	Refused int64 `json:"refused"`
}

// Demand sums each app's sessions, fees and refused requests since a time.
func (l *Ledger) Demand(ctx context.Context, since time.Time) (map[string]AppDemand, error) {
	rows, err := l.db.QueryContext(ctx, `SELECT app, fee_wei, sessions, refused FROM app_hours WHERE hour >= ?`, hourOf(since.UnixMilli()))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	fees := map[string]*big.Int{}
	out := map[string]AppDemand{}
	for rows.Next() {
		var app, fee string
		var sessions, refused int64
		if err := rows.Scan(&app, &fee, &sessions, &refused); err != nil {
			return nil, err
		}
		amount, err := wei(fee)
		if err != nil {
			return nil, err
		}
		if fees[app] == nil {
			fees[app] = new(big.Int)
		}
		fees[app].Add(fees[app], amount)
		d := out[app]
		d.App, d.Sessions, d.Refused = app, d.Sessions+sessions, d.Refused+refused
		out[app] = d
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for app, d := range out {
		d.FeesWei = fees[app].String()
		out[app] = d
	}
	return out, nil
}
