package ledger_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Livepeer-Open-Pool/pool/internal/ledger"
	"github.com/Livepeer-Open-Pool/pool/internal/store"
	"github.com/Livepeer-Open-Pool/pool/internal/testkit"
)

type books struct {
	t      *testing.T
	ctx    context.Context
	db     *store.DB
	ledger *ledger.Ledger
	clock  *testkit.Clock
	next   int
}

func newBooks(t *testing.T) *books {
	t.Helper()
	b := &books{t: t, ctx: context.Background(), db: testkit.OpenDB(t), clock: testkit.NewClock()}
	b.ledger = ledger.New(b.db, b.clock.Now, nil)
	return b
}

// member adds a member row, since the ledger tells members from other runners by it.
func (b *books) member(id, name, status string) {
	b.t.Helper()
	_, err := b.db.Exec(`INSERT INTO members (id, address, display_name, status, created_at, updated_at) VALUES (?, ?, ?, ?, 0, 0)`,
		id, "0x"+fmt.Sprintf("%040x", len(id)+b.next), name, status)
	require.NoError(b.t, err)
	b.next++
}

// event builds an event as the orchestrator publishes it, with a fresh id.
func (b *books) event(eventType string, fields map[string]any) []byte {
	b.t.Helper()
	b.next++
	e := map[string]any{
		"event_type": eventType, "event_id": fmt.Sprintf("evt-%d", b.next), "ts_ms": b.clock.Now().UnixMilli(),
		"orch_address": "0xAA", "round": 4100,
	}
	for k, v := range fields {
		e[k] = v
	}
	raw, err := json.Marshal(e)
	require.NoError(b.t, err)
	return raw
}

func (b *books) apply(eventType string, fields map[string]any) {
	b.t.Helper()
	require.NoError(b.t, b.ledger.Apply(b.ctx, b.event(eventType, fields)))
}

func session(runner, member, app, id string, more map[string]any) map[string]any {
	fields := map[string]any{"runner_id": runner, "member_id": member, "app": app, "session_id": id, "sender": "0xc1"}
	for k, v := range more {
		fields[k] = v
	}
	return fields
}

func (b *books) earnings(member string) []ledger.Earning {
	b.t.Helper()
	earnings, err := b.ledger.MemberEarnings(b.ctx, member, 0, 0)
	require.NoError(b.t, err)
	return earnings
}

func TestFeesBecomeEarningsPerMemberAppAndRound(t *testing.T) {
	b := newBooks(t)
	b.apply(ledger.EventSessionReserved, session("runner_a", "mbr_alice", "echo", "s1", map[string]any{"paid": true}))
	b.apply(ledger.EventFeeDebited, session("runner_a", "mbr_alice", "echo", "s1", map[string]any{"fee_wei": "100", "units": 1}))
	b.apply(ledger.EventFeeDebited, session("runner_a", "mbr_alice", "echo", "s1", map[string]any{"fee_wei": "150", "units": 1}))
	// The round turns over mid-session: the fee is booked where it happened.
	b.apply(ledger.EventFeeDebited, session("runner_a", "mbr_alice", "echo", "s1", map[string]any{"fee_wei": "50", "round": 4101}))
	b.apply(ledger.EventSessionReleased, session("runner_a", "mbr_alice", "echo", "s1", map[string]any{
		"round": 4101, "reason": "stopped", "duration_ms": 9000, "fee_wei_total": "300", "paid": true}))
	// Another app and another member.
	b.apply(ledger.EventSessionReserved, session("runner_b", "mbr_alice", "vllm", "s2", map[string]any{"paid": true}))
	b.apply(ledger.EventFeeDebited, session("runner_b", "mbr_alice", "vllm", "s2", map[string]any{"fee_wei": "7"}))
	b.apply(ledger.EventFeeDebited, session("runner_c", "mbr_bob", "echo", "s3", map[string]any{"fee_wei": "1000"}))

	require.Equal(t, []ledger.Earning{
		{Round: 4101, App: "echo", FeeWei: "50"},
		{Round: 4100, App: "echo", FeeWei: "250", Sessions: 1},
		{Round: 4100, App: "vllm", FeeWei: "7", Sessions: 1},
	}, b.earnings("mbr_alice"))
	require.Equal(t, []ledger.Earning{{Round: 4100, App: "echo", FeeWei: "1000"}}, b.earnings("mbr_bob"))

	only4101, err := b.ledger.MemberEarnings(b.ctx, "mbr_alice", 4101, 4101)
	require.NoError(t, err)
	require.Len(t, only4101, 1)
}

func TestAnEventDeliveredTwiceCountsOnce(t *testing.T) {
	b := newBooks(t)
	fee := b.event(ledger.EventFeeDebited, session("runner_a", "mbr_alice", "echo", "s1", map[string]any{"fee_wei": "100"}))
	redeemed := b.event(ledger.EventTicketRedeemed, map[string]any{"face_value_wei": "5000", "tx_hash": "0x01", "sender": "0xc1"})
	for i := 0; i < 3; i++ {
		require.NoError(t, b.ledger.Apply(b.ctx, fee))
		require.NoError(t, b.ledger.Apply(b.ctx, redeemed))
	}
	require.Equal(t, "100", b.earnings("mbr_alice")[0].FeeWei)
	pot, err := b.ledger.Redeemed(b.ctx, 4100, 4100)
	require.NoError(t, err)
	require.Equal(t, "5000", pot.String())
}

func TestLostFeeEventsAreMadeUpFromTheSessionTotal(t *testing.T) {
	b := newBooks(t)
	// Three fees were debited; only one event arrived, then the total.
	b.apply(ledger.EventFeeDebited, session("runner_a", "mbr_alice", "echo", "s1", map[string]any{"fee_wei": "100"}))
	b.apply(ledger.EventSessionReleased, session("runner_a", "mbr_alice", "echo", "s1", map[string]any{"fee_wei_total": "300"}))
	require.Equal(t, "300", b.earnings("mbr_alice")[0].FeeWei)

	// A straggler arriving after the total is already covered by it.
	b.apply(ledger.EventFeeDebited, session("runner_a", "mbr_alice", "echo", "s1", map[string]any{"fee_wei": "100"}))
	require.Equal(t, "300", b.earnings("mbr_alice")[0].FeeWei)

	// A total alone, with no fee events at all.
	b.apply(ledger.EventSessionReleased, session("runner_a", "mbr_alice", "echo", "s2", map[string]any{"fee_wei_total": "40"}))
	require.Equal(t, "340", b.earnings("mbr_alice")[0].FeeWei)

	// A total smaller than the fee events (it never should be) takes nothing away.
	b.apply(ledger.EventFeeDebited, session("runner_a", "mbr_alice", "echo", "s3", map[string]any{"fee_wei": "10"}))
	b.apply(ledger.EventSessionReleased, session("runner_a", "mbr_alice", "echo", "s3", map[string]any{"fee_wei_total": "0"}))
	require.Equal(t, "350", b.earnings("mbr_alice")[0].FeeWei)
}

func TestAmountsLargerThanA64BitIntegerAreKeptExactly(t *testing.T) {
	b := newBooks(t)
	// 50 ETH twice: each alone overflows nothing, their sum overflows int64.
	fifty := "50000000000000000000"
	b.apply(ledger.EventFeeDebited, session("runner_a", "mbr_alice", "echo", "s1", map[string]any{"fee_wei": fifty}))
	b.apply(ledger.EventFeeDebited, session("runner_a", "mbr_alice", "echo", "s2", map[string]any{"fee_wei": fifty}))
	require.Equal(t, "100000000000000000000", b.earnings("mbr_alice")[0].FeeWei)
	b.apply(ledger.EventTicketRedeemed, map[string]any{"face_value_wei": fifty})
	b.apply(ledger.EventTicketRedeemed, map[string]any{"face_value_wei": fifty})
	pot, err := b.ledger.Redeemed(b.ctx, 4100, 4100)
	require.NoError(t, err)
	require.Equal(t, "100000000000000000000", pot.String())
}

func TestThePoolsOwnTestsAreNotWork(t *testing.T) {
	b := newBooks(t)
	b.apply(ledger.EventSessionReserved, session("runner_a", "mbr_alice", "echo", "probe1", map[string]any{"paid": false, "sender": ""}))
	b.apply(ledger.EventSessionReleased, session("runner_a", "mbr_alice", "echo", "probe1", map[string]any{"paid": false, "sender": "", "fee_wei_total": "0"}))
	require.Empty(t, b.earnings("mbr_alice"))
	demand, err := b.ledger.Demand(b.ctx, b.clock.Now().Add(-time.Hour))
	require.NoError(t, err)
	require.Empty(t, demand)
}

func TestRedeemedTicketsAreThePot(t *testing.T) {
	b := newBooks(t)
	b.apply(ledger.EventTicketRedeemed, map[string]any{"face_value_wei": "5000", "round": 4100})
	b.apply(ledger.EventTicketRedeemed, map[string]any{"face_value_wei": "7000", "round": 4103})
	b.apply(ledger.EventTicketRedeemed, map[string]any{"face_value_wei": "900", "round": 4107})

	window, err := b.ledger.Redeemed(b.ctx, 4100, 4106)
	require.NoError(t, err)
	require.Equal(t, "12000", window.String())
	next, err := b.ledger.Redeemed(b.ctx, 4107, 4113)
	require.NoError(t, err)
	require.Equal(t, "900", next.String())
}

func TestFeesTellMembersFromOtherRunners(t *testing.T) {
	b := newBooks(t)
	b.member("mbr_alice", "Alice", "active")
	b.member("mbr_bob", "Bob", "active")
	b.member("mbr_mallory", "Mallory", "banned")
	b.apply(ledger.EventSessionReserved, session("runner_a", "mbr_alice", "echo", "s1", map[string]any{"paid": true}))
	b.apply(ledger.EventFeeDebited, session("runner_a", "mbr_alice", "echo", "s1", map[string]any{"fee_wei": "300"}))
	b.apply(ledger.EventFeeDebited, session("runner_b", "mbr_bob", "echo", "s2", map[string]any{"fee_wei": "700"}))
	b.apply(ledger.EventFeeDebited, session("runner_b", "mbr_bob", "vllm", "s3", map[string]any{"fee_wei": "50", "round": 4101}))
	b.apply(ledger.EventFeeDebited, session("runner_m", "mbr_mallory", "echo", "s4", map[string]any{"fee_wei": "9000"}))
	// A static runner the operator left in the orchestrator's configuration.
	b.apply(ledger.EventFeeDebited, session("runner_s", "operator", "echo", "s5", map[string]any{"fee_wei": "20"}))

	fees, err := b.ledger.Fees(b.ctx, 4100, 4106)
	require.NoError(t, err)
	got := map[string]string{}
	members := map[string]bool{}
	for _, f := range fees {
		got[f.MemberID], members[f.MemberID] = f.Fees.String(), f.Member
	}
	require.Equal(t, map[string]string{"mbr_alice": "300", "mbr_bob": "750", "mbr_mallory": "9000", "operator": "20"}, got)
	require.Equal(t, map[string]bool{"mbr_alice": true, "mbr_bob": true, "mbr_mallory": true, "operator": false}, members)
	require.Equal(t, "mbr_mallory", fees[0].MemberID, "most fees first")

	// The leaderboard shows members only, and not the banned.
	board, err := b.ledger.Leaderboard(b.ctx, 0, 0)
	require.NoError(t, err)
	require.Len(t, board, 2)
	require.Equal(t, "Bob", board[0].DisplayName)
	require.Equal(t, "750", board[0].FeesWei)
	require.Equal(t, "Alice", board[1].DisplayName)
	require.Equal(t, int64(1), board[1].Sessions)
	// From a later round on, only what was earned since.
	board, err = b.ledger.Leaderboard(b.ctx, 4101, 0)
	require.NoError(t, err)
	require.Len(t, board, 1)
	require.Equal(t, "50", board[0].FeesWei)
}

func TestDemandCountsSessionsFeesAndRefusedRequestsPerApp(t *testing.T) {
	b := newBooks(t)
	b.apply(ledger.EventSessionReserved, session("runner_a", "mbr_alice", "echo", "s1", map[string]any{"paid": true}))
	b.apply(ledger.EventFeeDebited, session("runner_a", "mbr_alice", "echo", "s1", map[string]any{"fee_wei": "300"}))
	b.apply(ledger.EventRequestsRefused, map[string]any{"app": "echo", "count": 4})
	b.apply(ledger.EventRequestsRefused, map[string]any{"app": "echo", "count": 2})
	b.apply(ledger.EventRequestsRefused, map[string]any{"app": "vllm", "count": 1})

	// A day and a bit later, more of the same.
	b.clock.Advance(25 * time.Hour)
	b.apply(ledger.EventSessionReserved, session("runner_a", "mbr_alice", "echo", "s2", map[string]any{"paid": true}))
	b.apply(ledger.EventRequestsRefused, map[string]any{"app": "echo", "count": 10})

	last24h, err := b.ledger.Demand(b.ctx, b.clock.Now().Add(-24*time.Hour))
	require.NoError(t, err)
	require.Equal(t, map[string]ledger.AppDemand{"echo": {App: "echo", Sessions: 1, FeesWei: "0", Refused: 10}}, last24h)
	all, err := b.ledger.Demand(b.ctx, b.clock.Now().Add(-48*time.Hour))
	require.NoError(t, err)
	require.Equal(t, ledger.AppDemand{App: "echo", Sessions: 2, FeesWei: "300", Refused: 16}, all["echo"])
	require.Equal(t, int64(1), all["vllm"].Refused)
}

func TestEventsTheLedgerCannotUseAreRefusedOrIgnored(t *testing.T) {
	b := newBooks(t)
	// Registrations and state changes are not bookkeeping.
	b.apply("live_runner_registered", map[string]any{"runner_id": "runner_a", "member_id": "mbr_alice", "app": "echo"})
	b.apply("live_runner_state_changed", map[string]any{"runner_id": "runner_a", "from": "probation", "to": "active"})

	malformed := [][]byte{
		[]byte("not json"),
		[]byte(`{"event_type":"live_runner_fee_debited"}`),
		b.event(ledger.EventFeeDebited, map[string]any{"runner_id": "runner_a", "member_id": "mbr_alice", "app": "echo", "fee_wei": "100"}),
		b.event(ledger.EventFeeDebited, session("runner_a", "mbr_alice", "echo", "s1", map[string]any{"fee_wei": "1.5"})),
		b.event(ledger.EventFeeDebited, session("runner_a", "mbr_alice", "echo", "s1", map[string]any{"fee_wei": "-5"})),
		b.event(ledger.EventTicketRedeemed, map[string]any{"face_value_wei": "5000", "round": 0}),
		b.event(ledger.EventRequestsRefused, map[string]any{"app": "echo", "count": 0}),
	}
	for _, raw := range malformed {
		require.ErrorIs(t, b.ledger.Apply(b.ctx, raw), ledger.ErrMalformed, string(raw))
	}
	require.Empty(t, b.earnings("mbr_alice"))

	// A malformed event leaves no trace, so a corrected one with its id is taken.
	bad := b.event(ledger.EventFeeDebited, session("runner_a", "mbr_alice", "echo", "s1", map[string]any{"fee_wei": "x", "event_id": "fixed"}))
	require.ErrorIs(t, b.ledger.Apply(b.ctx, bad), ledger.ErrMalformed)
	good := b.event(ledger.EventFeeDebited, session("runner_a", "mbr_alice", "echo", "s1", map[string]any{"fee_wei": "5", "event_id": "fixed"}))
	require.NoError(t, b.ledger.Apply(b.ctx, good))
	require.Equal(t, "5", b.earnings("mbr_alice")[0].FeeWei)
}
