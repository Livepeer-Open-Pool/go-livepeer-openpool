package payouts_test

import (
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"testing"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/stretchr/testify/require"

	"github.com/Livepeer-Open-Pool/pool/internal/core"
	"github.com/Livepeer-Open-Pool/pool/internal/ledger"
	"github.com/Livepeer-Open-Pool/pool/internal/payouts"
	"github.com/Livepeer-Open-Pool/pool/internal/store"
	"github.com/Livepeer-Open-Pool/pool/internal/testkit"
)

// eth is an amount in tenths of an ETH, in wei: eth(5) is 0.5 ETH.
func eth(tenths int64) *big.Int {
	return new(big.Int).Mul(big.NewInt(tenths), new(big.Int).Exp(big.NewInt(10), big.NewInt(17), nil))
}

type rounds struct {
	mu    sync.Mutex
	round int64
	err   error
}

func (r *rounds) CurrentRound(ctx context.Context) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.round, r.err
}

func (r *rounds) set(round int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.round = round
}

// pool is a payout service over a fresh database, a simulated chain with a
// funded payout wallet, a round counter and a clock the test controls.
type pool struct {
	t        *testing.T
	ctx      context.Context
	db       *store.DB
	ledger   *ledger.Ledger
	svc      *payouts.Service
	chain    *fakeChain
	key      *ecdsa.PrivateKey
	address  ethcommon.Address
	rounds   *rounds
	clock    *testkit.Clock
	mail     *testkit.Mailbox
	settings core.Settings
	next     int
}

func newPool(t *testing.T, funds *big.Int) *pool {
	t.Helper()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	p := &pool{
		t: t, ctx: context.Background(), db: testkit.OpenDB(t), key: key, address: crypto.PubkeyToAddress(key.PublicKey),
		rounds: &rounds{round: 4100}, clock: testkit.NewClock(), mail: &testkit.Mailbox{}, settings: core.DefaultSettings(),
	}
	p.chain = newFakeChain()
	p.chain.credit(p.address, funds)
	p.ledger = ledger.New(p.db, p.clock.Now, nil)
	p.svc = p.service(p.chain)
	return p
}

// service builds a payout service over the pool's database, as a restart would.
func (p *pool) service(chain payouts.Chain) *payouts.Service {
	return payouts.New(p.db, p.ledger, func(context.Context) (core.Settings, error) { return p.settings, nil }, p.rounds,
		payouts.NewWallet(chain, p.key, testChainID), p.mail, payouts.Config{
			RegionName: "US East", AdminURL: "https://admin.open-pool.com",
			SettleDelay: 10 * time.Minute, ConfirmWait: time.Millisecond, Now: p.clock.Now,
		}, nil)
}

// member adds a member and returns the address they are paid at.
func (p *pool) member(id, name, status string) ethcommon.Address {
	p.t.Helper()
	key, err := crypto.GenerateKey()
	require.NoError(p.t, err)
	address := crypto.PubkeyToAddress(key.PublicKey)
	_, err = p.db.Exec(`INSERT INTO members (id, address, display_name, status, created_at, updated_at) VALUES (?, ?, ?, ?, 0, 0)`,
		id, address.Hex(), name, status)
	require.NoError(p.t, err)
	return address
}

func (p *pool) event(eventType string, fields map[string]any) {
	p.t.Helper()
	p.next++
	e := map[string]any{"event_type": eventType, "event_id": fmt.Sprintf("evt-%d", p.next), "ts_ms": p.clock.Now().UnixMilli(), "round": 4100}
	for k, v := range fields {
		e[k] = v
	}
	raw, err := json.Marshal(e)
	require.NoError(p.t, err)
	require.NoError(p.t, p.ledger.Apply(p.ctx, raw))
}

func (p *pool) fee(member string, amount *big.Int, round int64) {
	p.t.Helper()
	p.event(ledger.EventFeeDebited, map[string]any{
		"runner_id": "runner_" + member, "member_id": member, "app": "echo", "session_id": fmt.Sprintf("s%d", p.next),
		"sender": "0xc1", "fee_wei": amount.String(), "round": round,
	})
}

func (p *pool) redeemed(amount *big.Int, round int64) {
	p.t.Helper()
	p.event(ledger.EventTicketRedeemed, map[string]any{"face_value_wei": amount.String(), "round": round})
}

func (p *pool) tick() {
	p.t.Helper()
	require.NoError(p.t, p.svc.Tick(p.ctx))
}

// closeWindow moves past the open window's last round and lets it settle.
func (p *pool) closeWindow(round int64) {
	p.t.Helper()
	p.rounds.set(round)
	p.tick() // notices the window can close
	p.clock.Advance(11 * time.Minute)
	p.tick() // closes it
}

// settle ticks, mining a block between passes, until the batch reaches the status.
func (p *pool) settle(svc *payouts.Service, batchID, status string) *payouts.Batch {
	p.t.Helper()
	for i := 0; i < 40; i++ {
		require.NoError(p.t, svc.Tick(p.ctx))
		p.chain.Commit()
		batch, err := svc.Batch(p.ctx, batchID)
		require.NoError(p.t, err)
		if batch.Status == status {
			return batch
		}
	}
	batch, err := svc.Batch(p.ctx, batchID)
	require.NoError(p.t, err)
	p.t.Fatalf("batch %s is %s, not %s: %+v", batchID, batch.Status, status, batch)
	return nil
}

func (p *pool) balance(address ethcommon.Address) *big.Int {
	p.t.Helper()
	balance, err := p.chain.BalanceAt(p.ctx, address, nil)
	require.NoError(p.t, err)
	return balance
}

func (p *pool) batch(id string) *payouts.Batch {
	p.t.Helper()
	batch, err := p.svc.Batch(p.ctx, id)
	require.NoError(p.t, err)
	return batch
}

func items(batch *payouts.Batch) map[string]payouts.Item {
	out := map[string]payouts.Item{}
	for _, it := range batch.Items {
		out[it.MemberID] = it
	}
	return out
}

func TestWindowClosesIntoABatchSplitByFees(t *testing.T) {
	p := newPool(t, eth(100))
	p.member("mbr_alice", "Alice", core.MemberActive)
	p.member("mbr_bob", "Bob", core.MemberActive)
	p.tick() // the first window starts with the first round the pool sees
	first, last, ok, err := p.svc.OpenWindow(p.ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []int64{4100, 4106}, []int64{first, last}, "seven rounds")

	// Alice earns 20% of the fees, Bob 80%; a static runner earns some too.
	p.fee("mbr_alice", big.NewInt(200), 4100)
	p.fee("mbr_bob", big.NewInt(500), 4102)
	p.fee("mbr_bob", big.NewInt(300), 4106)
	p.fee("operator", big.NewInt(50), 4101)
	// The node redeems half an ETH over the window.
	p.redeemed(eth(2), 4101)
	p.redeemed(eth(3), 4106)

	// The window is not closed while its last round is running, nor at once after.
	p.rounds.set(4106)
	p.tick()
	require.Equal(t, payouts.StatusOpen, p.batch("batch_4100").Status)
	p.rounds.set(4107)
	p.tick()
	p.clock.Advance(9 * time.Minute)
	p.tick()
	require.Equal(t, payouts.StatusOpen, p.batch("batch_4100").Status, "events still in flight are given time to arrive")
	p.clock.Advance(2 * time.Minute)
	p.tick()

	batch := p.batch("batch_4100")
	require.Equal(t, payouts.StatusReady, batch.Status)
	require.Equal(t, eth(5).String(), batch.PotWei)
	require.Equal(t, 1500, batch.CommissionBps)
	require.Equal(t, "425000000000000000", batch.PayableWei, "0.5 ETH less 15%")
	require.Equal(t, "1000", batch.TotalFeesWei)
	require.Equal(t, "50", batch.OtherFeesWei, "the static runner's fees are reported and not part of the split")
	got := items(batch)
	require.Len(t, got, 2)
	require.Equal(t, "85000000000000000", got["mbr_alice"].AmountWei, "0.425 × 20%")
	require.Equal(t, "340000000000000000", got["mbr_bob"].AmountWei, "0.425 × 80%")
	require.Equal(t, "200", got["mbr_alice"].FeesWei)
	require.True(t, got["mbr_alice"].Include)
	require.Equal(t, payouts.ItemPending, got["mbr_alice"].Status)

	// The operator is told, and the next window is open.
	notices := p.mail.AllNotices()
	require.Len(t, notices, 1)
	require.Equal(t, "Open Pool US East: payouts for rounds 4100 to 4106 need approval", notices[0].Subject)
	require.Contains(t, notices[0].Body, "https://admin.open-pool.com/batches/batch_4100")
	first, last, _, err = p.svc.OpenWindow(p.ctx)
	require.NoError(t, err)
	require.Equal(t, []int64{4107, 4113}, []int64{first, last})

	// Nothing is paid before the operator approves.
	p.tick()
	p.chain.Commit()
	require.Equal(t, eth(100), p.balance(p.address))
}

func TestApprovedBatchPaysEachMemberOnce(t *testing.T) {
	p := newPool(t, eth(100))
	alice := p.member("mbr_alice", "Alice", core.MemberActive)
	bob := p.member("mbr_bob", "Bob", core.MemberActive)
	p.tick()
	p.fee("mbr_alice", big.NewInt(200), 4100)
	p.fee("mbr_bob", big.NewInt(800), 4100)
	p.redeemed(eth(5), 4100)
	p.closeWindow(4107)

	_, err := p.svc.Approve(p.ctx, "admin:ops@open-pool.com", "batch_4100")
	require.NoError(t, err)
	batch := p.settle(p.svc, "batch_4100", payouts.StatusSent)

	require.Equal(t, "85000000000000000", p.balance(alice).String())
	require.Equal(t, "340000000000000000", p.balance(bob).String())
	for _, it := range batch.Items {
		require.Equal(t, payouts.ItemConfirmed, it.Status)
		require.Len(t, it.TxHash, 66)
	}
	require.NotNil(t, batch.SentAt)
	require.Equal(t, "admin:ops@open-pool.com", batch.ApprovedBy)
	// The pool paid the gas: the wallet is down by the payments and a little more.
	spent := new(big.Int).Sub(eth(100), p.balance(p.address))
	paid, _ := new(big.Int).SetString("425000000000000000", 10)
	require.Equal(t, 1, spent.Cmp(paid))
	nonce, err := p.chain.NonceAt(p.ctx, p.address, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(2), nonce, "two payments, two transactions")

	// More passes change nothing.
	for i := 0; i < 3; i++ {
		p.tick()
		p.chain.Commit()
	}
	require.Equal(t, "85000000000000000", p.balance(alice).String())

	payoutsForAlice, err := p.svc.MemberPayouts(p.ctx, "mbr_alice")
	require.NoError(t, err)
	require.Len(t, payoutsForAlice, 1)
	require.Equal(t, payouts.ItemConfirmed, payoutsForAlice[0].Status)
	require.Equal(t, int64(4100), payoutsForAlice[0].FirstRound)
	require.Equal(t, payouts.StatusSent, payoutsForAlice[0].BatchStatus)

	_, err = p.svc.Approve(p.ctx, "admin", "batch_4100")
	require.Error(t, err, "a batch is approved once")
}

func TestAutomaticApprovalPaysWithoutTheOperator(t *testing.T) {
	p := newPool(t, eth(100))
	p.settings.PayoutApproval = core.PayoutApprovalAutomatic
	p.settings.CommissionBps = 0
	alice := p.member("mbr_alice", "Alice", core.MemberActive)
	p.tick()
	p.fee("mbr_alice", big.NewInt(1), 4100)
	p.redeemed(eth(1), 4100)
	p.closeWindow(4107)

	batch := p.settle(p.svc, "batch_4100", payouts.StatusSent)
	require.Equal(t, "pool", batch.ApprovedBy)
	require.Equal(t, eth(1), p.balance(alice), "with no commission the whole pot goes to members")
	require.Empty(t, p.mail.AllNotices(), "nobody is asked to approve")
}

func TestBannedMemberIsNotPaidUnlessTheOperatorSaysSo(t *testing.T) {
	p := newPool(t, eth(100))
	alice := p.member("mbr_alice", "Alice", core.MemberActive)
	mallory := p.member("mbr_mallory", "Mallory", core.MemberBanned)
	p.tick()
	p.fee("mbr_alice", big.NewInt(500), 4100)
	p.fee("mbr_mallory", big.NewInt(500), 4100)
	p.redeemed(eth(10), 4100)
	p.closeWindow(4107)

	got := items(p.batch("batch_4100"))
	require.True(t, got["mbr_mallory"].Banned)
	require.False(t, got["mbr_mallory"].Include)
	require.Equal(t, got["mbr_alice"].AmountWei, got["mbr_mallory"].AmountWei, "her share is worked out like anyone's")

	// The operator may switch a payment on or off until the batch is approved.
	batch, err := p.svc.SetItemIncluded(p.ctx, "admin", "batch_4100", "mbr_mallory", true)
	require.NoError(t, err)
	require.True(t, items(batch)["mbr_mallory"].Include)
	_, err = p.svc.SetItemIncluded(p.ctx, "admin", "batch_4100", "mbr_mallory", false)
	require.NoError(t, err)
	_, err = p.svc.SetItemIncluded(p.ctx, "admin", "batch_4100", "mbr_nobody", false)
	require.Error(t, err)

	_, err = p.svc.Approve(p.ctx, "admin", "batch_4100")
	require.NoError(t, err)
	_, err = p.svc.SetItemIncluded(p.ctx, "admin", "batch_4100", "mbr_mallory", true)
	require.Error(t, err, "not after approval")
	p.settle(p.svc, "batch_4100", payouts.StatusSent)

	// Alice is paid her half; Mallory's stays in the payout wallet.
	require.Equal(t, "425000000000000000", p.balance(alice).String())
	require.Zero(t, p.balance(mallory).Sign())
	require.Equal(t, payouts.ItemPending, items(p.batch("batch_4100"))["mbr_mallory"].Status)
}

func TestWindowWithNothingToPayIsEmpty(t *testing.T) {
	p := newPool(t, eth(100))
	p.member("mbr_alice", "Alice", core.MemberActive)
	p.tick()
	// Fees but nothing redeemed: winning tickets are a lottery.
	p.fee("mbr_alice", big.NewInt(500), 4100)
	p.closeWindow(4107)
	require.Equal(t, payouts.StatusEmpty, p.batch("batch_4100").Status)
	require.Empty(t, p.mail.AllNotices())

	// The next window has a pot and nobody who earned fees.
	p.redeemed(eth(1), 4108)
	p.closeWindow(4114)
	require.Equal(t, payouts.StatusEmpty, p.batch("batch_4107").Status)
	require.Equal(t, eth(1).String(), p.batch("batch_4107").PotWei)
}

func TestEventArrivingAfterItsWindowClosedIsPaidInTheNext(t *testing.T) {
	p := newPool(t, eth(100))
	p.member("mbr_alice", "Alice", core.MemberActive)
	p.tick()
	p.fee("mbr_alice", big.NewInt(100), 4100)
	p.redeemed(eth(1), 4100)
	p.closeWindow(4107)
	require.Equal(t, "100", p.batch("batch_4100").TotalFeesWei)

	// Stragglers from round 4105: the first window is settled, so they count in the second.
	p.fee("mbr_alice", big.NewInt(40), 4105)
	p.redeemed(eth(2), 4105)
	p.closeWindow(4114)
	second := p.batch("batch_4107")
	require.Equal(t, "40", second.TotalFeesWei)
	require.Equal(t, eth(2).String(), second.PotWei)
	require.Equal(t, "100", p.batch("batch_4100").TotalFeesWei, "the settled window is not reopened")
}

func TestServiceThatWasDownClosesEveryWindowItMissed(t *testing.T) {
	p := newPool(t, eth(100))
	p.member("mbr_alice", "Alice", core.MemberActive)
	p.tick()
	p.fee("mbr_alice", big.NewInt(100), 4100)
	p.redeemed(eth(1), 4100)
	p.fee("mbr_alice", big.NewInt(100), 4110)
	p.redeemed(eth(2), 4110)

	// Three windows later.
	p.closeWindow(4125)
	batches, err := p.svc.Batches(p.ctx)
	require.NoError(t, err)
	statuses := map[string]string{}
	for _, b := range batches {
		statuses[b.ID] = b.Status
	}
	require.Equal(t, map[string]string{
		"batch_4100": payouts.StatusReady, "batch_4107": payouts.StatusReady, "batch_4114": payouts.StatusEmpty, "batch_4121": payouts.StatusOpen,
	}, statuses)
}

func TestWalletThatCannotCoverABatchBlocksItUntilFunded(t *testing.T) {
	p := newPool(t, eth(10)) // 1 ETH
	alice := p.member("mbr_alice", "Alice", core.MemberActive)
	bob := p.member("mbr_bob", "Bob", core.MemberActive)
	p.tick()
	p.fee("mbr_alice", big.NewInt(500), 4100)
	p.fee("mbr_bob", big.NewInt(500), 4100)
	p.redeemed(eth(20), 4100) // 2 ETH: 0.85 ETH each, and the wallet holds 1
	p.closeWindow(4107)
	_, err := p.svc.Approve(p.ctx, "admin", "batch_4100")
	require.NoError(t, err)

	// The first payment fits; the second does not.
	batch := p.settle(p.svc, "batch_4100", payouts.StatusBlocked)
	require.Contains(t, batch.Note, "add at least")
	require.Contains(t, batch.Note, p.address.Hex())
	require.Equal(t, "850000000000000000", p.balance(alice).String())
	require.Zero(t, p.balance(bob).Sign())

	wallet, err := p.svc.Wallet(p.ctx)
	require.NoError(t, err)
	require.True(t, wallet.Configured)
	require.Equal(t, p.address.Hex(), wallet.Address)
	require.Equal(t, "850000000000000000", wallet.OwedWei)

	// The operator funds the wallet; the batch finishes by itself, and Alice is not paid again.
	p.fund(eth(10))
	wallet, err = p.svc.Wallet(p.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, mustBig(t, wallet.BalanceWei).Cmp(eth(10)))
	p.settle(p.svc, "batch_4100", payouts.StatusSent)
	require.Equal(t, "850000000000000000", p.balance(bob).String())
	require.Equal(t, "850000000000000000", p.balance(alice).String())
}

func mustBig(t *testing.T, s string) *big.Int {
	t.Helper()
	n, ok := new(big.Int).SetString(s, 10)
	require.True(t, ok, s)
	return n
}

// fund adds ETH to the payout wallet, as the operator topping it up does.
func (p *pool) fund(amount *big.Int) {
	p.t.Helper()
	p.chain.credit(p.address, amount)
}

func TestPaymentIsNeverMadeTwiceAcrossARestart(t *testing.T) {
	for name, crash := range map[string]func(*flakyChain){
		// The service died after storing the signed transaction and before sending it.
		"before the transaction was sent": func(c *flakyChain) { c.failSends = 1 },
		// The service died after the network took the transaction and before recording that.
		"after the transaction was sent": func(c *flakyChain) { c.sendThenFail = 1 },
	} {
		t.Run(name, func(t *testing.T) {
			p := newPool(t, eth(100))
			alice := p.member("mbr_alice", "Alice", core.MemberActive)
			p.tick()
			p.fee("mbr_alice", big.NewInt(500), 4100)
			p.redeemed(eth(10), 4100)
			p.closeWindow(4107)
			_, err := p.svc.Approve(p.ctx, "admin", "batch_4100")
			require.NoError(t, err)

			flaky := &flakyChain{fakeChain: p.chain}
			crash(flaky)
			dying := p.service(flaky)
			_ = dying.Tick(p.ctx)
			p.chain.Commit()
			stored := items(p.batch("batch_4100"))["mbr_alice"]
			require.NotEmpty(t, stored.TxHash, "the transaction was stored before anything was sent")

			// A new process over the same database.
			restarted := p.service(p.chain)
			batch := p.settle(restarted, "batch_4100", payouts.StatusSent)
			require.Equal(t, stored.TxHash, items(batch)["mbr_alice"].TxHash, "the same transaction, not a second one")
			require.Equal(t, "850000000000000000", p.balance(alice).String(), "paid exactly once")
			nonce, err := p.chain.NonceAt(p.ctx, p.address, nil)
			require.NoError(t, err)
			require.Equal(t, uint64(1), nonce)
		})
	}
}

func TestPaymentThatCannotBeMadeFailsAndCanBeRetried(t *testing.T) {
	p := newPool(t, eth(100))
	alice := p.member("mbr_alice", "Alice", core.MemberActive)
	// Bob's payout address is a contract that refuses ETH.
	bob := p.member("mbr_bob", "Bob", core.MemberActive)
	p.chain.refusers[bob] = true
	p.tick()
	p.fee("mbr_alice", big.NewInt(500), 4100)
	p.fee("mbr_bob", big.NewInt(500), 4100)
	p.redeemed(eth(10), 4100)
	p.closeWindow(4107)
	_, err := p.svc.Approve(p.ctx, "admin", "batch_4100")
	require.NoError(t, err)

	batch := p.settle(p.svc, "batch_4100", payouts.StatusFailed)
	got := items(batch)
	require.Equal(t, payouts.ItemConfirmed, got["mbr_alice"].Status, "one member's failure does not hold up another's payment")
	require.Equal(t, payouts.ItemFailed, got["mbr_bob"].Status)
	require.Contains(t, got["mbr_bob"].Error, "the payment cannot be made")
	require.Equal(t, "425000000000000000", p.balance(alice).String())

	// Retrying tries the failed payment again and leaves the paid one alone.
	_, err = p.svc.Retry(p.ctx, "admin", "batch_4100")
	require.NoError(t, err)
	p.settle(p.svc, "batch_4100", payouts.StatusFailed)
	require.Equal(t, "425000000000000000", p.balance(alice).String())
	_, err = p.svc.Retry(p.ctx, "admin", "batch_4107")
	require.Error(t, err)
}

func TestWalletOnTheWrongChainSendsNothing(t *testing.T) {
	p := newPool(t, eth(100))
	alice := p.member("mbr_alice", "Alice", core.MemberActive)
	p.tick()
	p.fee("mbr_alice", big.NewInt(500), 4100)
	p.redeemed(eth(10), 4100)
	p.closeWindow(4107)
	_, err := p.svc.Approve(p.ctx, "admin", "batch_4100")
	require.NoError(t, err)

	// Configured for Arbitrum One, connected to something else.
	wrong := payouts.New(p.db, p.ledger, func(context.Context) (core.Settings, error) { return p.settings, nil }, p.rounds,
		payouts.NewWallet(p.chain, p.key, 42161), p.mail, payouts.Config{Now: p.clock.Now, ConfirmWait: time.Millisecond}, nil)
	require.ErrorContains(t, wrong.Tick(p.ctx), "is on chain 1337")
	p.chain.Commit()
	require.Zero(t, p.balance(alice).Sign())
	require.Equal(t, payouts.StatusApproved, p.batch("batch_4100").Status)
}

func TestWithoutAWalletBatchesAreBuiltAndNotSent(t *testing.T) {
	p := newPool(t, eth(100))
	p.member("mbr_alice", "Alice", core.MemberActive)
	noWallet := payouts.New(p.db, p.ledger, func(context.Context) (core.Settings, error) { return p.settings, nil }, p.rounds,
		nil, p.mail, payouts.Config{Now: p.clock.Now}, nil)
	require.NoError(t, noWallet.Tick(p.ctx))
	p.fee("mbr_alice", big.NewInt(500), 4100)
	p.redeemed(eth(10), 4100)
	p.rounds.set(4107)
	require.NoError(t, noWallet.Tick(p.ctx))
	p.clock.Advance(11 * time.Minute)
	require.NoError(t, noWallet.Tick(p.ctx))
	_, err := noWallet.Approve(p.ctx, "admin", "batch_4100")
	require.NoError(t, err)
	require.NoError(t, noWallet.Tick(p.ctx))
	require.Equal(t, payouts.StatusApproved, p.batch("batch_4100").Status)

	wallet, err := noWallet.Wallet(p.ctx)
	require.NoError(t, err)
	require.False(t, wallet.Configured)
	require.Equal(t, "850000000000000000", wallet.OwedWei)
}

func TestOrchestratorThatDoesNotSayTheRoundLeavesWindowsAlone(t *testing.T) {
	p := newPool(t, eth(100))
	p.rounds.err = errors.New("missing ETH client")
	require.Error(t, p.svc.Tick(p.ctx))
	batches, err := p.svc.Batches(p.ctx)
	require.NoError(t, err)
	require.Empty(t, batches)
}
