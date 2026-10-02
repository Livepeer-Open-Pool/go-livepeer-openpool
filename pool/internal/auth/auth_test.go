package auth_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Livepeer-Open-Pool/pool/internal/auth"
	"github.com/Livepeer-Open-Pool/pool/internal/testkit"
)

const testSecret = "a-session-signing-secret-of-enough-length"

func newTokens(t *testing.T, clock *testkit.Clock) *auth.Tokens {
	t.Helper()
	tokens, err := auth.NewTokens(testSecret, 12*time.Hour, clock.Now)
	require.NoError(t, err)
	return tokens
}

func TestTokensRoundTripAndExpire(t *testing.T) {
	clock := testkit.NewClock()
	tokens := newTokens(t, clock)

	token, expires := tokens.Issue(auth.RoleWallet, "0xAbC")
	id, err := tokens.Verify(token)
	require.NoError(t, err)
	require.Equal(t, &auth.Identity{Role: auth.RoleWallet, Subject: "0xAbC", ExpiresAt: expires}, id)
	require.Equal(t, clock.Now().Add(12*time.Hour), expires)

	clock.Advance(12*time.Hour - time.Second)
	_, err = tokens.Verify(token)
	require.NoError(t, err)
	clock.Advance(time.Second)
	_, err = tokens.Verify(token)
	require.ErrorIs(t, err, auth.ErrInvalid)
}

func TestTokensRejectAnythingNotSignedWithTheSecret(t *testing.T) {
	clock := testkit.NewClock()
	tokens := newTokens(t, clock)
	token, _ := tokens.Issue(auth.RoleWallet, "0xAbC")

	// A token for a member cannot be edited into one for the operator.
	payload, sig, _ := strings.Cut(token, ".")
	admin, _ := tokens.Issue(auth.RoleAdmin, "ops@example.com")
	adminPayload, _, _ := strings.Cut(admin, ".")
	for _, forged := range []string{"", "garbage", payload, payload + ".", adminPayload + "." + sig, token + "x", "." + sig} {
		_, err := tokens.Verify(forged)
		require.ErrorIs(t, err, auth.ErrInvalid, forged)
	}

	// Another region with the same secret accepts it; one with another secret does not.
	sameSecret, err := auth.NewTokens(testSecret, time.Hour, clock.Now)
	require.NoError(t, err)
	_, err = sameSecret.Verify(token)
	require.NoError(t, err)
	otherSecret, err := auth.NewTokens(strings.Repeat("z", 40), time.Hour, clock.Now)
	require.NoError(t, err)
	_, err = otherSecret.Verify(token)
	require.ErrorIs(t, err, auth.ErrInvalid)

	_, err = auth.NewTokens("short", time.Hour, nil)
	require.Error(t, err)
}

func TestRecoverSigner(t *testing.T) {
	wallet := testkit.NewWallet(t)
	sig := wallet.Sign(t, "hello")

	signer, err := auth.RecoverSigner("hello", sig)
	require.NoError(t, err)
	require.Equal(t, wallet.Address, signer)

	// A signature over other text recovers to some other address.
	signer, err = auth.RecoverSigner("hello!", sig)
	require.NoError(t, err)
	require.NotEqual(t, wallet.Address, signer)

	for _, bad := range []string{"", "0x", "0x1234", "not hex", sig[:len(sig)-2] + "ff"} {
		_, err := auth.RecoverSigner("hello", bad)
		require.Error(t, err, bad)
	}
}

func TestParseSignInMessage(t *testing.T) {
	wallet := testkit.NewWallet(t)
	issued := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	m, err := auth.ParseSignInMessage(wallet.SignInMessage("app.open-pool.com", 42161, "abc123", issued))
	require.NoError(t, err)
	require.Equal(t, "app.open-pool.com", m.Domain)
	require.Equal(t, wallet.Address, m.Address)
	require.Equal(t, "Sign in to Open Pool.", m.Statement)
	require.Equal(t, "https://app.open-pool.com", m.URI)
	require.Equal(t, "1", m.Version)
	require.Equal(t, 42161, m.ChainID)
	require.Equal(t, "abc123", m.Nonce)
	require.True(t, issued.Equal(m.IssuedAt))
	require.Nil(t, m.ExpirationTime)

	// Without a statement, and with the optional fields.
	noStatement := "app.open-pool.com wants you to sign in with your Ethereum account:\n" + wallet.Address +
		"\n\n\nURI: https://app.open-pool.com\nVersion: 1\nChain ID: 42161\nNonce: n1\nIssued At: 2026-10-01T12:00:00Z\n" +
		"Expiration Time: 2026-10-01T13:00:00Z\nResources:\n- https://example.com/a"
	m, err = auth.ParseSignInMessage(noStatement)
	require.NoError(t, err)
	require.Empty(t, m.Statement)
	require.NotNil(t, m.ExpirationTime)

	for name, bad := range map[string]string{
		"not a sign-in message": "hello",
		"no address":            "app.open-pool.com wants you to sign in with your Ethereum account:\nnobody\n\n\nURI: x\nVersion: 1\nChain ID: 1\nNonce: n\nIssued At: 2026-10-01T12:00:00Z",
		"no nonce":              strings.Replace(noStatement, "Nonce: n1\n", "", 1),
		"no chain id":           strings.Replace(noStatement, "Chain ID: 42161\n", "", 1),
		"repeated field":        strings.Replace(noStatement, "Nonce: n1\n", "Nonce: n1\nNonce: n2\n", 1),
		"scheme in domain":      "https://" + noStatement,
	} {
		_, err := auth.ParseSignInMessage(bad)
		require.Error(t, err, name)
	}
}

func newWalletSignIn(t *testing.T) (*auth.Wallet, *auth.Tokens, *testkit.Clock) {
	t.Helper()
	clock := testkit.NewClock()
	tokens := newTokens(t, clock)
	return auth.NewWallet(testkit.OpenDB(t), tokens, []string{"app.open-pool.com"}, 42161, clock.Now), tokens, clock
}

func TestWalletSignIn(t *testing.T) {
	signIn, tokens, clock := newWalletSignIn(t)
	wallet := testkit.NewWallet(t)
	ctx := context.Background()

	nonce, err := signIn.Nonce(ctx)
	require.NoError(t, err)
	message := wallet.SignInMessage("app.open-pool.com", 42161, nonce, clock.Now())
	token, id, err := signIn.SignIn(ctx, message, wallet.Sign(t, message))
	require.NoError(t, err)
	require.Equal(t, wallet.Address, id.Subject)
	verified, err := tokens.Verify(token)
	require.NoError(t, err)
	require.Equal(t, auth.RoleWallet, verified.Role)
	require.Equal(t, wallet.Address, verified.Subject)

	// The same signed message cannot be used again: its nonce is spent.
	_, _, err = signIn.SignIn(ctx, message, wallet.Sign(t, message))
	require.ErrorIs(t, err, auth.ErrInvalid)
}

func TestWalletSignInRefusals(t *testing.T) {
	signIn, _, clock := newWalletSignIn(t)
	wallet, other := testkit.NewWallet(t), testkit.NewWallet(t)
	ctx := context.Background()
	freshNonce := func() string {
		nonce, err := signIn.Nonce(ctx)
		require.NoError(t, err)
		return nonce
	}
	refused := func(name, message, signature string) {
		t.Helper()
		_, _, err := signIn.SignIn(ctx, message, signature)
		require.ErrorIs(t, err, auth.ErrInvalid, name)
	}

	message := wallet.SignInMessage("evil.example", 42161, freshNonce(), clock.Now())
	refused("a message for another site", message, wallet.Sign(t, message))

	message = wallet.SignInMessage("app.open-pool.com", 1, freshNonce(), clock.Now())
	refused("another chain", message, wallet.Sign(t, message))

	message = wallet.SignInMessage("app.open-pool.com", 42161, "never-issued", clock.Now())
	refused("a nonce this service never issued", message, wallet.Sign(t, message))

	message = wallet.SignInMessage("app.open-pool.com", 42161, freshNonce(), clock.Now())
	refused("signed by someone else", message, other.Sign(t, message))
	refused("not a signature", message, "0x1234")

	message = wallet.SignInMessage("app.open-pool.com", 42161, freshNonce(), clock.Now().Add(-16*time.Minute))
	refused("signed too long ago", message, wallet.Sign(t, message))

	message = wallet.SignInMessage("app.open-pool.com", 42161, freshNonce(), clock.Now().Add(10*time.Minute))
	refused("dated in the future", message, wallet.Sign(t, message))

	// A nonce expires unused.
	nonce := freshNonce()
	message = wallet.SignInMessage("app.open-pool.com", 42161, nonce, clock.Now())
	signature := wallet.Sign(t, message)
	clock.Advance(11 * time.Minute)
	refused("an expired nonce", message, signature)

	// A wrong signature does not spend the nonce: the rightful signer can still use it.
	nonce = freshNonce()
	message = wallet.SignInMessage("app.open-pool.com", 42161, nonce, clock.Now())
	refused("someone else's signature", message, other.Sign(t, message))
	_, _, err := signIn.SignIn(ctx, message, wallet.Sign(t, message))
	require.NoError(t, err)
}

func newAdminSignIn(t *testing.T) (*auth.Admin, *auth.Tokens, *testkit.Mailbox, *testkit.Clock) {
	t.Helper()
	clock := testkit.NewClock()
	tokens := newTokens(t, clock)
	mailbox := &testkit.Mailbox{}
	return auth.NewAdmin(testkit.OpenDB(t), tokens, mailbox, "Ops@Open-Pool.com", clock.Now), tokens, mailbox, clock
}

func TestAdminSignInWithEmailedCode(t *testing.T) {
	admin, tokens, mailbox, _ := newAdminSignIn(t)
	ctx := context.Background()

	// Asking with any other address sends nothing and says nothing.
	require.NoError(t, admin.RequestCode(ctx, "someone@example.com"))
	require.Zero(t, mailbox.Codes())

	require.NoError(t, admin.RequestCode(ctx, " ops@open-pool.com "))
	code := mailbox.LastCode()
	require.Regexp(t, `^\d{6}$`, code)

	token, id, err := admin.SignIn(ctx, "ops@open-pool.com", code)
	require.NoError(t, err)
	require.Equal(t, auth.RoleAdmin, id.Role)
	verified, err := tokens.Verify(token)
	require.NoError(t, err)
	require.Equal(t, auth.RoleAdmin, verified.Role)
	require.Equal(t, "ops@open-pool.com", verified.Subject)

	// A code works once.
	_, _, err = admin.SignIn(ctx, "ops@open-pool.com", code)
	require.ErrorIs(t, err, auth.ErrInvalid)
}

func TestAdminSignInRefusals(t *testing.T) {
	admin, _, mailbox, clock := newAdminSignIn(t)
	ctx := context.Background()

	_, _, err := admin.SignIn(ctx, "ops@open-pool.com", "000000")
	require.ErrorIs(t, err, auth.ErrInvalid, "no code was ever sent")

	require.NoError(t, admin.RequestCode(ctx, "ops@open-pool.com"))
	code := mailbox.LastCode()
	require.ErrorIs(t, admin.RequestCode(ctx, "ops@open-pool.com"), auth.ErrTooSoon)
	require.Equal(t, 1, mailbox.Codes())

	_, _, err = admin.SignIn(ctx, "someone@example.com", code)
	require.ErrorIs(t, err, auth.ErrInvalid, "the right code from the wrong address")

	// Five wrong guesses kill the code, even for the right guess after them.
	wrong := "000000"
	if code == wrong {
		wrong = "111111"
	}
	for i := 0; i < 5; i++ {
		_, _, err = admin.SignIn(ctx, "ops@open-pool.com", wrong)
		require.ErrorIs(t, err, auth.ErrInvalid)
	}
	_, _, err = admin.SignIn(ctx, "ops@open-pool.com", code)
	require.ErrorIs(t, err, auth.ErrInvalid)

	// A new code after the wait works; an old one expires.
	clock.Advance(time.Minute)
	require.NoError(t, admin.RequestCode(ctx, "ops@open-pool.com"))
	second := mailbox.LastCode()
	clock.Advance(10*time.Minute + time.Second)
	_, _, err = admin.SignIn(ctx, "ops@open-pool.com", second)
	require.ErrorIs(t, err, auth.ErrInvalid, "an expired code")

	require.NoError(t, admin.RequestCode(ctx, "ops@open-pool.com"))
	_, _, err = admin.SignIn(ctx, "ops@open-pool.com", mailbox.LastCode())
	require.NoError(t, err)
}
