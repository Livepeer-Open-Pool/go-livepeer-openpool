package mail

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

type sentEmail struct {
	path, auth string
	body       map[string]any
}

// fakeResend is a Resend-compatible API at a custom base URL.
func fakeResend(t *testing.T) (*httptest.Server, func() []sentEmail) {
	t.Helper()
	var mu sync.Mutex
	var sent []sentEmail
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(data, &body)
		mu.Lock()
		sent = append(sent, sentEmail{r.URL.Path, r.Header.Get("Authorization"), body})
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"email_1"}`)
	}))
	t.Cleanup(srv.Close)
	return srv, func() []sentEmail {
		mu.Lock()
		defer mu.Unlock()
		return append([]sentEmail(nil), sent...)
	}
}

func TestMailerSendsThroughTheConfiguredResendAPI(t *testing.T) {
	srv, sent := fakeResend(t)
	// With and without a trailing slash, and with a path prefix, as a
	// self-hosted Resend-compatible service might have.
	for _, base := range []string{srv.URL, srv.URL + "/", srv.URL + "/api"} {
		mailer, err := NewResend(base, "re_test_key", "Open Pool <pool@open-pool.com>", "ops@open-pool.com")
		require.NoError(t, err)
		require.NoError(t, mailer.NotifyAdmin(context.Background(), "An application needs approval", "Alice applied."))
	}
	emails := sent()
	require.Len(t, emails, 3)
	require.Equal(t, "/emails", emails[0].path)
	require.Equal(t, "/emails", emails[1].path)
	require.Equal(t, "/api/emails", emails[2].path)
	require.Equal(t, "Bearer re_test_key", emails[0].auth)
	require.Equal(t, map[string]any{
		"from": "Open Pool <pool@open-pool.com>", "to": []any{"ops@open-pool.com"},
		"subject": "An application needs approval", "text": "Alice applied.",
	}, emails[0].body)
}

func TestMailerSendsTheSignInCode(t *testing.T) {
	srv, sent := fakeResend(t)
	mailer, err := NewResend(srv.URL, "re_test_key", "pool@open-pool.com", "ops@open-pool.com")
	require.NoError(t, err)
	require.NoError(t, mailer.SendSignInCode(context.Background(), "ops@open-pool.com", "482913"))

	email := sent()[0]
	require.Equal(t, []any{"ops@open-pool.com"}, email.body["to"])
	require.Equal(t, "Your Open Pool sign-in code", email.body["subject"])
	require.Contains(t, email.body["text"], "482913")
}

func TestMailerReportsAFailedSend(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = io.WriteString(w, `{"name":"validation_error","message":"invalid from address"}`)
	}))
	defer srv.Close()
	mailer, err := NewResend(srv.URL, "re_test_key", "pool@open-pool.com", "ops@open-pool.com")
	require.NoError(t, err)
	require.Error(t, mailer.NotifyAdmin(context.Background(), "s", "b"))
}

func TestNewResendNeedsItsSettings(t *testing.T) {
	_, err := NewResend("", "", "pool@open-pool.com", "ops@open-pool.com")
	require.Error(t, err)
	_, err = NewResend("", "re_key", "", "ops@open-pool.com")
	require.Error(t, err)
	_, err = NewResend("not a url", "re_key", "pool@open-pool.com", "ops@open-pool.com")
	require.Error(t, err)
	// No base URL means Resend's own.
	_, err = NewResend("", "re_key", "pool@open-pool.com", "ops@open-pool.com")
	require.NoError(t, err)
}

func TestLogStandsInWhenEmailIsNotConfigured(t *testing.T) {
	require.NoError(t, Log{}.NotifyAdmin(context.Background(), "subject", "body"))
	require.NoError(t, Log{}.SendSignInCode(context.Background(), "ops@open-pool.com", "123456"))
}
