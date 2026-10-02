// Package mail sends the operator's email: the sign-in code, and notices
// that something is waiting for approval. Members are never emailed.
package mail

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"strings"

	"github.com/resend/resend-go/v2"
)

// Mailer sends email to the admin address through Resend.
type Mailer struct {
	client *resend.Client
	from   string
	admin  string
}

// NewResend returns a mailer for a Resend-compatible API at baseURL.
func NewResend(baseURL, apiKey, from, adminEmail string) (*Mailer, error) {
	if apiKey == "" || from == "" || adminEmail == "" {
		return nil, fmt.Errorf("email needs an API key, a from address and the admin email")
	}
	client := resend.NewClient(apiKey)
	if baseURL != "" {
		// The SDK resolves request paths against the base URL, which
		// therefore has to end with a slash.
		u, err := url.Parse(strings.TrimRight(baseURL, "/") + "/")
		if err != nil || u.Host == "" {
			return nil, fmt.Errorf("email base URL %q is not a URL", baseURL)
		}
		client.BaseURL = u
	}
	return &Mailer{client: client, from: from, admin: adminEmail}, nil
}

func (m *Mailer) send(ctx context.Context, to, subject, body string) error {
	_, err := m.client.Emails.SendWithContext(ctx, &resend.SendEmailRequest{
		From:    m.from,
		To:      []string{to},
		Subject: subject,
		Text:    body,
	})
	return err
}

// NotifyAdmin emails the operator.
func (m *Mailer) NotifyAdmin(ctx context.Context, subject, body string) error {
	return m.send(ctx, m.admin, subject, body)
}

// SendSignInCode emails a sign-in code.
func (m *Mailer) SendSignInCode(ctx context.Context, email, code string) error {
	return m.send(ctx, email, "Your Open Pool sign-in code",
		fmt.Sprintf("Your Open Pool admin sign-in code is %s\n\nIt works once and expires in ten minutes. If you did not ask for it, ignore this email.", code))
}

// Log stands in for email when none is configured: it writes what would
// have been sent to the log, the sign-in code included. That is for local
// development only; a deployment configures Resend.
type Log struct {
	Logger *slog.Logger
}

func (l Log) logger() *slog.Logger {
	if l.Logger != nil {
		return l.Logger
	}
	return slog.Default()
}

// NotifyAdmin logs the notice.
func (l Log) NotifyAdmin(ctx context.Context, subject, body string) error {
	l.logger().Warn("email is not configured; admin notice not sent", "subject", subject, "body", body)
	return nil
}

// SendSignInCode logs the code.
func (l Log) SendSignInCode(ctx context.Context, email, code string) error {
	l.logger().Warn("email is not configured; sign-in code written to the log instead", "email", email, "code", code)
	return nil
}
