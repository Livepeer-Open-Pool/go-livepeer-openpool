package core

import (
	"context"
	"strconv"
)

// Settings are the pool's policy, edited on the admin site.
type Settings struct {
	// CommissionBps is the orchestrator's share of the pot, in hundredths of a percent.
	CommissionBps int `json:"commission_bps"`
	// WindowRounds is the length of a payout window.
	WindowRounds int `json:"window_rounds"`
	// PayoutApproval is "required" or "automatic".
	PayoutApproval string `json:"payout_approval"`
	// TestIntervalSeconds is how often a runner is tested.
	TestIntervalSeconds int `json:"test_interval_seconds"`
	// TestFailuresToSuspend is how many failures in a row suspend a runner.
	TestFailuresToSuspend int `json:"test_failures_to_suspend"`
	// TestPassesToReinstate is how many passes in a row bring a suspended runner back.
	TestPassesToReinstate int `json:"test_passes_to_reinstate"`
	// VersionDeadlineHours is how long runners on an app's previous version
	// have before they are suspended.
	VersionDeadlineHours int `json:"version_deadline_hours"`
}

// Payout approval modes.
const (
	PayoutApprovalRequired  = "required"
	PayoutApprovalAutomatic = "automatic"
)

// DefaultSettings is the policy of a new pool.
func DefaultSettings() Settings {
	return Settings{
		CommissionBps:         1500,
		WindowRounds:          7,
		PayoutApproval:        PayoutApprovalRequired,
		TestIntervalSeconds:   600,
		TestFailuresToSuspend: 3,
		TestPassesToReinstate: 3,
		VersionDeadlineHours:  24,
	}
}

func (st Settings) validate() error {
	switch {
	case st.CommissionBps < 0 || st.CommissionBps > 10000:
		return invalidf("commission must be between 0%% and 100%%")
	case st.WindowRounds < 1 || st.WindowRounds > 365:
		return invalidf("window must be between 1 and 365 rounds")
	case st.PayoutApproval != PayoutApprovalRequired && st.PayoutApproval != PayoutApprovalAutomatic:
		return invalidf("payout approval must be %q or %q", PayoutApprovalRequired, PayoutApprovalAutomatic)
	case st.TestIntervalSeconds < 30:
		return invalidf("test interval must be at least 30 seconds")
	case st.TestFailuresToSuspend < 1 || st.TestPassesToReinstate < 1:
		return invalidf("test thresholds must be at least 1")
	case st.VersionDeadlineHours < 0:
		return invalidf("version deadline must not be negative")
	}
	return nil
}

// Settings returns the pool's policy: the defaults, overridden by what was saved.
func (s *Service) Settings(ctx context.Context) (Settings, error) {
	st := DefaultSettings()
	rows, err := s.db.QueryContext(ctx, `SELECT key, value FROM settings`)
	if err != nil {
		return st, err
	}
	defer rows.Close()
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return st, err
		}
		n, _ := strconv.Atoi(value)
		switch key {
		case "commission_bps":
			st.CommissionBps = n
		case "window_rounds":
			st.WindowRounds = n
		case "payout_approval":
			st.PayoutApproval = value
		case "test_interval_seconds":
			st.TestIntervalSeconds = n
		case "test_failures_to_suspend":
			st.TestFailuresToSuspend = n
		case "test_passes_to_reinstate":
			st.TestPassesToReinstate = n
		case "version_deadline_hours":
			st.VersionDeadlineHours = n
		}
	}
	return st, rows.Err()
}

// SaveSettings replaces the pool's policy.
func (s *Service) SaveSettings(ctx context.Context, actor string, st Settings) error {
	if err := st.validate(); err != nil {
		return err
	}
	values := map[string]string{
		"commission_bps":           strconv.Itoa(st.CommissionBps),
		"window_rounds":            strconv.Itoa(st.WindowRounds),
		"payout_approval":          st.PayoutApproval,
		"test_interval_seconds":    strconv.Itoa(st.TestIntervalSeconds),
		"test_failures_to_suspend": strconv.Itoa(st.TestFailuresToSuspend),
		"test_passes_to_reinstate": strconv.Itoa(st.TestPassesToReinstate),
		"version_deadline_hours":   strconv.Itoa(st.VersionDeadlineHours),
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for key, value := range values {
		if _, err := tx.ExecContext(ctx, `INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value`, key, value); err != nil {
			return err
		}
	}
	if err := s.audit(ctx, tx, actor, "settings.save", "", mustJSON(st)); err != nil {
		return err
	}
	return tx.Commit()
}
