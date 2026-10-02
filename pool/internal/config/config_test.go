package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func setRequired(t *testing.T) {
	t.Helper()
	for name, value := range map[string]string{
		"OPENPOOL_REGION_ID":        "us-east",
		"OPENPOOL_PUBLIC_URL":       "https://us-east.api.open-pool.com",
		"OPENPOOL_ORCHESTRATOR_URL": "https://orch.us-east.open-pool.com:8935",
		"OPENPOOL_POOL_SECRET":      "pool-secret",
		"OPENPOOL_SESSION_SECRET":   "a-session-signing-secret-of-enough-length",
		"OPENPOOL_ADMIN_EMAIL":      "ops@open-pool.com",
	} {
		t.Setenv(name, value)
	}
}

func TestFromEnvDefaults(t *testing.T) {
	setRequired(t)
	cfg, err := FromEnv()
	require.NoError(t, err)
	require.Equal(t, "us-east", cfg.RegionID)
	require.Equal(t, "us-east", cfg.RegionName, "the id stands in for a missing name")
	require.Equal(t, "http://127.0.0.1:7935", cfg.OrchestratorCLIURL)
	require.Equal(t, 42161, cfg.ChainID, "Arbitrum One")
	require.Equal(t, 12*time.Hour, cfg.SessionTTL)
	require.Equal(t, []string{"app.open-pool.com"}, cfg.SignInDomains)
	require.Equal(t, []string{"https://open-pool.com", "https://app.open-pool.com", "https://admin.open-pool.com"}, cfg.AllowedOrigins)
	require.False(t, cfg.MailConfigured())
}

func TestFromEnvReportsEveryProblemAtOnce(t *testing.T) {
	for _, name := range []string{"OPENPOOL_REGION_ID", "OPENPOOL_PUBLIC_URL", "OPENPOOL_ORCHESTRATOR_URL", "OPENPOOL_POOL_SECRET", "OPENPOOL_ADMIN_EMAIL"} {
		t.Setenv(name, "")
	}
	t.Setenv("OPENPOOL_SESSION_SECRET", "short")
	t.Setenv("OPENPOOL_CHAIN_ID", "arbitrum")
	t.Setenv("OPENPOOL_SESSION_TTL", "forever")
	_, err := FromEnv()
	require.Error(t, err)
	for _, want := range []string{
		"OPENPOOL_REGION_ID is required", "OPENPOOL_PUBLIC_URL is required", "OPENPOOL_ORCHESTRATOR_URL is required",
		"OPENPOOL_POOL_SECRET is required", "OPENPOOL_ADMIN_EMAIL is required",
		"OPENPOOL_SESSION_SECRET must be at least 32 characters", "OPENPOOL_CHAIN_ID must be a positive number",
		"OPENPOOL_SESSION_TTL must be a duration",
	} {
		require.ErrorContains(t, err, want)
	}
}

func TestSecretsCanComeFromFiles(t *testing.T) {
	setRequired(t)
	path := filepath.Join(t.TempDir(), "pool-secret")
	require.NoError(t, os.WriteFile(path, []byte("from-a-file\n"), 0o600))
	t.Setenv("OPENPOOL_POOL_SECRET", "")
	t.Setenv("OPENPOOL_POOL_SECRET_FILE", path)
	t.Setenv("OPENPOOL_RESEND_API_KEY", "re_123")
	t.Setenv("OPENPOOL_MAIL_FROM", "Open Pool <pool@open-pool.com>")
	t.Setenv("OPENPOOL_SIGN_IN_DOMAINS", "app.open-pool.com, localhost:5173")

	cfg, err := FromEnv()
	require.NoError(t, err)
	require.Equal(t, "from-a-file", cfg.PoolSecret)
	require.True(t, cfg.MailConfigured())
	require.Equal(t, []string{"app.open-pool.com", "localhost:5173"}, cfg.SignInDomains)

	t.Setenv("OPENPOOL_POOL_SECRET_FILE", filepath.Join(t.TempDir(), "missing"))
	_, err = FromEnv()
	require.ErrorContains(t, err, "OPENPOOL_POOL_SECRET_FILE")
}

func TestPayoutAndKafkaSettings(t *testing.T) {
	setRequired(t)
	cfg, err := FromEnv()
	require.NoError(t, err)
	require.False(t, cfg.PayoutsConfigured())
	require.Empty(t, cfg.KafkaBrokers)
	require.Equal(t, "openpool-us-east", cfg.KafkaGroup)
	require.Equal(t, 10*time.Minute, cfg.SettleDelay)
	require.Equal(t, 30*time.Second, cfg.PayoutInterval)
	require.Equal(t, 15*time.Second, cfg.ReconcileInterval)

	// A node without a keystore, or the reverse, is a mistake worth stopping for.
	t.Setenv("OPENPOOL_ETH_RPC_URL", "https://arb1.arbitrum.io/rpc")
	_, err = FromEnv()
	require.ErrorContains(t, err, "OPENPOOL_ETH_RPC_URL and OPENPOOL_PAYOUT_KEYSTORE go together")

	t.Setenv("OPENPOOL_PAYOUT_KEYSTORE", "/etc/openpool/payout.json")
	t.Setenv("OPENPOOL_PAYOUT_PASSWORD", "secret")
	t.Setenv("OPENPOOL_KAFKA_BROKERS", "kafka-1:9092, kafka-2:9092")
	t.Setenv("OPENPOOL_KAFKA_TOPIC", "orchestrator-events-us-east")
	cfg, err = FromEnv()
	require.NoError(t, err)
	require.True(t, cfg.PayoutsConfigured())
	require.Equal(t, []string{"kafka-1:9092", "kafka-2:9092"}, cfg.KafkaBrokers)
	require.Equal(t, "secret", cfg.PayoutPassword)
}
