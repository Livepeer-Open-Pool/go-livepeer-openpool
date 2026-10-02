// Package config reads the pool service's deployment settings from the
// environment. Pool policy (commission, windows, tests) is not here: it
// lives in the database and is edited on the admin site.
package config

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Config is where and as what this pool service runs.
type Config struct {
	RegionID   string
	RegionName string
	Listen     string
	// PublicURL is this service's address as the sites and agents reach it.
	PublicURL string
	DBPath    string

	// OrchestratorURL is the node's public service address; OrchestratorCLIURL
	// its CLI port; PoolSecret the node's -liveRunnerPoolSecret.
	OrchestratorURL    string
	OrchestratorCLIURL string
	PoolSecret         string
	// OrchestratorInsecureTLS skips certificate verification when testing
	// runners through the orchestrator, for one that serves its default
	// self-signed certificate.
	OrchestratorInsecureTLS bool

	// SessionSecret signs session tokens and must be the same in every region.
	SessionSecret string
	SessionTTL    time.Duration
	// SignInDomains are the domains a wallet sign-in message may be for.
	SignInDomains []string
	ChainID       int

	AdminEmail    string
	AdminURL      string
	ResendBaseURL string
	ResendAPIKey  string
	MailFrom      string

	// AllowedOrigins are the sites whose pages may call this API.
	AllowedOrigins []string

	// ReconcileInterval is how often the service looks over the registered
	// runners: which are due a test, which to drain, which run an old version.
	ReconcileInterval time.Duration

	// Kafka is where the orchestrator's events are read from.
	KafkaBrokers  []string
	KafkaTopic    string
	KafkaGroup    string
	KafkaUser     string
	KafkaPassword string

	// The member agent. ComposeKeyFile holds the key that signs the compose
	// files agents apply, and is created when missing. AgentImage is the
	// agent's image and TunnelImage the image holding livepeer_runner_tunnel,
	// pinned by digest. Without a TunnelImage machines are not run by the agent.
	ComposeKeyFile string
	AgentImage     string
	TunnelImage    string

	// The payout wallet: an Ethereum node, a keystore file and its password.
	EthRPCURL      string
	PayoutKeystore string
	PayoutPassword string
	// PayoutInterval is how often windows are checked and payments moved forward.
	PayoutInterval time.Duration
	// SettleDelay is how long after a window's last round the window is closed.
	SettleDelay time.Duration
}

// value returns the environment variable name, or the content of the file
// named by name_FILE, so secrets can come from mounted files.
func value(name string) (string, error) {
	if path := os.Getenv(name + "_FILE"); path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("%s_FILE: %w", name, err)
		}
		return strings.TrimSpace(string(data)), nil
	}
	return strings.TrimSpace(os.Getenv(name)), nil
}

func list(s string) []string {
	out := []string{}
	for _, item := range strings.Split(s, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}

// FromEnv reads the configuration from OPENPOOL_* environment variables and
// reports everything that is missing or malformed at once.
func FromEnv() (*Config, error) {
	var problems []string
	get := func(name, fallback string, required bool) string {
		v, err := value("OPENPOOL_" + name)
		if err != nil {
			problems = append(problems, err.Error())
			return ""
		}
		if v == "" {
			v = fallback
		}
		if v == "" && required {
			problems = append(problems, "OPENPOOL_"+name+" is required")
		}
		return v
	}

	cfg := &Config{
		RegionID:           get("REGION_ID", "", true),
		RegionName:         get("REGION_NAME", "", false),
		Listen:             get("LISTEN", "127.0.0.1:8090", false),
		PublicURL:          get("PUBLIC_URL", "", true),
		DBPath:             get("DB_PATH", "openpool.db", false),
		OrchestratorURL:    get("ORCHESTRATOR_URL", "", true),
		OrchestratorCLIURL: get("ORCHESTRATOR_CLI_URL", "http://127.0.0.1:7935", false),
		PoolSecret:         get("POOL_SECRET", "", true),
		SessionSecret:      get("SESSION_SECRET", "", true),
		SignInDomains:      list(get("SIGN_IN_DOMAINS", "app.open-pool.com", false)),
		AdminEmail:         get("ADMIN_EMAIL", "", true),
		AdminURL:           get("ADMIN_URL", "https://admin.open-pool.com", false),
		ResendBaseURL:      get("RESEND_BASE_URL", "", false),
		ResendAPIKey:       get("RESEND_API_KEY", "", false),
		MailFrom:           get("MAIL_FROM", "", false),
		AllowedOrigins:     list(get("ALLOWED_ORIGINS", "https://open-pool.com,https://app.open-pool.com,https://admin.open-pool.com", false)),
	}
	cfg.OrchestratorInsecureTLS = get("ORCHESTRATOR_INSECURE_TLS", "false", false) == "true"
	cfg.KafkaBrokers = list(get("KAFKA_BROKERS", "", false))
	cfg.KafkaTopic = get("KAFKA_TOPIC", "", false)
	cfg.KafkaGroup = get("KAFKA_GROUP", "openpool-"+cfg.RegionID, false)
	cfg.KafkaUser = get("KAFKA_USER", "", false)
	cfg.KafkaPassword = get("KAFKA_PASSWORD", "", false)
	cfg.ComposeKeyFile = get("COMPOSE_KEY_FILE", "openpool-compose.key", false)
	cfg.AgentImage = get("AGENT_IMAGE", "tztcloud/openpool-agent:latest", false)
	cfg.TunnelImage = get("TUNNEL_IMAGE", "", false)
	if cfg.TunnelImage != "" && !pinnedImage.MatchString(cfg.TunnelImage) {
		problems = append(problems, "OPENPOOL_TUNNEL_IMAGE must be pinned by digest, as name@sha256:<64 hex digits>")
	}
	cfg.EthRPCURL = get("ETH_RPC_URL", "", false)
	cfg.PayoutKeystore = get("PAYOUT_KEYSTORE", "", false)
	cfg.PayoutPassword = get("PAYOUT_PASSWORD", "", false)
	if (cfg.EthRPCURL == "") != (cfg.PayoutKeystore == "") {
		problems = append(problems, "OPENPOOL_ETH_RPC_URL and OPENPOOL_PAYOUT_KEYSTORE go together: set both to pay out, or neither")
	}
	if cfg.RegionID != "" && !regionID.MatchString(cfg.RegionID) {
		problems = append(problems, "OPENPOOL_REGION_ID must be lowercase letters, digits and dashes, such as us-east")
	}
	if cfg.RegionName == "" {
		cfg.RegionName = cfg.RegionID
	}
	if len(cfg.SessionSecret) > 0 && len(cfg.SessionSecret) < 32 {
		problems = append(problems, "OPENPOOL_SESSION_SECRET must be at least 32 characters")
	}

	chainID, err := strconv.Atoi(get("CHAIN_ID", "42161", false))
	if err != nil || chainID <= 0 {
		problems = append(problems, "OPENPOOL_CHAIN_ID must be a positive number")
	}
	cfg.ChainID = chainID
	ttl, err := time.ParseDuration(get("SESSION_TTL", "12h", false))
	if err != nil || ttl <= 0 {
		problems = append(problems, "OPENPOOL_SESSION_TTL must be a duration such as 12h")
	}
	cfg.SessionTTL = ttl
	interval, err := time.ParseDuration(get("RECONCILE_INTERVAL", "15s", false))
	if err != nil || interval <= 0 {
		problems = append(problems, "OPENPOOL_RECONCILE_INTERVAL must be a duration such as 15s")
	}
	cfg.ReconcileInterval = interval
	if cfg.PayoutInterval, err = time.ParseDuration(get("PAYOUT_INTERVAL", "30s", false)); err != nil || cfg.PayoutInterval <= 0 {
		problems = append(problems, "OPENPOOL_PAYOUT_INTERVAL must be a duration such as 30s")
	}
	if cfg.SettleDelay, err = time.ParseDuration(get("SETTLE_DELAY", "10m", false)); err != nil || cfg.SettleDelay <= 0 {
		problems = append(problems, "OPENPOOL_SETTLE_DELAY must be a duration such as 10m")
	}

	if len(problems) > 0 {
		return nil, fmt.Errorf("configuration:\n  %s", strings.Join(problems, "\n  "))
	}
	return cfg, nil
}

var regionID = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,31}$`)

var pinnedImage = regexp.MustCompile(`^[a-z0-9][a-z0-9._:/-]*@sha256:[0-9a-f]{64}$`)

// AgentConfigured says whether machines are run by the member agent.
func (c *Config) AgentConfigured() bool { return c.TunnelImage != "" }

// PayoutsConfigured says whether the service has a wallet to pay from.
func (c *Config) PayoutsConfigured() bool {
	return c.EthRPCURL != "" && c.PayoutKeystore != ""
}

// MailConfigured says whether email can be sent.
func (c *Config) MailConfigured() bool {
	return c.ResendAPIKey != "" && c.MailFrom != ""
}
