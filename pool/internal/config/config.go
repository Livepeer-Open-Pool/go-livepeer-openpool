// Package config reads the pool service's deployment settings from the
// environment. Pool policy (commission, windows, tests) is not here: it
// lives in the database and is edited on the admin site.
package config

import (
	"fmt"
	"os"
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

	if len(problems) > 0 {
		return nil, fmt.Errorf("configuration:\n  %s", strings.Join(problems, "\n  "))
	}
	return cfg, nil
}

// MailConfigured says whether email can be sent.
func (c *Config) MailConfigured() bool {
	return c.ResendAPIKey != "" && c.MailFrom != ""
}
