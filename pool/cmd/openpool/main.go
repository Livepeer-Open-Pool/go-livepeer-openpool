// openpool is the Open Pool service for one region: it sits beside one
// orchestrator node and decides who may join, what each machine runs,
// whether it works, what it earned, and pays it out. See pool/DESIGN.md.
package main

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Livepeer-Open-Pool/pool/internal/api"
	"github.com/Livepeer-Open-Pool/pool/internal/auth"
	"github.com/Livepeer-Open-Pool/pool/internal/compose"
	"github.com/Livepeer-Open-Pool/pool/internal/config"
	"github.com/Livepeer-Open-Pool/pool/internal/core"
	"github.com/Livepeer-Open-Pool/pool/internal/ledger"
	"github.com/Livepeer-Open-Pool/pool/internal/mail"
	"github.com/Livepeer-Open-Pool/pool/internal/orchestrator"
	"github.com/Livepeer-Open-Pool/pool/internal/payouts"
	"github.com/Livepeer-Open-Pool/pool/internal/store"
)

// pricePushInterval is how often the catalog's prices are sent to the
// orchestrator regardless of changes. An orchestrator that restarted has
// lost its price table and gets it back within this long.
const pricePushInterval = time.Minute

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(log); err != nil {
		log.Error("openpool stopped", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.FromEnv()
	if err != nil {
		return err
	}
	db, err := store.Open(cfg.DBPath)
	if err != nil {
		return err
	}
	defer db.Close()

	orch, err := orchestrator.New(cfg.OrchestratorCLIURL)
	if err != nil {
		return err
	}

	// One value sends both kinds of email: notices and the sign-in code.
	type mailer interface {
		core.Notifier
		auth.CodeSender
	}
	var sender mailer = mail.Log{Logger: log}
	if cfg.MailConfigured() {
		sender, err = mail.NewResend(cfg.ResendBaseURL, cfg.ResendAPIKey, cfg.MailFrom, cfg.AdminEmail)
		if err != nil {
			return err
		}
	} else {
		log.Warn("email is not configured: admin notices and sign-in codes go to this log. Set OPENPOOL_RESEND_API_KEY and OPENPOOL_MAIL_FROM")
	}

	tokens, err := auth.NewTokens(cfg.SessionSecret, cfg.SessionTTL, nil)
	if err != nil {
		return err
	}
	var agent *api.AgentOptions
	composeKey := ""
	if cfg.AgentConfigured() {
		key, err := compose.LoadOrCreateKey(cfg.ComposeKeyFile)
		if err != nil {
			return err
		}
		agent = &api.AgentOptions{Key: key, Image: cfg.AgentImage, TunnelImage: cfg.TunnelImage, OrchestratorURL: cfg.OrchestratorURL}
		composeKey = compose.EncodePublicKey(key.Public().(ed25519.PublicKey))
		log.Info("member agent ready", "agent_image", cfg.AgentImage, "tunnel_image", cfg.TunnelImage, "compose_key", composeKey)
	} else {
		log.Warn("no tunnel image configured: members start their runners by hand, and the agent is not offered. Set OPENPOOL_TUNNEL_IMAGE")
	}
	service := core.New(db, orch, sender, core.Config{
		RegionID:        cfg.RegionID,
		RegionName:      cfg.RegionName,
		PublicURL:       cfg.PublicURL,
		OrchestratorURL: cfg.OrchestratorURL,
		AdminURL:        cfg.AdminURL,
		AgentImage:      cfg.AgentImage,
		ComposeKey:      composeKey,
		RecoverSigner:   auth.RecoverSigner,
	}, log)
	books := ledger.New(db, nil, log)
	var wallet *payouts.Wallet
	if cfg.PayoutsConfigured() {
		wallet, err = payouts.OpenWallet(context.Background(), cfg.EthRPCURL, cfg.PayoutKeystore, cfg.PayoutPassword, int64(cfg.ChainID))
		if err != nil {
			return err
		}
		log.Info("payout wallet ready", "address", wallet.Address().Hex(), "chain_id", cfg.ChainID)
	} else {
		log.Warn("no payout wallet configured: batches are built and can be approved, and nothing is paid. Set OPENPOOL_ETH_RPC_URL and OPENPOOL_PAYOUT_KEYSTORE")
	}
	payer := payouts.New(db, books, service.Settings, orch, wallet, sender, payouts.Config{
		RegionName: cfg.RegionName, AdminURL: cfg.AdminURL, SettleDelay: cfg.SettleDelay,
	}, log)

	prober, err := orchestrator.NewProber(cfg.OrchestratorURL, cfg.PoolSecret, cfg.OrchestratorInsecureTLS)
	if err != nil {
		return err
	}
	service.SetProber(prober)
	server := api.New(api.Options{
		Core:           service,
		Ledger:         books,
		Payouts:        payer,
		Tokens:         tokens,
		Wallet:         auth.NewWallet(db, tokens, cfg.SignInDomains, cfg.ChainID, nil),
		Admin:          auth.NewAdmin(db, tokens, sender, cfg.AdminEmail, nil),
		PoolSecret:     cfg.PoolSecret,
		AllowedOrigins: cfg.AllowedOrigins,
		SignIn:         api.SignInInfo{Domains: cfg.SignInDomains, ChainID: cfg.ChainID},
		Agent:          agent,
		Logger:         log,
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		ticker := time.NewTicker(pricePushInterval)
		defer ticker.Stop()
		for {
			if err := service.PushPrices(ctx); err != nil {
				log.Warn("prices not pushed to the orchestrator", "err", err)
			}
			select {
			case <-ticker.C:
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		ticker := time.NewTicker(cfg.ReconcileInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := service.Reconcile(ctx); err != nil {
					log.Warn("runners not reconciled", "err", err)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	kafka := ledger.KafkaConfig{Brokers: cfg.KafkaBrokers, Topic: cfg.KafkaTopic, Group: cfg.KafkaGroup, User: cfg.KafkaUser, Password: cfg.KafkaPassword}
	if kafka.Configured() {
		go func() {
			if err := books.Consume(ctx, kafka); err != nil {
				log.Error("orchestrator events are no longer read", "err", err)
			}
		}()
	} else {
		log.Warn("no Kafka topic configured: earnings are recorded only from events uploaded to /v1/orchestrator/events. Set OPENPOOL_KAFKA_BROKERS and OPENPOOL_KAFKA_TOPIC")
	}
	go func() {
		ticker := time.NewTicker(cfg.PayoutInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := payer.Tick(ctx); err != nil {
					log.Warn("payouts not moved forward", "err", err)
				}
				if err := books.Prune(ctx); err != nil {
					log.Warn("event ids not pruned", "err", err)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	httpServer := &http.Server{Addr: cfg.Listen, Handler: server.Handler(), ReadHeaderTimeout: 10 * time.Second}
	failed := make(chan error, 1)
	go func() { failed <- httpServer.ListenAndServe() }()
	log.Info("openpool listening", "addr", cfg.Listen, "region", cfg.RegionID, "orchestrator_cli", cfg.OrchestratorCLIURL)

	select {
	case err := <-failed:
		return fmt.Errorf("listen on %s: %w", cfg.Listen, err)
	case <-ctx.Done():
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdown); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
