// livepeer_runner_tunnel exposes a plain HTTP app as a Live Runner without a
// public address: it registers with an orchestrator, opens a tunnel to it,
// and serves the requests the orchestrator sends through the tunnel by
// proxying them to the app. See doc/live-runner.md, "Tunneled runners".
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/livepeer/go-livepeer/ai/runner/tunnelclient"
)

func main() {
	var (
		orchestrator = flag.String("orchestrator", "https://127.0.0.1:8935", "Orchestrator service address")
		orchSecret   = flag.String("orchSecret", "", "Registration secret, or the path of a file holding it")
		target       = flag.String("target", "http://127.0.0.1:8000", "The local app to serve")
		health       = flag.String("health", "/health", "Root-relative health path on the target; empty reports ready without checking")
		app          = flag.String("app", "", "App name to register, for example blueclaw/chat/qwen3-27b")
		mode         = flag.String("mode", "single-shot", "single-shot or persistent")
		capacity     = flag.Int("capacity", 1, "Concurrent sessions")
		price        = flag.String("price", "", "Price the orchestrator advertises (required on-chain)")
		currency     = flag.String("currency", "usd", "Price currency")
		unit         = flag.String("unit", "hour", "Price unit: hour, 720p, or fixed")
		metadata     = flag.String("metadata", "", "Discovery metadata, or @path to read it from a file")
		label        = flag.String("label", "", "Optional label")
		version      = flag.String("version", "", "Version string to advertise")
		insecure     = flag.Bool("insecure", true, "Skip TLS verification toward the orchestrator (self-signed certificates)")
		verbose      = flag.Bool("v", false, "Debug logging")
	)
	flag.Parse()

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	secret := *orchSecret
	if data, err := os.ReadFile(secret); err == nil {
		secret = strings.TrimSpace(string(data))
	}
	meta := *metadata
	if strings.HasPrefix(meta, "@") {
		data, err := os.ReadFile(meta[1:])
		if err != nil {
			fmt.Fprintln(os.Stderr, "metadata:", err)
			os.Exit(2)
		}
		meta = strings.TrimSpace(string(data))
	}

	client, err := tunnelclient.New(tunnelclient.Config{
		Orchestrator: *orchestrator,
		OrchSecret:   secret,
		Target:       *target,
		Health:       *health,
		App:          *app,
		Mode:         *mode,
		Label:        *label,
		Version:      *version,
		Metadata:     meta,
		Capacity:     *capacity,
		Price:        *price,
		Currency:     *currency,
		Unit:         *unit,
		InsecureTLS:  *insecure,
		Logger:       logger,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := client.Run(ctx); err != nil {
		logger.Error("runner tunnel stopped", "err", err)
		os.Exit(1)
	}
}
