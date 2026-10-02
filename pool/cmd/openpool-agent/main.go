// openpool-agent runs on a pool member's GPU machine and keeps it running
// what the pool assigned it. See pool/DESIGN.md, "The member agent".
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Livepeer-Open-Pool/pool/internal/agent"
	"github.com/Livepeer-Open-Pool/pool/internal/compose"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(log); err != nil {
		log.Error("openpool-agent stopped", "err", err)
		os.Exit(1)
	}
}

// secret returns the environment variable name, or the content of the file
// named by name_FILE.
func secret(name string) (string, error) {
	if path := os.Getenv(name + "_FILE"); path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("%s_FILE: %w", name, err)
		}
		return strings.TrimSpace(string(data)), nil
	}
	return strings.TrimSpace(os.Getenv(name)), nil
}

func run(log *slog.Logger) error {
	pool := flag.String("pool", os.Getenv("OPENPOOL_POOL"), "The pool region's API address, as shown with the install command")
	key := flag.String("key", os.Getenv("OPENPOOL_KEY"), "The pool's compose signing key, as shown with the install command")
	interval := flag.Duration("interval", 30*time.Second, "How often to check with the pool")
	version := flag.Bool("version", false, "Print the version and exit")
	flag.Parse()
	if *version {
		fmt.Println(agent.Version)
		return nil
	}

	credential, err := secret(compose.CredentialVariable)
	if err != nil {
		return err
	}
	var problems []string
	if *pool == "" {
		problems = append(problems, "--pool is required")
	}
	if credential == "" {
		problems = append(problems, compose.CredentialVariable+" is required: the machine's credential, from its page on the member site")
	}
	publicKey, err := compose.ParsePublicKey(*key)
	if err != nil {
		problems = append(problems, "--key: "+err.Error())
	}
	if *interval < time.Second {
		problems = append(problems, "--interval must be at least 1s")
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	a := &agent.Agent{
		PoolURL:    *pool,
		Credential: credential,
		Key:        publicKey,
		Docker:     agent.CLI{Project: agent.ProjectFor(*pool)},
		ListGPUs:   agent.NvidiaGPUs,
		Log:        log,
	}
	log.Info("openpool-agent started", "version", agent.Version, "pool", *pool)
	return a.Run(ctx, *interval)
}
