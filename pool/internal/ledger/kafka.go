package ledger

import (
	"context"
	"crypto/tls"
	"errors"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

// KafkaConfig says where the orchestrator's events are read from. Each
// region has its own topic: the events of two nodes must not be mixed.
type KafkaConfig struct {
	Brokers []string
	Topic   string
	// Group is the consumer group, which is how the service remembers where
	// it stopped reading.
	Group string
	// User and Password, when set, sign in with SASL over TLS, as go-livepeer's producer does.
	User     string
	Password string
}

// Configured says whether there is a topic to read.
func (c KafkaConfig) Configured() bool { return len(c.Brokers) > 0 && c.Topic != "" }

// Consume reads the topic into the ledger until ctx ends. An event is
// committed only once it is applied, so a crash replays it, and replaying is
// harmless. An event that can never be applied is logged and passed over.
func (l *Ledger) Consume(ctx context.Context, cfg KafkaConfig) error {
	dialer := &kafka.Dialer{Timeout: 30 * time.Second, DualStack: true}
	if cfg.User != "" && cfg.Password != "" {
		dialer.SASLMechanism = &plain.Mechanism{Username: cfg.User, Password: cfg.Password}
		dialer.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: cfg.Brokers, Topic: cfg.Topic, GroupID: cfg.Group, Dialer: dialer,
		MinBytes: 1, MaxBytes: 10 << 20, MaxWait: time.Second,
	})
	defer reader.Close()
	l.log.Info("reading orchestrator events", "topic", cfg.Topic, "group", cfg.Group)

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			l.log.Warn("reading orchestrator events", "err", err)
			select {
			case <-time.After(5 * time.Second):
				continue
			case <-ctx.Done():
				return nil
			}
		}
		for {
			err := l.Apply(ctx, msg.Value)
			if err == nil {
				break
			}
			if errors.Is(err, ErrMalformed) {
				l.log.Error("orchestrator event passed over", "err", err, "event", string(msg.Value))
				break
			}
			// Our own fault, for instance the database being busy: try again
			// rather than lose the event.
			l.log.Warn("orchestrator event not applied yet", "err", err)
			select {
			case <-time.After(2 * time.Second):
			case <-ctx.Done():
				return nil
			}
		}
		if err := reader.CommitMessages(ctx, msg); err != nil && ctx.Err() == nil {
			l.log.Warn("committing orchestrator event offset", "err", err)
		}
	}
}
