package pkg

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
)

type NatsConfig struct {
	Servers           []string       `toml:"servers"`
	Stream            string         `toml:"stream"`
	Durable           string         `toml:"durable"`
	Secure            bool           `toml:"secure"`
	InactiveThreshold time.Duration  `toml:"inactive_threshold"`
	DeliveryPolicy    DeliveryPolicy `toml:"delivery_policy"`
	DeliveryStartSeq  uint64         `toml:"delivery_start_seq"`
	DeliveryStartTime *time.Time     `toml:"delivery_start_time"`
	Nkey              string         `toml:"nkey"`
	NkeyPrivate       string         `toml:"nkey_private"`
	User              string         `toml:"user"`
	Password          string         `toml:"password"`
	Token             string         `toml:"token"`
}

func (c *NatsConfig) Validate() error {
	if c.Stream == "" {
		return errors.New("stream is required")
	}
	return nil
}

// LoadEnvVars attempts to read a set of configuration values
// from environment variables, if they are set
func (c *NatsConfig) LoadEnvVars() {
	if v := os.Getenv("NATS_NKEY"); v != "" {
		c.Nkey = v
	}
	if v := os.Getenv("NATS_NKEY_PRIVATE"); v != "" {
		c.NkeyPrivate = v
	}
	if v := os.Getenv("NATS_USER"); v != "" {
		c.User = v
	}
	if v := os.Getenv("NATS_PASSWORD"); v != "" {
		c.Password = v
	}
	if v := os.Getenv("NATS_TOKEN"); v != "" {
		c.Token = v
	}
}

type DeliveryPolicy struct {
	jetstream.DeliverPolicy
}

func (p *DeliveryPolicy) UnmarshalText(text []byte) error {
	jsstring := `"` + string(text) + `"`
	return p.DeliverPolicy.UnmarshalJSON([]byte(jsstring))
}

// NatsReceiver consumes Minio Bucket events from a Nats stream
type NatsReceiver struct {
	connOpts    nats.Options
	consumerCfg jetstream.ConsumerConfig
	stream      string

	// If true, handle object stat (HEAD) requests the same
	// as set (CREATE if not exists). Used for back-filling from object stats.
	SetFromStat bool
}

// NewNatsReceiver constructs a NatsReceiver using Nats connection options,
// an existing stream name, and stream consumer options
func NewNatsReceiver(cfg NatsConfig) *NatsReceiver {
	connOpts := nats.GetDefaultOptions()
	connOpts.Servers = cfg.Servers
	connOpts.Secure = cfg.Secure
	connOpts.Nkey = cfg.Nkey
	connOpts.User = cfg.User
	connOpts.Password = cfg.Password
	connOpts.Token = cfg.Token

	// used if Nkey is set
	if connOpts.Nkey != "" {
		connOpts.SignatureCB = func(nonce []byte) ([]byte, error) {
			seed := []byte(cfg.NkeyPrivate)
			if keyData, err := os.ReadFile(cfg.NkeyPrivate); err == nil {
				if keyData = bytes.TrimSpace(keyData); len(keyData) > 0 {
					seed = keyData
				}
			}
			u, err := nkeys.FromSeed(seed)
			if err != nil {
				return nil, err
			}
			return u.Sign(nonce)
		}
	}

	consumer := jetstream.ConsumerConfig{
		Name:              cfg.Durable,
		Durable:           cfg.Durable,
		InactiveThreshold: cfg.InactiveThreshold,
		DeliverPolicy:     cfg.DeliveryPolicy.DeliverPolicy,
		OptStartSeq:       cfg.DeliveryStartSeq,
		OptStartTime:      cfg.DeliveryStartTime,
	}

	return &NatsReceiver{
		connOpts:    connOpts,
		consumerCfg: consumer,
		stream:      cfg.Stream,
	}
}

// Listen to a Nats stream, consuming Bucket events and passing them to the event channel.
// Blocks until the context is cancelled.
func (r *NatsReceiver) Listen(ctx context.Context, events chan<- *BucketEvent) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	slog.Info("Connecting to nats", slog.Any("servers", r.connOpts.Servers))
	fmt.Println("nkey", r.connOpts.Nkey)
	conn, err := r.connOpts.Connect()
	if err != nil {
		return fmt.Errorf("failed to connect to nats server: %w", err)
	}
	defer conn.Close()

	defer slog.Info("Disconnecting from nats")

	js, err := jetstream.New(conn)
	if err != nil {
		return fmt.Errorf("failed to connect to nats jetstream: %w", err)
	}

	consumer, err := js.CreateOrUpdateConsumer(ctx, r.stream, r.consumerCfg)
	if err != nil {
		return fmt.Errorf("failed to create or update nats consumer: %w", err)
	}

	info := consumer.CachedInfo()
	if info.Delivered.Last != nil {
		slog.Info("Resume reading messages from nats stream",
			slog.String("stream", r.stream),
			slog.Uint64("seqid", info.Delivered.Stream),
			slog.Time("seqtime", *info.Delivered.Last),
		)
	} else {
		slog.Info("Reading messages from nats stream",
			slog.String("stream", r.stream),
		)
	}

	it, err := consumer.Messages()
	if err != nil {
		return fmt.Errorf("failed to iterate nats consumer: %w", err)
	}
	defer it.Stop()

	go func() {
		<-ctx.Done()
		it.Stop()
	}()

	for {
		msg, err := it.Next()
		if errors.Is(err, jetstream.ErrMsgIteratorClosed) {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to receive nats message: %w", err)
		}

		md, _ := msg.Metadata()

		var evt BucketEvent
		if err = json.Unmarshal(msg.Data(), &evt); err != nil {
			msg.TermWithReason("bad json format")
			slog.Warn("failed to decode nats jetstream bucket event (skipping)",
				slog.Any("error", err.Error()),
				slog.Uint64("seqid", md.Sequence.Stream),
			)
			continue
		}

		// Only interested in a subset of events
		typ := evt.Type()
		switch typ {
		case BucketEventOther, BucketEventStat:
			if typ == BucketEventStat && r.SetFromStat {
				// allow
				break
			}
			msg.Ack()
			slog.Debug("Skipping BucketEvent", "event", &evt)
			continue
		}

		slog.Debug("Received BucketEvent", "event", &evt)
		select {
		case events <- &evt:
			msg.Ack()
		case <-ctx.Done():
			break
		}
	}

	return nil
}
