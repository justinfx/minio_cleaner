package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"golang.org/x/sync/errgroup"

	"github.com/justinfx/minio_cleaner/pkg"
)

func main() {
	var level slog.LevelVar
	level.Set(slog.LevelInfo)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: &level}))
	slog.SetDefault(logger)

	opts := nats.GetDefaultOptions()

	// flags
	natsStreamFlag := flag.String("nats-stream", "MINIO", "Nats stream name.")
	natsDurableFlag := flag.String("nats-durable", "minio-bucket-consumer", "Nats durable name")
	dbPathFlag := flag.String("db-path", "minio_cleaner.sqlite", "Db path")

	flag.Parse()

	stream := *natsStreamFlag
	durable := *natsDurableFlag
	dbpath := *dbPathFlag

	// TODO: from flags
	level.Set(slog.LevelInfo)
	opts.Servers = []string{"nats://127.0.0.1:4222"}
	setObjectFromStat := true

	consumer := jetstream.ConsumerConfig{
		Name:    durable,
		Durable: durable,
		// TODO: from flags?
		InactiveThreshold: 1 * time.Hour,
	}

	// TODO: from flags/config
	minioCfg := &pkg.MinioConfig{
		Endpoint:      "localhost:9000",
		AccessKey:     "minioadmin",
		SecretKey:     "minioadmin",
		CheckInterval: 10 * time.Second,
		BucketPolicies: []*pkg.CleanupPolicy{
			&pkg.CleanupPolicy{Bucket: "testbucket", TargetSize: 500 * humanize.MiByte},
		},
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	evts := make(chan *pkg.BucketEvent)

	receiver := pkg.NewNatsReceiver(opts, stream, consumer)
	receiver.SetFromStat = setObjectFromStat

	store, err := pkg.NewSQLiteBucketStore(dbpath)
	if err != nil {
		slog.Error("Failed to initialize bucket event store", "error", err)
		os.Exit(1)
	}
	defer store.Close()

	manager := pkg.NewMinioManager(minioCfg, store)

	var grp errgroup.Group

	grp.Go(func() error {
		if err := pkg.StoreEvents(ctx, evts, store); err != nil {
			cancel()
			return fmt.Errorf("BucketEvent store Receive failed: %w", err)
		}
		return nil
	})

	grp.Go(func() error {
		if err := receiver.Listen(ctx, evts); err != nil {
			cancel()
			return fmt.Errorf("Nats receiver failed: %w", err)
		}
		return nil
	})

	grp.Go(func() error {
		if err := manager.Run(ctx); err != nil {
			cancel()
			return fmt.Errorf("MinioManager failed: %w", err)
		}
		return nil
	})

	if err := grp.Wait(); err != nil {
		log.Fatal(err)
	}
}
