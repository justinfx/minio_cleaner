package main

import (
	"context"
	"flag"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

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

	level.Set(slog.LevelDebug)
	opts.Servers = []string{"nats://127.0.0.1:4222"}

	stream := *natsStreamFlag
	//stream := "MINIO"

	durable := *natsDurableFlag
	//durable := "minio-bucket-consumer"

	dbpath := *dbPathFlag
	//dbpath := "minio_cleaner.sqlite"
	//dbpath := pkg.SQLiteInMemory
	setFromStat := true

	consumer := jetstream.ConsumerConfig{
		Name:              durable,
		Durable:           durable,
		InactiveThreshold: 1 * time.Hour,
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	evts := make(chan *pkg.BucketEvent)

	var (
		store pkg.BucketStore
		err   error
	)

	receiver := pkg.NewNatsReceiver(opts, stream, consumer)
	receiver.SetFromStat = setFromStat

	//store = pkg.NewConsoleBucketStore()
	store, err = pkg.NewSQLiteBucketStore(dbpath)
	if err != nil {
		slog.Error("Failed to initialize bucket event store", "error", err)
		os.Exit(1)
	}
	defer store.Close()

	go func() {
		if err := pkg.StoreEvents(ctx, evts, store); err != nil {
			slog.Error("BucketEvent store Receive failed", "error", err)
			cancel()
		}
	}()

	// TODO: Cleanup policy manager
	//go func() {
	//	tk := time.NewTicker(5 * time.Second)
	//	defer tk.Stop()
	//	for ctx.Err() == nil {
	//		select {
	//		case <-tk.C:
	//			// pass
	//		case <-ctx.Done():
	//			return
	//		}
	//
	//		ret, err := store.TakeOldest("testbucket", 3)
	//		if err != nil {
	//			slog.Error("BucketEvent TakeOldest failed", "error", err)
	//			continue
	//		}
	//		slog.Debug("BucketEvent TakeOldest returned top 3:")
	//		for i, b := range ret {
	//			fmt.Fprintf(os.Stderr, "  %d: %+v\n", i, b)
	//		}
	//	}
	//}()
	// END DEBUG

	err = receiver.Listen(ctx, evts)
	if err != nil {
		log.Fatal(err)
	}
}
