package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	flag "github.com/spf13/pflag"
	"golang.org/x/sync/errgroup"

	"github.com/justinfx/minio_cleaner/pkg"
)

var (
	Version  = "dev"
	logLevel slog.LevelVar
)

func main() {
	logLevel.Set(slog.LevelInfo)

	flag.CommandLine.SortFlags = false
	flag.Usage = func() {
		name := filepath.Base(os.Args[0])
		fmt.Fprintf(os.Stderr, "Usage: %s -c|--config <config.toml> [flags]\n", name)
		flag.PrintDefaults()
	}

	configFlag := flag.StringP("config", "c", "", "config file path")
	verboseFlag := flag.BoolP("verbose", "v", false, "verbose log output")
	versionFlag := flag.BoolP("version", "V", false, "print version")
	helpFlag := flag.BoolP("help", "h", false, "print help")
	flag.Parse()

	if *helpFlag {
		flag.Usage()
		os.Exit(0)
	}

	if *versionFlag {
		fmt.Println(Version)
		os.Exit(0)
	}

	if *configFlag == "" {
		flag.Usage()
		fmt.Fprintln(os.Stderr, "\nconfig flag is required")
		os.Exit(1)
	}

	config, err := parseConfig(*configFlag)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	config.LoadEnvVars()

	if *verboseFlag {
		config.LogLevel = "debug"
	}
	if lvl, err := parseLogLevel(config.LogLevel); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	} else {
		logLevel.Set(lvl)
	}

	if err := config.Validate(); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	setupLogging(config)

	minioSecret := config.MinioConfig.SecretKey
	config.MinioConfig.SecretKey = "...redacted..."
	cfgData, _ := json.Marshal(config)
	slog.Debug("loaded config", "config", string(cfgData))
	config.MinioConfig.SecretKey = minioSecret

	run(config)
}

func setupLogging(config *AppConfig) {
	var logHandler slog.Handler
	logOpts := &slog.HandlerOptions{Level: &logLevel}
	if config.LogJson {
		logHandler = slog.NewJSONHandler(os.Stderr, logOpts)
	} else {
		logHandler = slog.NewTextHandler(os.Stderr, logOpts)
	}
	slog.SetDefault(slog.New(logHandler))
}

func run(config *AppConfig) {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	evts := make(chan *pkg.BucketEvent)

	receiver := pkg.NewNatsReceiver(config.NatsConfig)
	receiver.SetFromStat = config.SetFromStat

	store, err := pkg.NewSQLiteBucketStore(config.StoreConfig)
	if err != nil {
		slog.Error("Failed to initialize bucket event store", "error", err)
		os.Exit(1)
	}
	defer store.Close()

	manager := pkg.NewMinioManager(&config.MinioConfig, store)

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
