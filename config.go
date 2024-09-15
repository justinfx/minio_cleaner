package main

import (
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/BurntSushi/toml"

	"github.com/justinfx/minio_cleaner/pkg"
)

type AppConfig struct {
	LogLevel    string `toml:"log_level"`
	LogJson     bool   `toml:"log_json"`
	SetFromStat bool   `toml:"set_from_stat"`

	MinioConfig pkg.MinioConfig `toml:"minio"`
	NatsConfig  pkg.NatsConfig  `toml:"nats"`
	StoreConfig pkg.StoreConfig `toml:"store"`
}

func (c *AppConfig) Validate() error {
	if err := c.MinioConfig.Validate(); err != nil {
		return fmt.Errorf("minio config: %w", err)
	}
	if err := c.NatsConfig.Validate(); err != nil {
		return fmt.Errorf("nats config: %w", err)
	}
	if err := c.StoreConfig.Validate(); err != nil {
		return fmt.Errorf("store config: %w", err)
	}
	return nil
}

func (c *AppConfig) LoadEnvVars() {
	c.NatsConfig.LoadEnvVars()
}

func parseConfig(path string) (*AppConfig, error) {
	var config AppConfig
	fh, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file %q: %v", path, err)
	}
	defer fh.Close()

	_, err = toml.NewDecoder(fh).Decode(&config)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config file %q: %v", path, err)
	}

	return &config, nil
}

func parseLogLevel(name string) (slog.Level, error) {
	switch strings.ToLower(name) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return slog.LevelDebug,
			fmt.Errorf("unknown log level %q must be one of [debug info warn error]", name)
	}
}
