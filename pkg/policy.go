package pkg

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/hashicorp/go-multierror"
	madmin "github.com/minio/madmin-go/v3"
	mclient "github.com/minio/minio-go/v7"
	mcreds "github.com/minio/minio-go/v7/pkg/credentials"
)

type MinioConfig struct {
	Endpoint  string `toml:"endpoint"`
	AccessKey string `toml:"access_key"`
	SecretKey string `toml:"secret_key"`
	Secure    bool   `toml:"secure"`

	// How often the cleanup interval should run.
	// If the duration is zero, then policies will not be executed.
	CheckInterval time.Duration `toml:"check_interval"`

	// One or more policies with a specific Bucket to clean.
	// The cleanup operation runs the policies in order.
	BucketPolicies []*CleanupPolicy `toml:"policies"`
}

func (c *MinioConfig) Validate() error {
	if c.Endpoint == "" {
		return errors.New("minio endpoint is required")
	}
	if c.AccessKey == "" {
		return errors.New("minio access key is required")
	}
	if c.SecretKey == "" {
		return errors.New("minio secret key is required")
	}
	var err error
	for i, policy := range c.BucketPolicies {
		if e := policy.Validate(); e != nil {
			err = multierror.Append(err, fmt.Errorf("bucket policy[%d] is invalid: %s", i, e.Error()))
		}
	}
	return err
}

// NewCleanupPolicy creates a new policy for a target bucket, and
// a target usage size specified as a string (ie "200G").
func NewCleanupPolicy(bucket, targetSize string) (*CleanupPolicy, error) {
	if bucket == "" {
		return nil, errors.New("bucket name is required")
	}
	pol := &CleanupPolicy{Bucket: bucket}
	if err := pol.SetTargetSize(targetSize); err != nil {
		return nil, err
	}
	return pol, nil
}

// CleanupPolicy defines a policy for a single bucket.
type CleanupPolicy struct {
	Bucket string `toml:"bucket"`

	// Target size in bytes of the Minio storage minioUsage, used to trigger cleanups.
	// If the target size is 0, then the policy will not be executed.
	TargetSize DataSize `toml:"target_size"`

	// Must be explicitly set to true if you want to set target_size
	// to 0 and remove all data on each policy execution.
	AllowRemoveAll bool `toml:"allow_remove_all"`

	// If greater than zero, limit the size of data that will be removed during
	// each execution of the policy.
	MaxRemoveSize DataSize `toml:"max_remove_size"`
}

// Validate returns an error if the policy is not defined correctly.
func (p *CleanupPolicy) Validate() error {
	if p.Bucket == "" {
		return errors.New("bucket name is required")
	}
	return nil
}

// IsValid returns whether the policy is valid and should be executed
func (p *CleanupPolicy) IsValid() bool {
	if p.Bucket == "" {
		return false
	}
	if p.TargetSize == 0 && !p.AllowRemoveAll {
		return false
	}
	return true
}

// SetTargetSize sets the target minio cluster size from a
// string, such as "200G" or "5000 MB"
func (p *CleanupPolicy) SetTargetSize(size string) error {
	return p.TargetSize.SetFromString(size)
}

type bucketSizeMode int

const (
	// size of bucket is read as a total from objects in database
	bucketSizeModeDB bucketSizeMode = 0
	// size of bucket is read from Minio cluster disk stats
	bucketSizeModeS3 bucketSizeMode = 1
)

// MinioManager defines credentials to connect to a Minio cluster endpoint,
// and a collection of cleanup policies to execute at a defined interval.
type MinioManager struct {
	cfg   *MinioConfig
	store BucketStore

	madmin     *madmin.AdminClient
	mclient    *mclient.Client
	clientOnce sync.Once

	// A hook used for testing, to perform an alternate implementation
	// of fetching the Minio cluster usage details
	minioUsageFn func(context.Context) (madmin.DataUsageInfo, error)

	bucketSizeMode bucketSizeMode
}

// NewMinioManager creates a MinioManager using a configuration of credentials,
// and a BucketStore to read object access times and sizes.
// Manager does not take ownership of calling BucketStore.Close()
func NewMinioManager(cfg *MinioConfig, store BucketStore) *MinioManager {
	return &MinioManager{cfg: cfg, store: store, bucketSizeMode: bucketSizeModeDB}
}

// initClients sets up the minio client and admin interfaces.
// Only executes once, and all subsequent calls are no-op.
func (m *MinioManager) initClients() error {
	var err error
	m.clientOnce.Do(func() {
		creds := mcreds.NewStaticV4(m.cfg.AccessKey, m.cfg.SecretKey, "")
		madm, err := madmin.NewWithOptions(m.cfg.Endpoint, &madmin.Options{
			Creds:  creds,
			Secure: m.cfg.Secure,
		})
		if err != nil {
			err = fmt.Errorf("error creating minio admin client: %w", err)
			return
		}
		m.madmin = madm

		mc, err := mclient.New(m.cfg.Endpoint, &mclient.Options{
			Creds:  creds,
			Secure: m.cfg.Secure,
		})
		if err != nil {
			err = fmt.Errorf("error creating minio client: %w", err)
			return
		}
		m.mclient = mc
	})
	if err != nil {
		m.clientOnce = sync.Once{}
	}
	return err
}

// Run starts a blocking loop and executes the bucket
// policies after the configured interval.
// Call returns when the context is cancelled.
// If the interval is not > 0, an error will be returned.
func (m *MinioManager) Run(ctx context.Context) error {
	interval := m.cfg.CheckInterval
	if interval == 0 {
		return fmt.Errorf("check interval cannot be zero")
	}

	if err := m.initClients(); err != nil {
		return err
	}

	for {
		select {
		case <-time.After(interval):
			// errors are logged, and loop keeps trying
			slog.Info("Running policies")
			if err := m.runOnce(ctx); err != nil {
				slog.Error("Policy run failed", "err", err)
			}
		case <-ctx.Done():
			return nil
		}
	}
}

type bucketSizeFn func(bucket string) (size int, err error)

// runOnce will run one iteration of the bucket policies.
// Bucket policies are executed in order.
// Errors from any of the policy executes will be aggregated into a single
// returned error value.
func (m *MinioManager) runOnce(ctx context.Context) error {
	if len(m.cfg.BucketPolicies) == 0 {
		slog.Warn("No cleanup policies have been set. Nothing to do.")
		return nil
	}

	if err := m.initClients(); err != nil {
		return err
	}

	var (
		errs         error
		totalRemoved int
	)

	var bucketSizer bucketSizeFn

	switch m.bucketSizeMode {

	case bucketSizeModeDB:
		bucketSizer = func(bucket string) (int, error) {
			return m.store.Size(bucket)
		}

	case bucketSizeModeS3:
		// Minio has a delayed update for its disk usage, via its async scanner.
		// So we need to track the timestamp of the stats and not process the
		// policies again until we have a stats update that has changed.
		stats, err := m.clusterStats(ctx)
		if err != nil {
			return fmt.Errorf("failed to query minio cluster stats: %w", err)
		}
		lastUpdate, err := m.store.LastClusterUpdate()
		if err != nil {
			return err
		}
		if !lastUpdate.IsZero() && !stats.LastUpdate.After(lastUpdate) {
			slog.Info("Minio cluster storage info has not updated since last run",
				"last_update", lastUpdate)
			return nil
		}
		bucketSizer = func(bucket string) (int, error) {
			return int(stats.BucketsUsage[bucket].Size), nil
		}
		defer func() {
			// If we removed at least one object, mark this timestamp to prevent the policies
			// from running again until we get new Minio stats.
			if totalRemoved > 0 {
				slog.Info("Updating last cluster stat timestamp", "last_update", stats.LastUpdate)
				if err := m.store.SetLastClusterUpdate(stats.LastUpdate); err != nil {
					slog.Error("Failed to set the last cluster update", "err", err)
					errs = multierror.Append(errs,
						fmt.Errorf("failed to set last cluster update: %w", err))
				}
			}
		}()

	}

	for i, policy := range m.cfg.BucketPolicies {
		if ctx.Err() != nil {
			break //cancellation
		}

		if policy.Bucket == "" {
			slog.Warn("Bucket policy does not define bucket name. Skipping", "idx", i)
			continue
		}

		slog.Info("Running bucket policy", "idx", i, "bucket", policy.Bucket)
		removed, err := m.runBucketPolicy(ctx, policy, bucketSizer)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				break
			}
			slog.Error("Bucket policy failed", "idx", i, "bucket", policy.Bucket, "err", err)
			errs = multierror.Append(errs,
				fmt.Errorf("bucket %q policy failed (idx:%d): %w", policy.Bucket, i, err))
		}
		totalRemoved += removed
	}

	return errs
}

// runBucketPolicy runs one execution of the bucket policy, and returns the number of
// objects that have been removed.
// If the policy is nil, this call does nothing.
func (m *MinioManager) runBucketPolicy(ctx context.Context, policy *CleanupPolicy, sizeFn bucketSizeFn) (int, error) {
	if policy == nil {
		return 0, nil
	}
	if policy.Bucket == "" {
		return 0, fmt.Errorf("bucket policy does not define bucket name")
	}
	if !policy.IsValid() {
		return 0, nil
	}

	bucketSize, err := sizeFn(policy.Bucket)
	if err != nil {
		return 0, err
	}

	bytesToRemove := bucketSize - int(policy.TargetSize)
	if bytesToRemove <= 0 {
		slog.Info("Cleanup: Bucket usage is under policy target size",
			"bucket", policy.Bucket,
			"current_size", humanize.IBytes(uint64(bucketSize)),
			"target_size", humanize.IBytes(uint64(policy.TargetSize)))
		return 0, nil
	}

	if policy.MaxRemoveSize > 0 {
		bytesToRemove = min(bytesToRemove, int(policy.MaxRemoveSize))
	}

	items, err := m.store.TakeOldest(policy.Bucket, bytesToRemove)
	if err != nil {
		return 0, fmt.Errorf("cleanup failed to take store items (maxsize: %d): %w", bytesToRemove, err)
	}

	slog.Info("Cleanup: processing objects for removal",
		"bucket", policy.Bucket,
		"to_remove", len(items),
		"current_size", humanize.IBytes(uint64(bucketSize)),
		"max_removal_size", humanize.IBytes(uint64(bytesToRemove)),
		"target_size", humanize.IBytes(uint64(policy.TargetSize)))

	removals := make(chan mclient.ObjectInfo, len(items))
	go func() {
		defer close(removals)
		for _, item := range items {
			removals <- mclient.ObjectInfo{Key: item.Key}
		}
	}()

	var success, fail int

	// The Minio API does not seem to break on context cancellation,
	// so we are expected to drain the channel to avoid a goroutine leak.
	// Using a generous request timeout to ensure we don't try for too long
	// under remote error conditions.
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()

	resultCh := m.mclient.RemoveObjectsWithResult(ctx, policy.Bucket, removals, mclient.RemoveObjectsOptions{})

	err = nil

	for result := range resultCh {
		if result.Err != nil {
			if errors.Is(result.Err, context.Canceled) {
				break
			}
			fail++

			err = multierror.Append(err, result.Err)
			slog.Warn("Cleanup: failed to remove object",
				slog.String("bucket", policy.Bucket),
				slog.String("key", result.ObjectName),
				slog.String("error", result.Err.Error()))
			continue
		}

		success++
		slog.Debug("Cleanup: successfully removed object",
			slog.String("bucket", policy.Bucket),
			slog.String("key", result.ObjectName))
	}

	if fail > 0 {
		err = fmt.Errorf("bucket %q cleanup completed (errors: %d, removed: %d): %w",
			policy.Bucket, fail, success, err)
		return success, err
	}
	slog.Info("Bucket cleanup completed", "bucket", policy.Bucket, "removed", success)
	return success, nil
}

// clusterStats retrieves the DataUsageInfo from the Minio cluster.
func (m *MinioManager) clusterStats(ctx context.Context) (*madmin.DataUsageInfo, error) {
	if err := m.initClients(); err != nil {
		return nil, err
	}
	getusage := m.madmin.DataUsageInfo
	if m.minioUsageFn != nil {
		getusage = m.minioUsageFn
	}
	usage, err := getusage(ctx)
	if err != nil {
		return nil, err
	}
	return &usage, err
}

type DataSize uint64

// NewDataSize returns a new DataSize, parsed from a human-readable string.
// ie: "500GB", or "100 MB"
func NewDataSize(size string) (DataSize, error) {
	var ds DataSize
	if err := ds.SetFromString(size); err != nil {
		return ds, err
	}
	return ds, nil
}

func (s *DataSize) UnmarshalText(text []byte) error {
	return s.SetFromString(string(text))
}

func (s *DataSize) SetFromString(size string) error {
	bsize, err := humanize.ParseBytes(size)
	if err != nil {
		return fmt.Errorf("invalid size format %q: %w", size, err)
	}
	*s = DataSize(bsize)
	return nil
}
