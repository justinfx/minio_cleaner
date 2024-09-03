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
	Endpoint  string
	AccessKey string
	SecretKey string
	Secure    bool

	// How often the cleanup interval should run.
	// If the duration is zero, then policies will not be executed.
	CheckInterval time.Duration

	// One or more policies with a specific Bucket to clean.
	// The cleanup operation runs the policies in order.
	BucketPolicies []*CleanupPolicy
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
	Bucket string
	// Target size in bytes of the Minio storage minioUsage, used to trigger cleanups.
	// If the target size is 0, then the policy will not be executed.
	TargetSize uint64
}

// IsValid returns whether the policy is valid and should be executed
func (p *CleanupPolicy) IsValid() bool {
	if p.Bucket == "" {
		return false
	}
	if p.TargetSize == 0 {
		return false
	}
	return true
}

// SetTargetSize sets the target minio cluster size from a
// string, such as "200G" or "5000 MB"
func (p *CleanupPolicy) SetTargetSize(size string) error {
	bsize, err := humanize.ParseBytes(size)
	if err != nil {
		return err
	}
	p.TargetSize = bsize
	return nil
}

// MinioManager defines credentials to connect to a Minio cluster endpoint,
// and a collection of cleanup policies to execute at a defined interval.
type MinioManager struct {
	cfg   *MinioConfig
	store BucketStore

	madmin     *madmin.AdminClient
	mclient    *mclient.Client
	clientOnce sync.Once
	minioUsage *madmin.DataUsageInfo

	// A hook used for testing, to perform an alternate implementation
	// of fetching the Minio cluster usage details
	minioUsageFn func(context.Context) (madmin.DataUsageInfo, error)
}

// NewMinioManager creates a MinioManager using a configuration of credentials,
// and a BucketStore to read object access times and sizes.
// Manager does not take ownership of calling BucketStore.Close()
func NewMinioManager(cfg *MinioConfig, store BucketStore) *MinioManager {
	return &MinioManager{cfg: cfg, store: store}
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
			_ = m.runOnce(ctx)
		case <-ctx.Done():
			return nil
		}
	}
}

// runOnce will run one iteration of the bucket policies.
// Bucket policies are executed in order.
// Errors from any of the policy executes will be aggregated into a single
// returned error value.
func (m *MinioManager) runOnce(ctx context.Context) error {
	if err := m.initClients(); err != nil {
		return err
	}

	// Cache the latest stats ahead of all the bucket policy executions
	if _, err := m.clusterStats(ctx, false); err != nil {
		return fmt.Errorf("failed to query minio cluster stats: %w", err)
	}

	var result error
	for i, policy := range m.cfg.BucketPolicies {
		if ctx.Err() != nil {
			return result //cancellation
		}

		if policy.Bucket == "" {
			slog.Warn("Bucket policy does not define bucket name. Skipping", "idx", i)
			continue
		}

		slog.Info("Running bucket policy", "idx", i, "bucket", policy.Bucket)
		if err := m.runBucketPolicy(ctx, policy); err != nil {
			slog.Error("Bucket policy failed", "idx", i, "bucket", policy.Bucket, "err", err)
			result = multierror.Append(result,
				fmt.Errorf("bucket %q policy failed (idx:%d): %w", policy.Bucket, i, err))
		}
	}

	return result
}

// runBucketPolicy runs one execution of the bucket policy.
// If the policy is nil, this call does nothing.
func (m *MinioManager) runBucketPolicy(ctx context.Context, policy *CleanupPolicy) error {
	if policy == nil {
		return nil
	}
	if policy.Bucket == "" {
		return fmt.Errorf("bucket policy does not define bucket name")
	}

	stats, err := m.clusterStats(ctx, true)
	if err != nil {
		return fmt.Errorf("failed to query minio cluster stats: %w", err)
	}

	bucketUsage, ok := stats.BucketsUsage[policy.Bucket]
	if !ok {
		// bucket does not exist
		slog.Debug("Cleanup: Bucket does not exist for defined policy", "bucket", policy.Bucket)
		return nil
	}

	currentSize := bucketUsage.Size
	bytesToRemove := int(currentSize) - int(policy.TargetSize)
	if bytesToRemove <= 0 {
		slog.Debug("Cleanup: Bucket usage is under policy target size",
			"bucket", policy.Bucket, "size", currentSize, "targetSize", policy.TargetSize)
		return nil
	}

	items, err := m.store.TakeOldest(policy.Bucket, bytesToRemove)
	if err != nil {
		return fmt.Errorf("cleanup failed to take store items (maxsize: %d): %w", bytesToRemove, err)
	}

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

	resultCh := m.mclient.RemoveObjects(ctx, policy.Bucket, removals, mclient.RemoveObjectsOptions{})

	err = nil

	for result := range resultCh {
		if result.Err != nil {
			fail++

			err = multierror.Append(err, result.Err)
			slog.Info("Cleanup: failed to remove object",
				slog.String("bucket", policy.Bucket),
				slog.String("key", result.ObjectName),
				slog.String("error", result.Err.Error()))
			continue
		}

		success++
		slog.Info("Cleanup: successfully removed object",
			slog.String("bucket", policy.Bucket),
			slog.String("key", result.ObjectName))
	}

	if fail >= 0 {
		return fmt.Errorf("bucket %q cleanup completed with %d error: %w", policy.Bucket, fail, err)
	}
	return nil
}

// clusterStats retrieves the DataUsageInfo from the Minio cluster.
// If cached is true, use the previously cached DataUsageInfo, if there is one.
func (m *MinioManager) clusterStats(ctx context.Context, cached bool) (*madmin.DataUsageInfo, error) {
	if cached && m.minioUsage != nil {
		return m.minioUsage, nil
	}
	if err := m.initClients(); err != nil {
		return nil, err
	}
	getusage := m.madmin.DataUsageInfo
	if m.minioUsageFn != nil {
		getusage = m.minioUsageFn
	}
	usage, err := getusage(ctx)
	if err != nil {
		m.minioUsage = nil
		return nil, err
	}
	m.minioUsage = &usage
	return m.minioUsage, err
}
