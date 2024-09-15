package pkg

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/madmin-go/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDataSize(t *testing.T) {
	mustNew := func(size string) DataSize {
		ds, err := NewDataSize(size)
		require.NoError(t, err)
		return ds
	}

	_, err := NewDataSize("")
	assert.Error(t, err)

	assert.Equal(t, int(0), int(DataSize(0)))
	assert.Equal(t, int(0), int(mustNew("0")))
	assert.Equal(t, 10*humanize.Byte, int(mustNew("10")))
	assert.Equal(t, 100*humanize.MByte, int(mustNew("100MB")))
	assert.Equal(t, 10*humanize.GiByte, int(mustNew("10 GiB")))
}

func TestCleanupPolicy_IsValid(t *testing.T) {
	cases := []struct {
		name   string
		policy *CleanupPolicy
		valid  bool
	}{
		{"zero", &CleanupPolicy{}, false},
		{"no bucket", &CleanupPolicy{TargetSize: 10}, false},
		{"no size", &CleanupPolicy{Bucket: "name"}, false},
		{"valid", &CleanupPolicy{Bucket: "name", TargetSize: 10}, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.valid, tc.policy.IsValid())
		})
	}
}

func TestCleanupPolicy_SetTargetSize(t *testing.T) {
	cases := []struct {
		name    string
		size    string
		expect  uint64
		wantErr bool
	}{
		{"empty", "", 0, true},
		{"spaces", "  ", 0, true},
		{"150 MB", "150 MB", 150 * humanize.MByte, false},
		{"200G", "200G", 200 * humanize.GByte, false},
		{"10.5 G", "10.5 G", 10.5 * humanize.GByte, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var policy CleanupPolicy
			err := policy.SetTargetSize(tc.size)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, int64(tc.expect), int64(policy.TargetSize))
		})
	}
}

func TestMinioManager_clusterStats(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	manager := NewTestMinioManager(t, "cluster-stats")
	usage, err := manager.clusterStats(ctx)
	require.NoError(t, err)
	require.NotNil(t, usage)
	assert.Equal(t, usage.TotalCapacity, usage.TotalFreeCapacity)

	var size int64 = 3
	PutObject(t, manager, "cluster-stats", "key", size)

	// need to serve our own mock stats, because minio scanner isn't
	// fast enough to respond to immediate object changes
	manager.minioUsageFn = func(_ context.Context) (madmin.DataUsageInfo, error) {
		usage.TotalUsedCapacity = uint64(size)
		return *usage, nil
	}

	usage, err = manager.clusterStats(ctx)
	require.NoError(t, err)
	assert.Equal(t, size, int64(usage.TotalUsedCapacity))
}

func TestMinioManager_runOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	type ExpectBucket struct {
		bucket string
		count  int
		size   int
	}

	tests := []struct {
		name     string
		objSize  int
		objCount int
		policies []*CleanupPolicy
		expect   map[string]ExpectBucket
	}{
		{
			name:     "no cleanup",
			objSize:  1,
			objCount: 10,
			policies: []*CleanupPolicy{
				{Bucket: "run-once-1", TargetSize: 10 * humanize.Byte},
			},
			expect: map[string]ExpectBucket{
				"run-once-1": {count: 10, size: 10 * (1 * humanize.Byte)},
			},
		},
		{
			name:     "remove all",
			objSize:  1,
			objCount: 10,
			policies: []*CleanupPolicy{
				{Bucket: "run-once-2", TargetSize: 0 * humanize.Byte, AllowRemoveAll: true},
			},
			expect: map[string]ExpectBucket{
				"run-once-2": {count: 0, size: 0 * humanize.Byte},
			},
		},
		{
			name:     "remove half",
			objSize:  2,
			objCount: 10,
			policies: []*CleanupPolicy{
				{Bucket: "run-once-3", TargetSize: 10 * humanize.Byte},
			},
			expect: map[string]ExpectBucket{
				"run-once-3": {count: 5, size: 5 * (2 * humanize.Byte)},
			},
		},
		{
			name:     "remove almost target",
			objSize:  3,
			objCount: 10,
			policies: []*CleanupPolicy{
				{Bucket: "run-once-4", TargetSize: 7 * humanize.Byte},
			},
			expect: map[string]ExpectBucket{
				"run-once-4": {count: 3, size: 3 * (3 * humanize.Byte)},
			},
		},
		{
			name:     "remove none",
			objSize:  1,
			objCount: 10,
			policies: []*CleanupPolicy{
				{Bucket: "run-once-5", TargetSize: 0 * humanize.Byte},
			},
			expect: map[string]ExpectBucket{
				"run-once-5": {count: 10, size: 10 * (1 * humanize.Byte)},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buckets []string
			for _, policy := range tt.policies {
				buckets = append(buckets, policy.Bucket)
			}
			manager := NewTestMinioManager(t, buckets...)

			// no policies
			require.NoError(t, manager.runOnce(ctx))

			manager.cfg.BucketPolicies = tt.policies

			for _, bucket := range buckets {
				for i := range tt.objCount {
					PutObject(t, manager, bucket, fmt.Sprintf("key%d", i), int64(tt.objSize))
				}
			}

			require.NoError(t, manager.runOnce(ctx))

			for bucket, expect := range tt.expect {
				count, err := manager.store.Count(bucket)
				require.NoError(t, err)
				assert.Equal(t, expect.count, count, "wrong count")

				size, err := manager.store.Size(bucket)
				require.NoError(t, err)
				assert.Equal(t, expect.size, size, "wrong size")
			}

			// One more time, no change
			require.NoError(t, manager.runOnce(ctx))
			for bucket, expect := range tt.expect {
				count, err := manager.store.Count(bucket)
				require.NoError(t, err)
				assert.Equal(t, expect.count, count, "wrong count")
			}
		})
	}
}

func TestMinioManager_MaxRemoveSize(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	bucket := "maxremovesize1"
	manager := NewTestMinioManager(t, bucket)

	N := 100
	size := 1 * humanize.Byte

	put := func() {
		for i := range N {
			PutObject(t, manager, bucket, fmt.Sprintf("key%d", i), int64(size))
		}
	}

	put()
	manager.cfg.BucketPolicies = []*CleanupPolicy{
		{Bucket: bucket, TargetSize: 10 * humanize.Byte},
	}
	// no max size
	require.NoError(t, manager.runOnce(ctx))
	count, err := manager.store.Count(bucket)
	require.NoError(t, err)
	assert.Equal(t, 10, count, "wrong count")

	ClearBucket(t, manager, bucket)
	put()
	manager.cfg.BucketPolicies = []*CleanupPolicy{
		{Bucket: bucket, TargetSize: 10 * humanize.Byte, MaxRemoveSize: 25 * humanize.Byte},
	}
	// up to 25 bytes at a time
	require.NoError(t, manager.runOnce(ctx))
	count, err = manager.store.Count(bucket)
	require.NoError(t, err)
	assert.Equal(t, 75, count, "wrong count")

	// up to 25 bytes at a time * 2
	require.NoError(t, manager.runOnce(ctx))
	require.NoError(t, manager.runOnce(ctx))
	count, err = manager.store.Count(bucket)
	require.NoError(t, err)
	assert.Equal(t, 25, count, "wrong count")

	// up to 25 bytes at a time, but the target is still 10 at the end
	require.NoError(t, manager.runOnce(ctx))
	count, err = manager.store.Count(bucket)
	require.NoError(t, err)
	assert.Equal(t, 10, count, "wrong count")
}
