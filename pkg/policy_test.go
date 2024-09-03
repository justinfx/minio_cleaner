package pkg

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	manager := NewTestMinioManager(t, "bucket")
	usage, err := manager.clusterStats(ctx, false)
	require.NoError(t, err)
	require.NotNil(t, usage)
	assert.Equal(t, usage.TotalCapacity, usage.TotalFreeCapacity)

	data := bytes.NewReader([]byte("foo"))
	_, err = manager.mclient.PutObject(ctx, "bucket", "key",
		data, data.Size(), minio.PutObjectOptions{})
	require.NoError(t, err)

	// need to serve our own mock stats, because minio scanner isn't
	// fast enough to respond to immediate object changes
	manager.minioUsageFn = func(_ context.Context) (madmin.DataUsageInfo, error) {
		usage.TotalUsedCapacity = uint64(data.Size())
		return *usage, nil
	}

	usage, err = manager.clusterStats(ctx, true)
	require.NoError(t, err)
	assert.Equal(t, usage.TotalCapacity, usage.TotalFreeCapacity)

	usage, err = manager.clusterStats(ctx, false)
	t.Log(usage.BucketsUsage)
	require.NoError(t, err)
	assert.Equal(t, data.Size(), int64(usage.TotalUsedCapacity))
}
