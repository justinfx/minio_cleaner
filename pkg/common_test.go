package pkg

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	mcli "github.com/minio/cli"
	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio-go/v7"
	mcreds "github.com/minio/minio-go/v7/pkg/credentials"
	mcmd "github.com/minio/minio/cmd"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	// disable minio server console feature, to avoid port binding conflicts
	os.Setenv("MINIO_BROWSER", "off")

	// disable the minio cli from exiting on errors
	mcli.OsExiter = func(exitCode int) {}

	slog.SetLogLoggerLevel(slog.LevelError)
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))

	exitCode := m.Run()

	for _, fn := range teardownFuncs {
		fn()
	}

	os.Exit(exitCode)
}

var teardownFuncs []func()

const (
	minioTestKey = "miniotest"
	minioTestPas = "miniotest"
)

var (
	minioAddr     string
	initMinioOnce sync.Once
)

func StartTestMinio(t *testing.T) *MinioConfig {
	t.Helper()

	initMinioOnce.Do(func() {
		storePath, err := os.MkdirTemp("", "")
		require.NoError(t, err)

		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)

		minioAddr = l.Addr().String()
		require.NoError(t, l.Close())

		u1, p1 := os.Getenv("MINIO_ROOT_USER"), os.Getenv("MINIO_ROOT_PASSWORD")
		os.Setenv("MINIO_ROOT_USER", minioTestKey)
		os.Setenv("MINIO_ROOT_PASSWORD", minioTestPas)
		defer func() {
			os.Setenv("MINIO_ROOT_USER", u1)
			os.Setenv("MINIO_ROOT_PASSWORD", p1)
		}()

		// start the minio server in a goroutine and wait until it reports ready
		starting := make(chan bool, 1)

		go func() {
			starting <- true
			mcmd.Main([]string{
				"minio", "server",
				"--quiet",
				"--address", minioAddr,
				"--console-address", "localhost:0",
				storePath})
		}()

		<-starting
		ctx := context.Background()

		admin, err := madmin.NewWithOptions(minioAddr, &madmin.Options{
			Creds:  mcreds.NewStaticV4(minioTestKey, minioTestPas, ""),
			Secure: false,
		})
		require.NoError(t, err)

		for i := 0; i < 200; i++ {
			if _, err = admin.ServerInfo(ctx); err == nil {
				_, _, err := admin.ServerHealthInfo(ctx,
					[]madmin.HealthDataType{madmin.HealthInfoVersion2}, 5*time.Second, "")
				if err == nil {
					break
				}
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		require.NoError(t, err, "failed to connect to embedded test minio")

		shutdownMinioFn := func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			admin.ServiceStop(ctx)
		}
		teardownFuncs = append(teardownFuncs, shutdownMinioFn)
	})

	return &MinioConfig{
		Endpoint:  minioAddr,
		AccessKey: minioTestKey,
		SecretKey: minioTestPas,
		Secure:    false,
	}
}

func StartTestStore(t *testing.T) BucketStore {
	store, err := NewSQLiteBucketStore(SQLiteInMemory)
	require.NoError(t, err)
	t.Cleanup(store.Close)
	return store
}

func NewTestMinioManager(t *testing.T, buckets ...string) *MinioManager {
	manager := NewMinioManager(StartTestMinio(t), StartTestStore(t))
	require.NoError(t, manager.initClients())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// TODO: trying to increase the scanner speed, to report minio data
	//   usage faster, for the sake of testing. It's not working.
	_, err := manager.madmin.SetConfigKV(ctx, "scanner delay=0 speed=fast")
	require.NoError(t, err)

	for _, bucket := range buckets {
		require.NoError(t, manager.mclient.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}))
	}
	return manager
}

func PutObject(t *testing.T, manager *MinioManager, bucket, key string, size int64) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	data := bytes.NewReader(bytes.Repeat([]byte(`x`), int(size)))
	_, err := manager.mclient.PutObject(ctx, bucket, key,
		data, data.Size(), minio.PutObjectOptions{})
	require.NoError(t, err)

	require.NoError(t, manager.store.Set(&BucketStoreItem{
		Bucket:     bucket,
		Key:        key,
		AccessTime: time.Now(),
		Size:       int(size),
	}))
}
