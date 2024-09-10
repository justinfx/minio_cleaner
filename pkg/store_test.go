package pkg

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/minio/minio-go/v7/pkg/notification"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	slog.SetLogLoggerLevel(slog.LevelWarn)
}

func newTestStore(t *testing.T) BucketStore {
	t.Helper()
	store, err := NewSQLiteBucketStore("")
	require.NoError(t, err)
	store.db.SetMaxOpenConns(1)
	t.Cleanup(store.Close)
	return store
}

func TestBucketStore_Count(t *testing.T) {
	store := newTestStore(t)
	count, err := store.Count("")
	require.NoError(t, err)
	require.Equal(t, 0, count)

	item := &BucketStoreItem{Bucket: "myBucket", Key: "myKey"}

	require.NoError(t, store.Set(item))

	count, err = store.Count("")
	require.Equal(t, 1, count)

	count, err = store.Count(item.Bucket)
	require.Equal(t, 1, count)

	count, err = store.Count("foobucket")
	require.Equal(t, 0, count)
}

func TestBucketStore_Size(t *testing.T) {
	store := newTestStore(t)
	size, err := store.Size("")
	require.NoError(t, err)
	require.Equal(t, 0, size)

	require.NoError(t, store.Set(&BucketStoreItem{Bucket: "myBucket", Key: "myKey", Size: 0}))
	size, err = store.Size("")
	require.Equal(t, 0, size)
	size, err = store.Size("myBucket")
	require.Equal(t, 0, size)

	require.NoError(t, store.Set(&BucketStoreItem{Bucket: "myBucket", Key: "myKey1", Size: 5}))
	size, err = store.Size("myBucket")
	require.Equal(t, 5, size)

	N := 5
	for i := range N {
		key := fmt.Sprintf("newKey%d", i)
		require.NoError(t, store.Set(&BucketStoreItem{Bucket: "myBucket", Key: key, Size: 5}))
	}
	size, err = store.Size("myBucket")
	require.Equal(t, N*5+5, size)
}

func TestBucketStore_Get(t *testing.T) {
	store := newTestStore(t)

	t.Run("missing", func(t *testing.T) {
		item := &BucketStoreItem{Bucket: "get", Key: "missing"}
		actual, err := store.Get(item.Bucket, item.Key)
		require.NoError(t, err)
		require.Nil(t, actual)
	})
	t.Run("existing", func(t *testing.T) {
		item := &BucketStoreItem{Bucket: "get", Key: "existing", Size: 10, AccessTime: time.Now()}
		require.NoError(t, store.Set(item))
		actual, err := store.Get(item.Bucket, item.Key)
		require.NoError(t, err)
		assert.Equal(t, item.Bucket, actual.Bucket)
		assert.Equal(t, item.Key, actual.Key)
		assert.Equal(t, item.Size, actual.Size)
		assert.WithinDuration(t, item.AccessTime, actual.AccessTime, 0)
	})
}

func TestBucketStore_Set(t *testing.T) {
	store := newTestStore(t)

	t.Run("new", func(t *testing.T) {
		item := &BucketStoreItem{Bucket: "set", Key: "newKey1"}
		n, err := store.Count(item.Bucket)
		require.NoError(t, err)
		require.Equal(t, 0, n)
		require.NoError(t, store.Set(item))
		actual, err := store.Get(item.Bucket, item.Key)
		require.NoError(t, err)
		require.Equal(t, item, actual)

		item.AccessTime = time.Now().Add(1 * time.Hour)
		item.Size = 20
		require.NoError(t, store.SetOrUpdate(item))
		actual, err = store.Get(item.Bucket, item.Key)
		require.NoError(t, err)
		assert.Equal(t, item.Size, actual.Size)
		assert.WithinDuration(t, item.AccessTime, actual.AccessTime, 0)
	})

	t.Run("multiple", func(t *testing.T) {
		bucket := "set2"
		items := []*BucketStoreItem{
			&BucketStoreItem{Bucket: bucket, Key: "multiple1"},
			&BucketStoreItem{Bucket: bucket, Key: "multiple2"},
			&BucketStoreItem{Bucket: bucket, Key: "multiple3"},
		}
		n, err := store.Count(bucket)
		require.NoError(t, err)
		require.Equal(t, 0, n)

		require.NoError(t, store.Set(items...))
		n, err = store.Count(bucket)
		require.NoError(t, err)
		require.Equal(t, len(items), n)
	})

	t.Run("with update", func(t *testing.T) {
		item := BucketStoreItem{Bucket: "withupdate", Key: "newKey1", AccessTime: time.Now(), Size: 10}
		require.NoError(t, store.SetOrUpdate(&item))
		item2 := item
		item2.AccessTime = time.Now().Add(1 * time.Hour)
		item2.Size = 20
		require.NoError(t, store.SetOrUpdate(&item2))
		actual, err := store.Get(item2.Bucket, item2.Key)
		require.NoError(t, err)
		assert.Equal(t, item2.Size, actual.Size)
		assert.WithinDuration(t, item2.AccessTime, actual.AccessTime, 0)
	})

	t.Run("no update", func(t *testing.T) {
		item := BucketStoreItem{Bucket: "noupdate", Key: "newKey1", AccessTime: time.Now(), Size: 10}
		require.NoError(t, store.Set(&item))
		item2 := item
		item2.AccessTime = time.Now().Add(1 * time.Hour)
		item2.Size = 20
		require.NoError(t, store.Set(&item2))
		actual, err := store.Get(item2.Bucket, item2.Key)
		require.NoError(t, err)
		assert.Equal(t, item.Size, actual.Size)
		assert.WithinDuration(t, item.AccessTime, actual.AccessTime, 0)
	})
}

func TestBucketStore_Update(t *testing.T) {
	store := newTestStore(t)

	t.Run("missing", func(t *testing.T) {
		item := &BucketStoreItem{Bucket: "update", Key: "missing", Size: 10, AccessTime: time.Now()}
		require.NoError(t, store.Update(item))
		n, err := store.Count(item.Bucket)
		require.NoError(t, err)
		require.Equal(t, 0, n)
	})
	t.Run("existing", func(t *testing.T) {
		item := &BucketStoreItem{Bucket: "update", Key: "existing", Size: 10, AccessTime: time.Now()}
		require.NoError(t, store.Set(item))
		n, err := store.Count(item.Bucket)
		require.NoError(t, err)
		require.Equal(t, 1, n)

		item.AccessTime = time.Now().Add(1 * time.Hour)
		require.NoError(t, store.Update(item))
		actual, err := store.Get(item.Bucket, item.Key)
		require.NoError(t, err)
		assert.WithinDuration(t, item.AccessTime, actual.AccessTime, 0)
	})
	t.Run("multiple", func(t *testing.T) {
		bucket := "update2"
		t1 := time.Now().Add(-1 * time.Hour)
		items := []*BucketStoreItem{
			&BucketStoreItem{Bucket: bucket, Key: "multiple1", AccessTime: t1},
			&BucketStoreItem{Bucket: bucket, Key: "multiple2", AccessTime: t1},
			&BucketStoreItem{Bucket: bucket, Key: "multiple3", AccessTime: t1},
		}
		n, err := store.Count(bucket)
		require.NoError(t, err)
		require.Equal(t, 0, n)

		require.NoError(t, store.Set(items...))
		n, err = store.Count(bucket)
		require.NoError(t, err)
		require.Equal(t, len(items), n)

		t2 := time.Now()
		for _, item := range items {
			item.AccessTime = t2
		}

		require.NoError(t, store.Update(items...))
		for i, item := range items {
			actual, err := store.Get(item.Bucket, item.Key)
			require.NoError(t, err)
			assert.Greaterf(t, actual.AccessTime, t1, "item %d access time not greater than original", i)
			assert.Greater(t, actual.AccessTime, t1, "item %d access time not greater than current time", i)
		}
	})
}

func TestBucketStore_Delete(t *testing.T) {
	store := newTestStore(t)

	t.Run("missing", func(t *testing.T) {
		item := &BucketStoreItem{Bucket: "delete", Key: "missing"}
		require.NoError(t, store.Delete(item))
	})
	t.Run("existing", func(t *testing.T) {
		item := &BucketStoreItem{Bucket: "delete", Key: "existing", Size: 10, AccessTime: time.Now()}
		require.NoError(t, store.Set(item))
		n, err := store.Count(item.Bucket)
		require.NoError(t, err)
		require.Equal(t, 1, n)

		require.NoError(t, store.Delete(item))
		n, err = store.Count(item.Bucket)
		require.NoError(t, err)
		require.Equal(t, 0, n)
	})
	t.Run("multiple", func(t *testing.T) {
		bucket := "delete2"
		items := []*BucketStoreItem{
			&BucketStoreItem{Bucket: bucket, Key: "multiple1"},
			&BucketStoreItem{Bucket: bucket, Key: "multiple2"},
			&BucketStoreItem{Bucket: bucket, Key: "multiple3"},
		}
		n, err := store.Count(bucket)
		require.NoError(t, err)
		require.Equal(t, 0, n)

		require.NoError(t, store.Set(items...))
		n, err = store.Count(bucket)
		require.NoError(t, err)
		require.Equal(t, len(items), n)

		require.NoError(t, store.Delete(items...))
		n, err = store.Count(bucket)
		require.NoError(t, err)
		require.Equal(t, 0, n)
	})
}

func TestBucketStore_TakeOldest(t *testing.T) {
	store := newTestStore(t)

	t.Run("empty", func(t *testing.T) {
		bucket := "empty"
		n, err := store.Count(bucket)
		require.NoError(t, err)
		require.Equal(t, 0, n)

		items, err := store.TakeOldest(bucket, 5)
		require.NoError(t, err)
		require.Len(t, items, 0)
	})
	t.Run("some", func(t *testing.T) {
		bucket := "some"
		t1 := time.Now()
		items := []*BucketStoreItem{
			// By Age - 3
			&BucketStoreItem{Bucket: bucket, Key: "item1",
				AccessTime: t1, Size: 2},
			// By Age - 4
			&BucketStoreItem{Bucket: bucket, Key: "item2",
				AccessTime: t1.Add(1 * time.Hour), Size: 4},
			// By Age - 1
			&BucketStoreItem{Bucket: bucket, Key: "item3",
				AccessTime: t1.Add(-1 * time.Hour), Size: 6},
			// By Age - 2
			&BucketStoreItem{Bucket: bucket, Key: "item4",
				AccessTime: t1.Add(1 * time.Minute), Size: 10},
		}
		require.NoError(t, store.Set(items...))

		n, err := store.Count(bucket)
		require.NoError(t, err)
		require.Equal(t, len(items), n)

		maxSize := 20
		actual, err := store.TakeOldest(bucket, maxSize)
		require.NoError(t, err)
		require.Len(t, actual, 3)
		sort.Slice(actual, func(i, j int) bool {
			return actual[i].Key < actual[j].Key
		})
		assert.Equal(t, items[0].Key, actual[0].Key)
		assert.Equal(t, items[2].Key, actual[1].Key)
		assert.Equal(t, items[3].Key, actual[2].Key)

		n, err = store.Count(bucket)
		require.NoError(t, err)
		require.Equal(t, 1, n)
	})
	t.Run("all", func(t *testing.T) {
		bucket := "all"
		t1 := time.Now()
		size := 5
		items := []*BucketStoreItem{
			&BucketStoreItem{Bucket: bucket, Key: "item1",
				AccessTime: t1, Size: size},
			&BucketStoreItem{Bucket: bucket, Key: "item2",
				AccessTime: t1.Add(1 * time.Hour), Size: size},
			&BucketStoreItem{Bucket: bucket, Key: "item3",
				AccessTime: t1.Add(-1 * time.Hour), Size: size},
			&BucketStoreItem{Bucket: bucket, Key: "item4",
				AccessTime: t1.Add(1 * time.Minute), Size: size},
		}
		require.NoError(t, store.Set(items...))

		n, err := store.Count(bucket)
		require.NoError(t, err)
		require.Equal(t, len(items), n)

		actual, err := store.TakeOldest(bucket, size*len(items)+1)
		require.NoError(t, err)
		require.Len(t, actual, len(items))

		n, err = store.Count(bucket)
		require.NoError(t, err)
		require.Equal(t, 0, n)
	})
}

func TestBucketStore_LastClusterUpdate(t *testing.T) {
	store := newTestStore(t)

	// test empty
	actual, err := store.LastClusterUpdate()
	require.NoError(t, err)
	require.True(t, actual.IsZero())

	// first update
	ts := time.Now()
	require.NoError(t, store.SetLastClusterUpdate(ts))
	actual, err = store.LastClusterUpdate()
	require.NoError(t, err)
	require.WithinDuration(t, ts, actual, 0)

	// set the same time again
	require.NoError(t, store.SetLastClusterUpdate(ts))
	actual, err = store.LastClusterUpdate()
	require.NoError(t, err)
	require.WithinDuration(t, ts, actual, 0)

	// update with a new time
	ts = time.Now().Add(1 * time.Minute)
	require.NoError(t, store.SetLastClusterUpdate(ts))
	actual, err = store.LastClusterUpdate()
	require.NoError(t, err)
	require.WithinDuration(t, ts, actual, 0)
}

type MockStore struct {
	ConsoleBucketStore
	NumSet         int
	NumSetOrUpdate int
	NumUpdate      int
	NumDelete      int
}

func (m *MockStore) reset() {
	m.NumSet = 0
	m.NumSetOrUpdate = 0
	m.NumUpdate = 0
	m.NumDelete = 0
}

func (m *MockStore) Set(_ ...*BucketStoreItem) error {
	m.NumSet += 1
	return nil
}

func (m *MockStore) SetOrUpdate(_ ...*BucketStoreItem) error {
	m.NumSetOrUpdate += 1
	return nil
}

func (m *MockStore) Update(_ ...*BucketStoreItem) error {
	m.NumUpdate += 1
	return nil
}

func (m *MockStore) Delete(_ ...*BucketStoreItem) error {
	m.NumDelete += 1
	return nil
}

func TestStoreEvents(t *testing.T) {
	var store MockStore
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events := make(chan *BucketEvent)

	var (
		wg       sync.WaitGroup
		storeErr error
	)

	ready := make(chan bool, 1)
	cbk := func(*BucketStoreItem) { ready <- true }
	ctx = context.WithValue(ctx, "_test_event_cbk", cbk)

	wg.Add(1)
	go func() {
		defer wg.Done()
		storeErr = StoreEvents(ctx, events, &store)
	}()

	select {
	case events <- newBucketEvent(notification.ObjectCreatedCopy, "bucket", "key1"):
		<-ready
		assert.Equal(t, 0, store.NumSet)
		assert.Equal(t, 1, store.NumSetOrUpdate)
		assert.Equal(t, 0, store.NumUpdate)
		assert.Equal(t, 0, store.NumDelete)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	select {
	case events <- newBucketEvent(notification.ObjectAccessedGet, "bucket", "key1"):
		<-ready
		assert.Equal(t, 0, store.NumSet)
		assert.Equal(t, 1, store.NumSetOrUpdate)
		assert.Equal(t, 1, store.NumUpdate)
		assert.Equal(t, 0, store.NumDelete)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	select {
	case events <- newBucketEvent(notification.ObjectAccessedHead, "bucket", "key1"):
		<-ready
		assert.Equal(t, 1, store.NumSet)
		assert.Equal(t, 1, store.NumSetOrUpdate)
		assert.Equal(t, 1, store.NumUpdate)
		assert.Equal(t, 0, store.NumDelete)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	select {
	case events <- newBucketEvent(notification.ObjectRemovedDelete, "bucket", "key1"):
		<-ready
		assert.Equal(t, 1, store.NumSet)
		assert.Equal(t, 1, store.NumSetOrUpdate)
		assert.Equal(t, 1, store.NumUpdate)
		assert.Equal(t, 1, store.NumDelete)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	store.reset()

	select {
	case events <- newBucketEvent(notification.ObjectTransitionCompleted, "bucket", "key1"):
		assert.Equal(t, 0, store.NumSet)
		assert.Equal(t, 0, store.NumSetOrUpdate)
		assert.Equal(t, 0, store.NumUpdate)
		assert.Equal(t, 0, store.NumDelete)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	cancel()

	wg.Wait()
	require.NoError(t, storeErr)
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}
