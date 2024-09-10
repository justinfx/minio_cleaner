package pkg

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// BucketStoreItem is an item stored in a BucketStore
type BucketStoreItem struct {
	Bucket     string
	Key        string
	AccessTime time.Time
	Size       int
}

// BucketStore defines an interface for storing BucketStoreItems
type BucketStore interface {
	// Count the number of items in a given bucket.
	// If bucket string is empty, count across all buckets.
	Count(bucket string) (int, error)
	// Get an item by bucket and key.
	// If item does not exist, return nil item.
	// Error is non nil only for problems with Store communication.
	Get(bucket, key string) (*BucketStoreItem, error)
	// Set 1 or more items in the store, if they do not already exist.
	Set(items ...*BucketStoreItem) error
	// SetOrUpdate sets 1 or more items in the store, and update the access time if they already exist.
	SetOrUpdate(items ...*BucketStoreItem) error
	// Update AccessTime and Size for existing items in the Store.
	Update(items ...*BucketStoreItem) error
	// Delete existing items.
	Delete(items ...*BucketStoreItem) error
	// TakeOldest pops 0 or more items from the Store with the oldest AccessTime, up to a max total size.
	TakeOldest(bucket string, totalSize int) ([]*BucketStoreItem, error)
	// LastClusterUpdate returns the timestamp that the last cluster data info was updated.
	// If SetLastClusterUpdate has not yet been called, returns a time with zero value and nil error.
	LastClusterUpdate() (time.Time, error)
	// SetLastClusterUpdate sets the timestamp of the last time the cluster data info was updated
	SetLastClusterUpdate(time.Time) error

	// Close the store and free related resources.
	Close()
}

var _ BucketStore = &ConsoleBucketStore{}

// ConsoleBucketStore is a store type used for debugging, which prints each event
// to the console stdout
type ConsoleBucketStore struct {
	enc *json.Encoder
}

func NewConsoleBucketStore() *ConsoleBucketStore {
	return &ConsoleBucketStore{enc: json.NewEncoder(os.Stdout)}
}

func (s *ConsoleBucketStore) Count(_ string) (int, error) { return 0, nil }

func (s *ConsoleBucketStore) Get(_, _ string) (*BucketStoreItem, error) { return nil, nil }

func (s *ConsoleBucketStore) Set(items ...*BucketStoreItem) error { return s.log(items...) }

func (s *ConsoleBucketStore) SetOrUpdate(items ...*BucketStoreItem) error { return s.log(items...) }

func (s *ConsoleBucketStore) Update(items ...*BucketStoreItem) error { return s.log(items...) }

func (s *ConsoleBucketStore) Delete(items ...*BucketStoreItem) error { return s.log(items...) }

func (s *ConsoleBucketStore) TakeOldest(_ string, _ int) ([]*BucketStoreItem, error) {
	return nil, nil
}

func (s *ConsoleBucketStore) LastClusterUpdate() (time.Time, error) { return time.Now(), nil }

func (s *ConsoleBucketStore) SetLastClusterUpdate(t time.Time) error {
	fmt.Printf("last update: %s\n", t)
	return nil
}

func (s *ConsoleBucketStore) log(items ...*BucketStoreItem) error {
	if s.enc == nil {
		s.enc = json.NewEncoder(os.Stdout)
	}
	for _, item := range items {
		if err := s.enc.Encode(item); err != nil {
			return err
		}
	}
	return nil
}

func (s *ConsoleBucketStore) Close() {}

// StoreEvents receives new BucketEvents and delivers them to a configured BucketStore.
// Blocks until either the event channel is closed, or the context is cancelled.
func StoreEvents(ctx context.Context, events <-chan *BucketEvent, store BucketStore) error {
	name := strings.SplitN(fmt.Sprintf("%T", store), ".", 2)[1]
	slog.Info("Starting store receiver", slog.String("type", name))

	var err error

	cbk, ok := ctx.Value("_test_event_cbk").(func(item *BucketStoreItem))
	if !ok {
		cbk = nil
	}

	for {
		select {
		case evt, ok := <-events:
			if !ok {
				return nil
			}

			if err = evt.Check(); err != nil {
				slog.Warn("skipping bad event item: %w", err)
				continue
			}

			// translate event to item
			rec := evt.Records[0]
			item := &BucketStoreItem{
				Bucket:     rec.S3.Bucket.Name,
				Key:        rec.S3.Object.Key,
				AccessTime: rec.EventTime,
				Size:       rec.S3.Object.Size,
			}

			// S3 Object name is url-encoded
			if key, err := url.PathUnescape(item.Key); err == nil {
				item.Key = key
			}

			// route item to the right action
			switch evt.Type() {
			case BucketEventWrite:
				if err = store.SetOrUpdate(item); err != nil {
					slog.Error("failed to handle write event", "error", err)
				}
			case BucketEventStat:
				if err = store.Set(item); err != nil {
					slog.Error("failed to handle stat event", "error", err)
				}
			case BucketEventRead:
				if err = store.Update(item); err != nil {
					slog.Error("failed to handle update event", "error", err)
				}
			case BucketEventDelete:
				if err = store.Delete(item); err != nil {
					slog.Error("failed to handle delete event", "error", err)
				}
			}

			if cbk != nil {
				cbk(item)
			}

		case <-ctx.Done():
			return nil
		}
	}
}
