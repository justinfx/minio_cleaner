package pkg

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	_ "modernc.org/sqlite"
)

// SQLiteInMemory defines a dbpath that is stored in-memory
const SQLiteInMemory = "file::memory:"

// SQLiteBucketStore is a store implementation that uses a sqlite
// database to store BucketStoreItems
type SQLiteBucketStore struct {
	db *sql.DB
}

var _ BucketStore = &SQLiteBucketStore{}

// NewSQLiteBucketStore opens the sqlite database named by dbpath,
// and builds the schema as needed.
// The store should be closed when it is no longer needed.
func NewSQLiteBucketStore(dbpath string) (*SQLiteBucketStore, error) {
	const schema = `
		CREATE TABLE IF NOT EXISTS bucket_events (
			bucket TEXT NOT NULL,
			key TEXT NOT NULL,
			access_time TIMESTAMP NOT NULL,
			size INTEGER NOT NULL,
		    PRIMARY KEY(bucket, key)
		);

		CREATE INDEX IF NOT EXISTS idx_bucket_access_time
			ON bucket_events (bucket, access_time);

		CREATE TABLE IF NOT EXISTS cluster_updates (
		    idx INTEGER,
		    timestamp TIMESTAMP NOT NULL,
		    PRIMARY KEY(idx)
		)
	`

	if dbpath == "" {
		dbpath = SQLiteInMemory
	}

	slog.Info("Opening sqlite bucket store", "db", dbpath)
	db, err := sql.Open("sqlite", dbpath)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite db: %w", err)
	}

	// Make database safe for concurrent write access
	db.SetMaxOpenConns(1)

	st := &SQLiteBucketStore{db: db}
	if err = st.mustExec("PRAGMA busy_timeout=5000;"); err != nil {
		return nil, fmt.Errorf("failed to set up sqlite timeout: %w", err)
	}
	// TODO: optional? Only if not network mount?
	if err = st.mustExec("PRAGMA synchronous=NORMAL;"); err != nil {
		return nil, fmt.Errorf("failed to set up sqlite sync mode: %w", err)
	}
	// TODO: optional? Only if not network mount?
	if err = st.mustExec("PRAGMA journal_mode=WAL;"); err != nil {
		return nil, fmt.Errorf("failed to set up sqlite journal mode: %w", err)
	}

	if _, err = db.Exec(schema); err != nil {
		return nil, err
	}

	return st, nil
}

func (s *SQLiteBucketStore) mustExec(query string, args ...any) error {
	_, err := s.db.Exec(query, args...)
	return err
}

func (s *SQLiteBucketStore) Close() {
	if s.db != nil {
		s.db.Close()
	}
}

func (s *SQLiteBucketStore) Count(bucket string) (int, error) {
	const (
		queryAll  = `SELECT COUNT(*) FROM bucket_events;`
		querySome = `SELECT COUNT(*) FROM bucket_events WHERE bucket = ?;`
	)
	q := queryAll
	if bucket != "" {
		q = querySome
	}
	var count int
	if err := s.db.QueryRow(q, bucket).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to count bucket events: %w", err)
	}
	return count, nil
}

func (s *SQLiteBucketStore) Size(bucket string) (int, error) {
	const query = `SELECT COALESCE(SUM(size), 0) as total FROM bucket_events WHERE bucket = ?;`
	var size int
	if err := s.db.QueryRow(query, bucket).Scan(&size); err != nil {
		return 0, fmt.Errorf("failed to get total byte size of bucket events: %w", err)
	}
	return size, nil
}

func (s *SQLiteBucketStore) Get(bucket, key string) (*BucketStoreItem, error) {
	const query = `
		SELECT bucket, key, access_time, size FROM bucket_events
		WHERE bucket = ? AND key = ?;
	`

	var item BucketStoreItem
	if err := s.db.QueryRow(query, bucket, key).
		Scan(&item.Bucket, &item.Key, &item.AccessTime, &item.Size); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get bucket %q, key %q: %w", bucket, key, err)
	}
	return &item, nil
}

func (s *SQLiteBucketStore) Set(items ...*BucketStoreItem) error {
	return s.set(false, items...)
}

func (s *SQLiteBucketStore) SetOrUpdate(items ...*BucketStoreItem) error {
	return s.set(true, items...)
}

func (s *SQLiteBucketStore) set(update bool, items ...*BucketStoreItem) error {
	const (
		query     = `INSERT INTO bucket_events (bucket, key, access_time, size) VALUES (?, ?, ?, ?) %s;`
		updateQ   = `ON CONFLICT DO UPDATE SET access_time=EXCLUDED.access_time, size=EXCLUDED.size`
		noUpdateQ = `ON CONFLICT DO NOTHING`
	)

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback()

	var stmt string
	if update {
		stmt = fmt.Sprintf(query, updateQ)
	} else {
		stmt = fmt.Sprintf(query, noUpdateQ)
	}

	q, err := tx.Prepare(stmt)
	if err != nil {
		return fmt.Errorf("failed to prepare query: %w", err)
	}
	defer q.Close()

	for _, item := range items {
		_, err := q.Exec(item.Bucket, item.Key, item.AccessTime, item.Size)
		if err != nil {
			return fmt.Errorf("failed to insert bucket item %+v: %w", item, err)
		}
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

func (s *SQLiteBucketStore) Update(items ...*BucketStoreItem) error {
	const query = `
		UPDATE bucket_events
		SET access_time = ?
		WHERE bucket = ? AND key = ?;
	`

	q, err := s.db.Prepare(query)
	if err != nil {
		return fmt.Errorf("failed to prepare query: %w", err)
	}
	defer q.Close()

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback()

	for _, item := range items {
		_, err = tx.Exec(query, item.AccessTime, item.Bucket, item.Key)
		if err != nil {
			return fmt.Errorf("failed to update bucket object record %+v: %w", item, err)
		}
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

func (s *SQLiteBucketStore) Delete(items ...*BucketStoreItem) error {
	const query = `
		DELETE from bucket_events WHERE bucket = ? AND key = ?;
	`

	q, err := s.db.Prepare(query)
	if err != nil {
		return fmt.Errorf("failed to prepare query: %w", err)
	}
	defer q.Close()

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback()

	for _, item := range items {
		_, err = tx.Exec(query, item.Bucket, item.Key)
		if err != nil {
			return fmt.Errorf("failed to delete bucket object record %+v: %w", item, err)
		}
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

func (s *SQLiteBucketStore) TakeOldest(bucket string, totalSize int) ([]*BucketStoreItem, error) {
	// delete and return the oldest accessed items, up to either a total size, or am item limit
	const query = `
		WITH rows_to_delete AS (
			SELECT bucket, key, access_time, size
			FROM (
				SELECT
					bucket, key, access_time, size,
					SUM(size) OVER (
						ORDER BY access_time ROWS UNBOUNDED PRECEDING
					) AS RunningSize
				FROM bucket_events
				WHERE bucket = ?
				ORDER BY access_time
			) sub
			WHERE RunningSize <= ?
			LIMIT 50000
		)
		DELETE FROM bucket_events
		WHERE (bucket, key) IN (
			SELECT bucket, key FROM rows_to_delete
		)
		RETURNING key, access_time, size;
	`

	rows, err := s.db.Query(query, bucket, totalSize)
	if err != nil {
		return nil, fmt.Errorf("failed to query bucket %q event records: %w", bucket, err)
	}

	var ret []*BucketStoreItem
	for rows.Next() {
		item := BucketStoreItem{Bucket: bucket}
		if err := rows.Scan(&item.Key, &item.AccessTime, &item.Size); err != nil {
			return nil, fmt.Errorf("failed to scan bucket event records: %w", err)
		}
		ret = append(ret, &item)
	}
	return ret, nil
}

func (s *SQLiteBucketStore) LastClusterUpdate() (time.Time, error) {
	const query = `SELECT timestamp FROM cluster_updates LIMIT 1`
	var last_update time.Time
	if err := s.db.QueryRow(query).Scan(&last_update); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return last_update, nil
		}
		return last_update, fmt.Errorf("failed to query last cluster update time: %w", err)
	}
	return last_update, nil
}

func (s *SQLiteBucketStore) SetLastClusterUpdate(t time.Time) error {
	const query = `
		INSERT INTO cluster_updates (idx, timestamp) VALUES (1, ?) 
		ON CONFLICT DO UPDATE SET timestamp=EXCLUDED.timestamp;
	`
	if _, err := s.db.Exec(query, t); err != nil {
		return fmt.Errorf("failed to set last cluster update time: %w", err)
	}
	return nil
}
