package pkg

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

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
	const query = `
		INSERT INTO bucket_events (bucket, key, access_time, size) VALUES (?, ?, ?, ?)
			ON CONFLICT DO UPDATE SET access_time=EXCLUDED.access_time, size=EXCLUDED.size;
	`

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback()

	q, err := tx.Prepare(query)
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

func (s *SQLiteBucketStore) TakeOldest(bucket string, limit int) ([]*BucketStoreItem, error) {
	const query = `
		DELETE FROM bucket_events
		  WHERE (bucket, key) IN (
			SELECT bucket, key FROM bucket_events
			WHERE bucket = ?
			ORDER BY access_time
			LIMIT ?
		  )
		RETURNING key, access_time, size;
	`

	rows, err := s.db.Query(query, bucket, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query bucket event records: %w", err)
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
