// Package store opens the pool service's SQLite database and keeps its
// schema current. Queries live with the code that owns the data.
package store

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"

	_ "github.com/mattn/go-sqlite3" // the SQLite driver
)

// DB is the pool service's database.
type DB struct {
	*sql.DB
}

// Open opens the database at path, creating it if needed, and applies any
// migrations it has not seen.
func Open(path string) (*DB, error) {
	// Write transactions take the write lock up front, so two of them queue
	// (for up to the busy timeout) rather than deadlock on upgrade.
	dsn := "file:" + url.PathEscape(path) + "?_foreign_keys=on&_busy_timeout=5000&_journal_mode=WAL&_txlock=immediate"
	sqlDB, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}
	db := &DB{DB: sqlDB}
	if err := db.migrate(); err != nil {
		sqlDB.Close()
		return nil, fmt.Errorf("migrate %s: %w", path, err)
	}
	return db, nil
}

// Tx runs fn in a transaction, committing when fn returns nil.
func (db *DB) Tx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (db *DB) migrate() error {
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)`); err != nil {
		return err
	}
	var current int
	if err := db.QueryRow(`SELECT COALESCE(MAX(version), 0) FROM schema_version`).Scan(&current); err != nil {
		return err
	}
	for version := current + 1; version <= len(migrations); version++ {
		err := db.Tx(context.Background(), func(tx *sql.Tx) error {
			if _, err := tx.Exec(migrations[version-1]); err != nil {
				return err
			}
			_, err := tx.Exec(`INSERT INTO schema_version (version) VALUES (?)`, version)
			return err
		})
		if err != nil {
			return fmt.Errorf("migration %d: %w", version, err)
		}
	}
	return nil
}

// Version is the number of migrations applied.
func (db *DB) Version() (int, error) {
	var version int
	err := db.QueryRow(`SELECT COALESCE(MAX(version), 0) FROM schema_version`).Scan(&version)
	return version, err
}
