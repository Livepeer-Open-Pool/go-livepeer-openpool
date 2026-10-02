package store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenCreatesTheSchemaOnceAndReopens(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pool.db")
	db, err := Open(path)
	require.NoError(t, err)
	version, err := db.Version()
	require.NoError(t, err)
	require.Equal(t, len(migrations), version)
	_, err = db.Exec(`INSERT INTO settings (key, value) VALUES ('k', 'v')`)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	db, err = Open(path)
	require.NoError(t, err)
	defer db.Close()
	var value string
	require.NoError(t, db.QueryRow(`SELECT value FROM settings WHERE key = 'k'`).Scan(&value))
	require.Equal(t, "v", value)
}

func TestTxRollsBackOnError(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "pool.db"))
	require.NoError(t, err)
	defer db.Close()

	boom := errors.New("boom")
	err = db.Tx(context.Background(), func(tx *sql.Tx) error {
		if _, err := tx.Exec(`INSERT INTO settings (key, value) VALUES ('k', 'v')`); err != nil {
			return err
		}
		return boom
	})
	require.ErrorIs(t, err, boom)
	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM settings`).Scan(&count))
	require.Zero(t, count)
}

func TestForeignKeysAreEnforced(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "pool.db"))
	require.NoError(t, err)
	defer db.Close()
	_, err = db.Exec(`INSERT INTO machines (id, member_id, label, gpus, status, created_at) VALUES ('m', 'nobody', 'box', '[]', 'approved', 0)`)
	require.Error(t, err)
}
