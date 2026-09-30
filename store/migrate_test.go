package store

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/goware/workflow/internal/pgtest"
)

func tableExists(t *testing.T, db *Store, name string) bool {
	t.Helper()
	var exists bool
	err := db.DB.Conn.QueryRow(t.Context(), `SELECT to_regclass($1) IS NOT NULL`, "public."+name).Scan(&exists)
	require.NoError(t, err)
	return exists
}

func TestMigrate(t *testing.T) {
	t.Run("creates the schema on an empty database and is idempotent", func(t *testing.T) {
		db := pgtest.Scratch(t)
		sqlDB := pgtest.SQL(t, db)

		require.NoError(t, Migrate(t.Context(), sqlDB))
		require.NoError(t, Migrate(t.Context(), sqlDB))

		st := New(db)
		require.True(t, tableExists(t, st, TableRuns))
		require.True(t, tableExists(t, st, TableTasks))
		version, err := st.SchemaVersion(t.Context())
		require.NoError(t, err)
		require.Equal(t, SchemaVersion, version)
	})

	t.Run("Down drops both tables", func(t *testing.T) {
		db := pgtest.Scratch(t)
		sqlDB := pgtest.SQL(t, db)
		require.NoError(t, Migrate(t.Context(), sqlDB))

		p, err := provider(sqlDB)
		require.NoError(t, err)
		_, err = p.Down(t.Context())
		require.NoError(t, err)

		st := New(db)
		require.False(t, tableExists(t, st, TableRuns))
		require.False(t, tableExists(t, st, TableTasks))
	})

	t.Run("SchemaVersion reports 0 when the version table is missing", func(t *testing.T) {
		version, err := New(pgtest.Scratch(t)).SchemaVersion(t.Context())
		require.NoError(t, err)
		require.Zero(t, version)
	})

	t.Run("SchemaVersion matches the highest embedded migration", func(t *testing.T) {
		entries, err := migrationsFS.ReadDir("migrations")
		require.NoError(t, err)
		var highest int64
		for _, e := range entries {
			var n int64
			_, err := fmt.Sscanf(e.Name(), "%d_", &n)
			require.NoError(t, err, e.Name())
			highest = max(highest, n)
		}
		require.Equal(t, SchemaVersion, highest)
	})
}
