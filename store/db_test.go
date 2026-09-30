package store

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/goware/workflow/internal/pgtest"
)

func testDB(t *testing.T) *Store {
	t.Helper()
	db := pgtest.Scratch(t)
	require.NoError(t, Migrate(t.Context(), pgtest.SQL(t, db)))
	return New(db)
}
