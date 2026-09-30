// Package pgtest gives the engine's tests a scratch Postgres database.
package pgtest

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand/v2"
	"os"
	"testing"

	"github.com/goware/pgkit/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
)

// EnvURL names the variable that points the tests at a Postgres server.
const EnvURL = "WORKFLOW_TEST_DATABASE_URL"

// Scratch creates a database on the EnvURL server, dropped on cleanup. Without
// EnvURL it skips, or fails when CI is set so a misconfigured job cannot pass.
func Scratch(t *testing.T) *pgkit.DB {
	t.Helper()
	url := os.Getenv(EnvURL)
	if url == "" && os.Getenv("CI") != "" {
		t.Fatalf("%s unset in CI", EnvURL)
	}
	if url == "" {
		t.Skipf("%s unset", EnvURL)
	}
	admin := connect(t, url, "")
	name := fmt.Sprintf("scratch_%08x", rand.Uint32())
	_, err := admin.Conn.Exec(t.Context(), "CREATE DATABASE "+name)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := admin.Conn.Exec(context.Background(), "DROP DATABASE "+name+" WITH (FORCE)")
		require.NoError(t, err)
	})
	return connect(t, url, name)
}

// SQL opens a database/sql handle on db's pool for the goose provider API.
func SQL(t *testing.T, db *pgkit.DB) *sql.DB {
	t.Helper()
	sqlDB := stdlib.OpenDBFromPool(db.Conn)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	return sqlDB
}

func connect(t *testing.T, url, database string) *pgkit.DB {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(url)
	require.NoError(t, err)
	if database != "" {
		cfg.ConnConfig.Database = database
	}
	db, err := pgkit.ConnectWithPGX("workflow-test", cfg)
	require.NoError(t, err)
	t.Cleanup(db.Conn.Close)
	return db
}
