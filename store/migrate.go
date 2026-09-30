package store

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"

	"github.com/Masterminds/squirrel"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pressly/goose/v3"
)

const (
	VersionTable        = "workflows_goose_db_version"
	SchemaVersion int64 = 1
)

// ErrSchemaOutdated reports a database whose store chain is behind SchemaVersion.
var ErrSchemaOutdated = errors.New("workflow store: schema outdated")

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Migrate applies the store's own goose chain under VersionTable.
func Migrate(ctx context.Context, db *sql.DB) error {
	p, err := provider(db)
	if err != nil {
		return err
	}
	if _, err := p.Up(ctx); err != nil {
		return fmt.Errorf("workflow store: migrate: %w", err)
	}
	return nil
}

func provider(db *sql.DB) (*goose.Provider, error) {
	dir, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		return nil, fmt.Errorf("workflow store: migrations fs: %w", err)
	}
	p, err := goose.NewProvider(goose.DialectPostgres, db, dir,
		goose.WithTableName(VersionTable),
		goose.WithDisableGlobalRegistry(true),
	)
	if err != nil {
		return nil, fmt.Errorf("workflow store: goose provider: %w", err)
	}
	return p, nil
}

// SchemaVersion returns the highest version the store chain has applied, or 0 when the chain has never run.
func (t *Store) SchemaVersion(ctx context.Context) (int64, error) {
	var version int64
	q := squirrel.Expr(`SELECT COALESCE(MAX(version_id), 0) FROM ` + VersionTable + ` WHERE is_applied`)
	if err := t.DB.Query.GetOne(ctx, q, &version); err != nil {
		if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok && pgErr.Code == pgerrcode.UndefinedTable {
			return 0, nil
		}
		return 0, fmt.Errorf("workflow store: read %s: %w", VersionTable, err)
	}
	return version, nil
}
