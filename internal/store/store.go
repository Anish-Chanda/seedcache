package store

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/sqlite"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/rs/zerolog"
	_ "modernc.org/sqlite"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

func Open(ctx context.Context, path string, logger zerolog.Logger) (*sql.DB, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("database path is required")
	}
	if path == ":memory:" || strings.HasPrefix(path, "file:") {
		return nil, errors.New("database path must name a persistent SQLite file")
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create database directory %q: %w", dir, err)
	}

	db, err := openSQLite(ctx, path)
	if err != nil {
		return nil, err
	}
	if err := migrateDatabase(ctx, path, logger); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	return db, nil
}

func openSQLite(ctx context.Context, path string) (*sql.DB, error) {
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve SQLite database path: %w", err)
	}
	// DSN pragmas also apply when database/sql replaces a connection.
	dsn := url.URL{Scheme: "file", Path: absolutePath}
	dsn.RawQuery = url.Values{"_pragma": {"busy_timeout(5000)", "foreign_keys(ON)"}}.Encode()
	db, err := sql.Open("sqlite", dsn.String())
	if err != nil {
		return nil, fmt.Errorf("open SQLite database: %w", err)
	}
	closeAfterSetupError := func(cause error) (*sql.DB, error) {
		if closeErr := db.Close(); closeErr != nil {
			cause = errors.Join(cause, fmt.Errorf("close SQLite database after setup failure: %w", closeErr))
		}
		return nil, cause
	}

	// A single connection serializes writes.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.PingContext(ctx); err != nil {
		return closeAfterSetupError(fmt.Errorf("connect to SQLite database: %w", err))
	}
	if _, err := db.ExecContext(ctx, "PRAGMA journal_mode = WAL"); err != nil {
		return closeAfterSetupError(fmt.Errorf("enable SQLite WAL mode: %w", err))
	}
	return db, nil
}

func migrateDatabase(ctx context.Context, path string, logger zerolog.Logger) error {
	// The migrate SQLite driver owns and closes its sql.DB, so migrations use a
	// dedicated connection pool instead of the pool returned to the application.
	db, err := openSQLite(ctx, path)
	if err != nil {
		return fmt.Errorf("open SQLite connection for migrations: %w", err)
	}
	return runMigrations(ctx, db, migrationFiles, logger)
}

func runMigrations(ctx context.Context, db *sql.DB, files fs.FS, logger zerolog.Logger) (err error) {
	source, err := iofs.New(files, "migrations")
	if err != nil {
		return errors.Join(fmt.Errorf("load embedded database migrations: %w", err), db.Close())
	}
	database, err := sqlite.WithInstance(db, &sqlite.Config{})
	if err != nil {
		return errors.Join(fmt.Errorf("configure SQLite migration driver: %w", err), source.Close(), db.Close())
	}
	migrator, err := migrate.NewWithInstance("iofs", source, "sqlite", database)
	if err != nil {
		return errors.Join(fmt.Errorf("create database migrator: %w", err), source.Close(), database.Close())
	}
	defer func() {
		sourceErr, databaseErr := migrator.Close()
		if sourceErr != nil {
			err = errors.Join(err, fmt.Errorf("close database migration source: %w", sourceErr))
		}
		if databaseErr != nil {
			err = errors.Join(err, fmt.Errorf("close SQLite migration driver: %w", databaseErr))
		}
	}()

	version, dirty, err := migrator.Version()
	if errors.Is(err, migrate.ErrNilVersion) {
		version, err = 0, nil
	}
	if err != nil {
		return fmt.Errorf("read database migration version: %w", err)
	}
	if dirty {
		return fmt.Errorf("database migration state is dirty at version %d", version)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("database migration canceled: %w", err)
	}

	logger.Info().Uint("current_version", version).Msg("applying SQLite database migrations")
	// migrate v4 exposes no context-aware Up method, so migrations run synchronously.
	if err := migrator.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("apply SQLite database migrations: %w", err)
	}

	finalVersion, dirty, err := migrator.Version()
	if err != nil {
		return fmt.Errorf("read final database migration version: %w", err)
	}
	if dirty {
		return fmt.Errorf("database migration state is dirty at version %d", finalVersion)
	}
	if version == finalVersion {
		logger.Info().Uint("version", finalVersion).Msg("SQLite database schema is up to date")
	} else {
		logger.Info().Uint("from_version", version).Uint("to_version", finalVersion).Msg("SQLite database migrations applied")
	}
	return nil
}
