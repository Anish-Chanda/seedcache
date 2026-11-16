package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/rs/zerolog"
	_ "modernc.org/sqlite"
)

func TestOpenInitializesMigrationsAndNoOpLogs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "seedcache.db")
	var logs bytes.Buffer
	logger := zerolog.New(&logs)

	db, err := Open(context.Background(), path, logger)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := assertMigrationSchema(db); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), "SQLite database migrations applied") {
		t.Errorf("initial migration log = %s, want applied status", logs.String())
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	logs.Reset()
	db, err = Open(context.Background(), path, logger)
	if err != nil {
		t.Fatalf("reopen() error = %v", err)
	}
	defer db.Close()
	if err := assertAppliedMigration(db, 3); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), "SQLite database schema is up to date") {
		t.Errorf("no-op migration log = %s, want up-to-date status", logs.String())
	}
}

func TestOpenPreservesExistingDataAndUserVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "existing.db")
	legacy, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := legacy.Exec("CREATE TABLE legacy_data (value TEXT NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	if _, err := legacy.Exec("INSERT INTO legacy_data(value) VALUES (?)", "kept"); err != nil {
		t.Fatal(err)
	}
	if _, err := legacy.Exec("PRAGMA user_version = 1"); err != nil {
		t.Fatal(err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}

	db, err := Open(context.Background(), path, zerolog.Nop())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	var value string
	if err := db.QueryRow("SELECT value FROM legacy_data").Scan(&value); err != nil {
		t.Fatalf("read preserved data: %v", err)
	}
	if value != "kept" {
		t.Errorf("legacy value = %q, want kept", value)
	}
	var userVersion int
	if err := db.QueryRow("PRAGMA user_version").Scan(&userVersion); err != nil {
		t.Fatalf("read user_version: %v", err)
	}
	if userVersion != 1 {
		t.Errorf("user_version = %d, want preserved value 1", userVersion)
	}
	if err := assertAppliedMigration(db, 3); err != nil {
		t.Fatal(err)
	}
}

func TestRunMigrationsAppliesFilesystemFixture(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fixture.db")
	files := fixtureMigrations(
		"CREATE TABLE fixture_data (value TEXT NOT NULL);",
		"INSERT INTO fixture_data(value) VALUES ('applied');",
	)
	db, err := openSQLite(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if err := runMigrations(context.Background(), db, files, zerolog.Nop()); err != nil {
		t.Fatalf("runMigrations() error = %v", err)
	}

	if err := db.Ping(); err == nil {
		t.Fatal("migration connection pool was left open")
	}
	check, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer check.Close()
	var value string
	if err := check.QueryRow("SELECT value FROM fixture_data").Scan(&value); err != nil {
		t.Fatalf("read migration fixture result: %v", err)
	}
	if value != "applied" {
		t.Errorf("fixture value = %q, want applied", value)
	}
	var version int
	if err := check.QueryRow("SELECT version FROM schema_migrations").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 2 {
		t.Errorf("fixture migration version = %d, want 2", version)
	}
}

func TestOpenEnforcesForeignKeys(t *testing.T) {
	db, err := Open(context.Background(), filepath.Join(t.TempDir(), "foreign-keys.db"), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxIdleConns(0)
	var timeout int
	if err := db.QueryRow("PRAGMA busy_timeout").Scan(&timeout); err != nil || timeout != 5000 {
		t.Fatalf("busy timeout = %d, error %v", timeout, err)
	}
	if _, err := db.Exec("CREATE TABLE parents (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE children (parent_id INTEGER REFERENCES parents(id))"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO children(parent_id) VALUES (404)"); err == nil {
		t.Fatal("insert with a missing foreign key succeeded")
	}
}

func TestOpenRejectsNewerAndDirtyMigrationVersions(t *testing.T) {
	tests := []struct {
		name    string
		version int
		dirty   bool
		want    string
	}{
		{name: "newer", version: 4, want: "no migration found for version 4"},
		{name: "dirty", version: 1, dirty: true, want: "dirty"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state.db")
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec("CREATE TABLE schema_migrations (version uint64, dirty bool)"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec("INSERT INTO schema_migrations(version, dirty) VALUES (?, ?)", tt.version, tt.dirty); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}

			opened, err := Open(context.Background(), path, zerolog.Nop())
			if err == nil {
				_ = opened.Close()
				t.Fatal("Open() succeeded for invalid migration state")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("Open() error = %v, want substring %q", err, tt.want)
			}
		})
	}
}

func TestRunMigrationsFailureLeavesDatabaseDirtyAndClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "failed.db")
	files := fstest.MapFS{
		"migrations/000001_initial.up.sql":   &fstest.MapFile{Data: []byte("CREATE TABLE rolled_back (value TEXT); INSERT INTO rolled_back VALUES ('discard'); INVALID SQL;")},
		"migrations/000001_initial.down.sql": &fstest.MapFile{Data: []byte("SELECT 1;")},
	}
	db, err := openSQLite(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if err := runMigrations(context.Background(), db, files, zerolog.Nop()); err == nil {
		t.Fatal("runMigrations() succeeded for invalid SQL")
	}

	if err := db.Ping(); err == nil {
		t.Fatal("migration connection pool was left open")
	}
	check, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer check.Close()
	var version int
	var dirty bool
	if err := check.QueryRow("SELECT version, dirty FROM schema_migrations").Scan(&version, &dirty); err != nil {
		t.Fatalf("read failed migration state: %v", err)
	}
	if version != 1 || !dirty {
		t.Errorf("migration state = (%d, %t), want (1, true)", version, dirty)
	}
	var tables int
	if err := check.QueryRow("SELECT count(*) FROM sqlite_master WHERE name = 'rolled_back'").Scan(&tables); err != nil {
		t.Fatal(err)
	}
	if tables != 0 {
		t.Fatal("failed migration did not roll back its SQL statements")
	}
}

func fixtureMigrations(first, second string) fs.FS {
	return fstest.MapFS{
		"migrations/000001_initial.up.sql":     &fstest.MapFile{Data: []byte(first)},
		"migrations/000001_initial.down.sql":   &fstest.MapFile{Data: []byte("DROP TABLE IF EXISTS fixture_data;")},
		"migrations/000002_follow_up.up.sql":   &fstest.MapFile{Data: []byte(second)},
		"migrations/000002_follow_up.down.sql": &fstest.MapFile{Data: []byte("DELETE FROM fixture_data;")},
	}
}

func assertMigrationSchema(db *sql.DB) error {
	if err := assertAppliedMigration(db, 3); err != nil {
		return err
	}
	columns, err := db.Query("PRAGMA table_info(replications)")
	if err != nil {
		return err
	}
	var foundCacheState, foundCacheOperation, foundCacheUpdatedAt, foundCacheError, foundEvictedAt, foundRestoredAt bool
	for columns.Next() {
		var cid, notnull, primaryKey int
		var name, columnType string
		var defaultValue sql.NullString
		if err := columns.Scan(&cid, &name, &columnType, &notnull, &defaultValue, &primaryKey); err != nil {
			_ = columns.Close()
			return err
		}
		switch name {
		case "cache_state":
			foundCacheState = defaultValue.Valid && defaultValue.String == "'present'"
		case "cache_operation":
			foundCacheOperation = defaultValue.Valid && defaultValue.String == "''"
		case "cache_updated_at":
			foundCacheUpdatedAt = true
		case "cache_error":
			foundCacheError = defaultValue.Valid && defaultValue.String == "''"
		case "evicted_at":
			foundEvictedAt = true
		case "restored_at":
			foundRestoredAt = true
		}
	}
	if err := columns.Err(); err != nil {
		_ = columns.Close()
		return err
	}
	if err := columns.Close(); err != nil {
		return err
	}
	if !foundCacheState || !foundCacheOperation || !foundCacheUpdatedAt || !foundCacheError || !foundEvictedAt || !foundRestoredAt {
		return fmt.Errorf("replications cache lifecycle columns missing or have incorrect defaults")
	}
	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
	if err != nil {
		return err
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return err
		}
		tables = append(tables, name)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(tables) != 2 || tables[0] != "replications" || tables[1] != "schema_migrations" {
		return fmt.Errorf("application tables = %v, want replications and schema_migrations", tables)
	}
	var userVersion int
	if err := db.QueryRow("PRAGMA user_version").Scan(&userVersion); err != nil {
		return err
	}
	if userVersion != 0 {
		return fmt.Errorf("user_version = %d, want untouched default 0", userVersion)
	}
	return nil
}

func TestOpenUpgradesPhaseOneSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "phase-one.db")
	phaseOne, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := phaseOne.Exec("CREATE TABLE schema_migrations (version uint64, dirty bool)"); err != nil {
		t.Fatal(err)
	}
	if _, err := phaseOne.Exec("INSERT INTO schema_migrations(version, dirty) VALUES (1, false)"); err != nil {
		t.Fatal(err)
	}
	if err := phaseOne.Close(); err != nil {
		t.Fatal(err)
	}

	db, err := Open(context.Background(), path, zerolog.Nop())
	if err != nil {
		t.Fatalf("Open() upgrading Phase 1 database: %v", err)
	}
	defer db.Close()
	if err := assertAppliedMigration(db, 3); err != nil {
		t.Fatal(err)
	}
	var table string
	if err := db.QueryRow("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'replications'").Scan(&table); err != nil {
		t.Fatalf("replications table was not created: %v", err)
	}
}

func assertAppliedMigration(db *sql.DB, expected int) error {
	var version int
	var dirty bool
	if err := db.QueryRow("SELECT version, dirty FROM schema_migrations").Scan(&version, &dirty); err != nil {
		return err
	}
	if version != expected || dirty {
		return fmt.Errorf("migration state = (%d, %t), want (%d, false)", version, dirty, expected)
	}
	return nil
}

func TestOpenSetupErrors(t *testing.T) {
	t.Run("canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		db, err := Open(ctx, filepath.Join(t.TempDir(), "canceled.db"), zerolog.Nop())
		if db != nil || !errors.Is(err, context.Canceled) {
			t.Fatalf("Open returned %v, %v", db, err)
		}
	})
	t.Run("directory", func(t *testing.T) {
		db, err := Open(context.Background(), t.TempDir(), zerolog.Nop())
		if db != nil || err == nil {
			t.Fatalf("Open returned %v, %v for a directory", db, err)
		}
	})
}
